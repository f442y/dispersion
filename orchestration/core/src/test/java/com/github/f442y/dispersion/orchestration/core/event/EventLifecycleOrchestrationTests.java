package com.github.f442y.dispersion.orchestration.core.event;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepCompletedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepStartedEvent;
import com.github.f442y.dispersion.event.guard.StateVisitLimitExceededEvent;
import com.github.f442y.dispersion.event.parallel.ParallelBranchCompletedEvent;
import com.github.f442y.dispersion.event.parallel.ParallelForkStartedEvent;
import com.github.f442y.dispersion.event.parallel.ParallelJoinCompletedEvent;
import com.github.f442y.dispersion.event.signal.SignalAwaitedEvent;
import com.github.f442y.dispersion.event.signal.SignalDeliveredEvent;
import com.github.f442y.dispersion.event.state.ActionExecutedEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.state.StateExitedEvent;
import com.github.f442y.dispersion.event.state.TransitionEvaluatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompensatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnFailedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.event.turn.TurnSuspendedEvent;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.AbstractStateMachineCallable;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandDeduplicatedEvent;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.core.OrchestrationBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationExecutor;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Event Lifecycle & Observability SPI Integration Tests")
class EventLifecycleOrchestrationTests {

    public enum SimpleState implements StateKey {
        START,
        PROCESS,
        COMPLETED
    }

    public static final class SimpleContext implements StateMachineContext {
        public String orderId = "ORD-1001";
        public int count = 0;
        public String status = "NEW";
    }

    public enum OrchFlowState implements StateKey {
        INIT,
        WAIT_SIGNAL,
        FINALIZE,
        DONE
    }

    public record PaymentSignal(
            String correlationKey,
            String paymentRef,
            double amount
    ) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "PaymentSignal";
        }
    }

    @Test
    @DisplayName("Should capture complete lifecycle events in an Atomic State Machine")
    void testAtomicStateMachineLifecycleEvents() throws Exception {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());

        StateMachineConfiguration<SimpleContext, SimpleState, Integer, String> machine =
                AtomicStateMachineBuilder.<SimpleContext, SimpleState, Integer, String>create("SimpleAtomicMachine", SimpleState.class)
                        .context(SimpleContext::new)
                        .initialState(SimpleState.START)
                        .endStates(SimpleState.COMPLETED)
                        .eventListener(events::add)
                        .input((ctx, in) -> {
                            ctx.count = in != null ? in : 0;
                            return ctx;
                        })
                        .state(SimpleState.START)
                            .action(ctx -> {
                                ctx.count += 10;
                                return ctx;
                            })
                            .transition(SimpleState.PROCESS)
                        .state(SimpleState.PROCESS)
                            .action(ctx -> {
                                ctx.count *= 2;
                                return ctx;
                            })
                            .transition(SimpleState.COMPLETED)
                        .output(ctx -> "RESULT=" + ctx.count)
                        .build();

        UUID execId = UUID.randomUUID();
        String result = AbstractStateMachineCallable.executeDirect(execId, machine, null, 5);
        assertEquals("RESULT=30", result);

        // Assert event sequence
        assertFalse(events.isEmpty());
        assertInstanceOf(TurnStartedEvent.class, events.get(0));

        TurnStartedEvent start = (TurnStartedEvent) events.get(0);
        assertEquals("SimpleAtomicMachine", start.machineName());
        assertEquals(execId, start.machineId());

        boolean hasStateEntered = events.stream().anyMatch(e -> e instanceof StateEnteredEvent see && see.stateName().equals("START"));
        boolean hasStateExited = events.stream().anyMatch(e -> e instanceof StateExitedEvent see && see.stateName().equals("START"));
        boolean hasTransition = events.stream().anyMatch(e -> e instanceof TransitionEvaluatedEvent tee && tee.sourceState().equals("START") && tee.targetState().equals("PROCESS"));
        boolean hasTurnCompleted = events.stream().anyMatch(e -> e instanceof TurnCompletedEvent tce && tce.finalStateName().equals("COMPLETED"));

        assertTrue(hasStateEntered, "Must emit StateEnteredEvent");
        assertTrue(hasStateExited, "Must emit StateExitedEvent");
        assertTrue(hasTransition, "Must emit TransitionEvaluatedEvent");
        assertTrue(hasTurnCompleted, "Must emit TurnCompletedEvent");
    }

    @Test
    @DisplayName("Should capture suspension, resumption, deduplication and signal delivery in Orchestration")
    void testOrchestrationLifecycleEventsWithSignalAndDeduplication() throws Exception {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());
        InMemoryCheckpointStore<SimpleContext, OrchFlowState> store = new InMemoryCheckpointStore<>();

        try (OrchestrationExecutor<SimpleContext, OrchFlowState, SimpleContext, String> executor =
                OrchestrationBuilder.<SimpleContext, OrchFlowState, SimpleContext, String>create("OrderOrchestrator", OrchFlowState.class)
                        .context(SimpleContext::new)
                        .initialState(OrchFlowState.INIT)
                        .endStates(OrchFlowState.DONE)
                        .eventListener(events::add)
                        .correlationKey(ctx -> ctx.orderId)
                        .checkpointStore(store)
                        .input((ctx, in) -> {
                            if (in != null) {
                                ctx.orderId = in.orderId;
                                ctx.status = in.status;
                            }
                            return ctx;
                        })
                        .state(OrchFlowState.INIT)
                            .action(ctx -> {
                                ctx.status = "INITIALIZED";
                                return ctx;
                            })
                            .transition(OrchFlowState.WAIT_SIGNAL)
                        .state(OrchFlowState.WAIT_SIGNAL)
                            .waitForCommand(PaymentSignal.class, (ctx, sig) -> {
                                ctx.status = "PAID:" + sig.paymentRef();
                                return ctx;
                            })
                            .transition(OrchFlowState.FINALIZE)
                        .state(OrchFlowState.FINALIZE)
                            .action(ctx -> {
                                ctx.status = ctx.status + "->FULFILLED";
                                return ctx;
                            })
                            .transition(OrchFlowState.DONE)
                        .output(ctx -> ctx.status)
                        .buildExecutor()) {

            SimpleContext initial = new SimpleContext();
            initial.orderId = "ORDER-1001";

            // 1. Initial Turn -> Should suspend at WAIT_SIGNAL
            OrchestrationTurnResult<SimpleContext, OrchFlowState, String> turn1 = executor.dispatchTurnSync(null, initial);
            assertTrue(turn1.isSuspended());
            assertEquals(OrchFlowState.WAIT_SIGNAL, turn1.currentStateKey());

            assertTrue(events.stream().anyMatch(e -> e instanceof SignalAwaitedEvent sae && sae.expectedSignal().equals("PaymentSignal")));
            assertTrue(events.stream().anyMatch(e -> e instanceof TurnSuspendedEvent tse && tse.stateName().equals("WAIT_SIGNAL")));

            // 2. Resume Turn with CommandEnvelope
            UUID commandId = UUID.randomUUID();
            CommandEnvelope<PaymentSignal> envelope = CommandEnvelope.of(
                    commandId,
                    new PaymentSignal("ORDER-1001", "PAY-XYZ", 150.0)
            );

            events.clear();
            CompletableFuture<OrchestrationTurnResult<SimpleContext, OrchFlowState, String>> resumeFuture =
                    executor.handleCommand(envelope);
            OrchestrationTurnResult<SimpleContext, OrchFlowState, String> turn2 = resumeFuture.get();

            assertTrue(turn2.isCompleted());
            assertEquals("PAID:PAY-XYZ->FULFILLED", turn2.output());

            assertTrue(events.stream().anyMatch(e -> e instanceof SignalDeliveredEvent sde && sde.signalName().equals("PaymentSignal")));
            assertTrue(events.stream().anyMatch(e -> e instanceof TurnCompletedEvent tce && tce.finalStateName().equals("DONE")));

            // 3. Resume Turn with DUPLICATE CommandEnvelope -> Should trigger CommandDeduplicatedEvent
            events.clear();
            CompletableFuture<OrchestrationTurnResult<SimpleContext, OrchFlowState, String>> dupFuture =
                    executor.handleCommand(envelope);
            OrchestrationTurnResult<SimpleContext, OrchFlowState, String> turn3 = dupFuture.get();
            assertNotNull(turn3);

            assertTrue(events.stream().anyMatch(e -> e instanceof CommandDeduplicatedEvent cde && cde.commandId().equals(commandId)));
        }
    }

    @Test
    @DisplayName("Should capture TurnCompensatedEvent during Saga compensation rollback")
    void testSagaCompensationRollbackLifecycleEvent() {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger compensatedCount = new AtomicInteger(0);

        try (OrchestrationExecutor<SimpleContext, OrchFlowState, Void, String> executor =
                OrchestrationBuilder.<SimpleContext, OrchFlowState, Void, String>create("SagaFaultMachine", OrchFlowState.class)
                        .context(SimpleContext::new)
                        .initialState(OrchFlowState.INIT)
                        .endStates(OrchFlowState.DONE)
                        .eventListener(events::add)
                        .state(OrchFlowState.INIT)
                            .action(ctx -> {
                                ctx.status = "STEP_A_DONE";
                                return ctx;
                            })
                            .compensate(ctx -> {
                                compensatedCount.incrementAndGet();
                                ctx.status = "STEP_A_COMPENSATED";
                                return ctx;
                            })
                            .transition(OrchFlowState.FINALIZE)
                        .state(OrchFlowState.FINALIZE)
                            .action(_ -> {
                                throw new RuntimeException("Simulated Database Crash in FINALIZE");
                            })
                            .transition(OrchFlowState.DONE)
                        .buildExecutor()) {

            assertThrows(Exception.class, () -> executor.dispatchSync(null));
            assertEquals(1, compensatedCount.get());

            // Verify TurnCompensatedEvent
            TurnCompensatedEvent compEvent = events.stream()
                    .filter(e -> e instanceof TurnCompensatedEvent)
                    .map(e -> (TurnCompensatedEvent) e)
                    .findFirst()
                    .orElse(null);

            assertNotNull(compEvent, "TurnCompensatedEvent must be emitted");
            assertEquals("SagaFaultMachine", compEvent.machineName());
            assertEquals("FINALIZE", compEvent.failedStateName());
            assertTrue(compEvent.compensatedStates().contains("INIT"));
            assertTrue(compEvent.cause().getMessage().contains("Simulated Database Crash"));
        }
    }

    @Test
    @DisplayName("Should exhaustively pattern match on all ExecutionEvent sealed subtypes")
    void testJava25ExhaustivePatternMatching() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        Duration dur = Duration.ofMillis(5);

        List<ExecutionEvent> testEvents = List.of(
                new TurnStartedEvent(id, "TestM", "CORR", now),
                new TurnSuspendedEvent(id, "TestM", "WAIT", "Sig1", "CORR", dur, now),
                new TurnCompletedEvent(id, "TestM", "DONE", "CORR", dur, now),
                new TurnCompensatedEvent(id, "TestM", "FAIL", List.of("WAIT"), new RuntimeException(), "CORR", dur, now),
                new TurnFailedEvent(id, "TestM", "FAIL", new RuntimeException(), "CORR", dur, now),
                new StateEnteredEvent(id, "TestM", "STATE_A", now),
                new StateExitedEvent(id, "TestM", "STATE_A", dur, now),
                new TransitionEvaluatedEvent(id, "TestM", "STATE_A", "STATE_B", now),
                new SignalAwaitedEvent(id, "TestM", "WAIT", "Sig1", "CORR", now),
                new SignalDeliveredEvent(id, "TestM", "WAIT", "Sig1", "CORR", now),
                new CommandDeduplicatedEvent(id, "TestM", id, "CORR", now)
        );

        for (ExecutionEvent event : testEvents) {
            String description = switch (event) {
                case TurnStartedEvent tse -> "STARTED: " + tse.machineName();
                case TurnSuspendedEvent tse -> "SUSPENDED: " + tse.stateName();
                case TurnCompletedEvent tce -> "COMPLETED: " + tce.finalStateName();
                case TurnCompensatedEvent tce -> "COMPENSATED: " + tce.failedStateName();
                case TurnFailedEvent tfe -> "FAILED: " + tfe.failedStateName();
                case StateEnteredEvent see -> "ENTERED: " + see.stateName();
                case StateExitedEvent see -> "EXITED: " + see.stateName();
                case TransitionEvaluatedEvent tee -> "TRANSITION: " + tee.sourceState() + "->" + tee.targetState();
                case SignalAwaitedEvent sae -> "AWAITING: " + sae.expectedSignal();
                case SignalDeliveredEvent sde -> "DELIVERED: " + sde.signalName();
                case CommandDeduplicatedEvent cde -> "DEDUP: " + cde.commandId();
                default -> "UNKNOWN: " + event.getClass().getSimpleName();
            };
            assertNotNull(description);
        }
    }

    @Test
    @DisplayName("Should capture TurnFailedEvent when Orchestration fails without compensation")
    void testUncompensatedFailureEmitsTurnFailedEvent() {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());

        try (OrchestrationExecutor<SimpleContext, OrchFlowState, Void, String> executor =
                OrchestrationBuilder.<SimpleContext, OrchFlowState, Void, String>create("UncompensatedFaultMachine", OrchFlowState.class)
                        .context(SimpleContext::new)
                        .initialState(OrchFlowState.INIT)
                        .endStates(OrchFlowState.DONE)
                        .eventListener(events::add)
                        .state(OrchFlowState.INIT)
                            .action(_ -> {
                                throw new IllegalStateException("Immediate failure without compensation");
                            })
                            .transition(OrchFlowState.DONE)
                        .buildExecutor()) {

            assertThrows(Exception.class, () -> executor.dispatchSync(null));

            // Must emit TurnFailedEvent and NOT TurnCompensatedEvent
            boolean hasTurnFailed = events.stream().anyMatch(e -> e instanceof TurnFailedEvent tfe
                    && tfe.failedStateName().equals("INIT")
                    && tfe.cause().getMessage().contains("Immediate failure without compensation"));
            boolean hasTurnCompensated = events.stream().anyMatch(e -> e instanceof TurnCompensatedEvent);

            assertTrue(hasTurnFailed, "TurnFailedEvent must be emitted for uncompensated failure");
            assertFalse(hasTurnCompensated, "TurnCompensatedEvent must NOT be emitted when no compensation executed");
        }
    }

    @Test
    @DisplayName("Should capture TurnFailedEvent when Atomic State Machine fails")
    void testAtomicFsmFailureEmitsTurnFailedEvent() {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());

        StateMachineConfiguration<SimpleContext, SimpleState, Integer, String> machine =
                AtomicStateMachineBuilder.<SimpleContext, SimpleState, Integer, String>create("FailingAtomicMachine", SimpleState.class)
                        .context(SimpleContext::new)
                        .initialState(SimpleState.START)
                        .endStates(SimpleState.COMPLETED)
                        .eventListener(events::add)
                        .state(SimpleState.START)
                            .action(_ -> {
                                throw new IllegalArgumentException("Action failed in atomic machine");
                            })
                            .transition(SimpleState.COMPLETED)
                        .build();

        assertThrows(Exception.class, () -> AbstractStateMachineCallable.executeDirect(UUID.randomUUID(), machine, null, 1));

        boolean hasTurnFailed = events.stream().anyMatch(e -> e instanceof TurnFailedEvent tfe
                && tfe.failedStateName().equals("START")
                && tfe.cause().getMessage().contains("Action failed in atomic machine"));
        boolean hasTurnCompensated = events.stream().anyMatch(e -> e instanceof TurnCompensatedEvent);

        assertTrue(hasTurnFailed, "Atomic machine failure must emit TurnFailedEvent");
        assertFalse(hasTurnCompensated, "Atomic machine failure must NOT emit TurnCompensatedEvent");
    }

    @Test
    @DisplayName("Should capture granular CompensationStepStarted and CompensationStepCompleted events during Saga rollback")
    void testGranularCompensationStepEvents() {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());
        List<String> compensated = new ArrayList<>();

        try (OrchestrationExecutor<SimpleContext, OrchFlowState, SimpleContext, String> executor =
                OrchestrationBuilder.<SimpleContext, OrchFlowState, SimpleContext, String>create("SagaCompensationMachine", OrchFlowState.class)
                        .context(SimpleContext::new)
                        .initialState(OrchFlowState.INIT)
                        .endStates(OrchFlowState.DONE)
                        .eventListener(events::add)
                        .state(OrchFlowState.INIT)
                            .action(ctx -> {
                                ctx.status = "STEP_1_OK";
                                return ctx;
                            })
                            .compensate(ctx -> {
                                compensated.add("INIT");
                                ctx.status = "INIT_COMPENSATED";
                                return ctx;
                            })
                            .transition(OrchFlowState.FINALIZE)
                        .state(OrchFlowState.FINALIZE)
                            .action(_ -> {
                                throw new IllegalStateException("Simulated step failure triggering Saga rollback");
                            })
                            .transition(OrchFlowState.DONE)
                        .buildExecutor()) {

            assertThrows(Exception.class, () -> executor.dispatchSync(null));

            boolean hasCompStarted = events.stream().anyMatch(e -> e instanceof CompensationStepStartedEvent cs
                    && cs.stateName().equals("INIT") && !cs.isRouted());
            boolean hasCompCompleted = events.stream().anyMatch(e -> e instanceof CompensationStepCompletedEvent cc
                    && cc.stateName().equals("INIT") && cc.duration() != null);
            boolean hasTurnCompensated = events.stream().anyMatch(e -> e instanceof TurnCompensatedEvent tc
                    && tc.compensatedStates().contains("INIT"));

            assertTrue(hasCompStarted, "Must emit CompensationStepStartedEvent for INIT state");
            assertTrue(hasCompCompleted, "Must emit CompensationStepCompletedEvent for INIT state");
            assertTrue(hasTurnCompensated, "Must emit TurnCompensatedEvent after compensations finish");
            assertEquals(List.of("INIT"), compensated);
        }
    }

    @Test
    @DisplayName("Should capture ParallelForkStarted, ParallelBranchCompleted, and ParallelJoinCompleted events")
    void testParallelForkJoinEvents() throws Exception {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());

        try (OrchestrationExecutor<SimpleContext, OrchFlowState, SimpleContext, String> executor =
                OrchestrationBuilder.<SimpleContext, OrchFlowState, SimpleContext, String>create("ParallelMachine", OrchFlowState.class)
                        .context(SimpleContext::new)
                        .initialState(OrchFlowState.INIT)
                        .endStates(OrchFlowState.DONE)
                        .eventListener(events::add)
                        .state(OrchFlowState.INIT)
                            .parallel()
                                .branch("BranchA", ctx -> {
                                    ctx.count += 10;
                                    return ctx;
                                })
                                .branch("BranchB", ctx -> {
                                    ctx.count += 20;
                                    return ctx;
                                })
                            .transition(OrchFlowState.DONE)
                        .buildExecutor()) {

            executor.dispatchSync(null);

            boolean hasForkStarted = events.stream().anyMatch(e -> e instanceof ParallelForkStartedEvent pf
                    && pf.branchNames().containsAll(List.of("BranchA", "BranchB")));
            boolean hasBranchACompleted = events.stream().anyMatch(e -> e instanceof ParallelBranchCompletedEvent pb
                    && pb.branchName().equals("BranchA"));
            boolean hasBranchBCompleted = events.stream().anyMatch(e -> e instanceof ParallelBranchCompletedEvent pb
                    && pb.branchName().equals("BranchB"));
            boolean hasJoinCompleted = events.stream().anyMatch(e -> e instanceof ParallelJoinCompletedEvent pj
                    && pj.totalBranches() == 2);

            assertTrue(hasForkStarted, "Must emit ParallelForkStartedEvent");
            assertTrue(hasBranchACompleted, "Must emit ParallelBranchCompletedEvent for BranchA");
            assertTrue(hasBranchBCompleted, "Must emit ParallelBranchCompletedEvent for BranchB");
            assertTrue(hasJoinCompleted, "Must emit ParallelJoinCompletedEvent");
        }
    }

    @Test
    @DisplayName("Should capture ActionExecutedEvent and FSM loop limit StateVisitLimitExceededEvent")
    void testActionExecutedAndGuardrailEvents() throws Exception {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());

        StateMachineConfiguration<SimpleContext, SimpleState, Integer, String> machine =
                AtomicStateMachineBuilder.<SimpleContext, SimpleState, Integer, String>create("LoopGuardedMachine", SimpleState.class)
                        .context(SimpleContext::new)
                        .initialState(SimpleState.START)
                        .endStates(SimpleState.COMPLETED)
                        .eventListener(events::add)
                        .state(SimpleState.START)
                            .maxVisits(2, SimpleState.COMPLETED)
                            .action(ctx -> {
                                ctx.count++;
                                return ctx;
                            })
                            .transition(SimpleState.START) // loop back to START
                        .build();

        AbstractStateMachineCallable.executeDirect(UUID.randomUUID(), machine, null, 0);

        boolean hasActionExecuted = events.stream().anyMatch(e -> e instanceof ActionExecutedEvent ae
                && ae.stateName().equals("START"));
        boolean hasLoopLimitExceeded = events.stream().anyMatch(e -> e instanceof StateVisitLimitExceededEvent sle
                && sle.stateName().equals("START") && "COMPLETED".equals(sle.fallbackState()));

        assertTrue(hasActionExecuted, "Must emit ActionExecutedEvent");
        assertTrue(hasLoopLimitExceeded, "Must emit StateVisitLimitExceededEvent when visit limit exceeded");
    }
}
