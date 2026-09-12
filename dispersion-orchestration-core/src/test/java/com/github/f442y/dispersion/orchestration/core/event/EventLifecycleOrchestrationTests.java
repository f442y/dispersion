package com.github.f442y.dispersion.orchestration.core.event;

import com.github.f442y.dispersion.fsm.StateMachine;
import com.github.f442y.dispersion.fsm.StateMachineFuture;
import com.github.f442y.dispersion.fsm.config.*;
import com.github.f442y.dispersion.fsm.context.*;
import com.github.f442y.dispersion.fsm.exception.*;
import com.github.f442y.dispersion.fsm.executor.*;
import com.github.f442y.dispersion.fsm.state.*;
import com.github.f442y.dispersion.fsm.core.*;
import com.github.f442y.dispersion.fsm.core.atomic.*;
import com.github.f442y.dispersion.fsm.core.builder.*;
import com.github.f442y.dispersion.event.*;
import com.github.f442y.dispersion.event.dispatcher.*;
import com.github.f442y.dispersion.orchestration.*;
import com.github.f442y.dispersion.orchestration.batch.*;
import com.github.f442y.dispersion.orchestration.command.*;
import com.github.f442y.dispersion.orchestration.messaging.*;
import com.github.f442y.dispersion.orchestration.core.*;
import com.github.f442y.dispersion.orchestration.core.batch.*;
import com.github.f442y.dispersion.orchestration.core.messaging.*;

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
        assertInstanceOf(ExecutionEvent.TurnStartedEvent.class, events.get(0));

        ExecutionEvent.TurnStartedEvent start = (ExecutionEvent.TurnStartedEvent) events.get(0);
        assertEquals("SimpleAtomicMachine", start.machineName());
        assertEquals(execId, start.machineId());

        boolean hasStateEntered = events.stream().anyMatch(e -> e instanceof ExecutionEvent.StateEnteredEvent see && see.stateName().equals("START"));
        boolean hasStateExited = events.stream().anyMatch(e -> e instanceof ExecutionEvent.StateExitedEvent see && see.stateName().equals("START"));
        boolean hasTransition = events.stream().anyMatch(e -> e instanceof ExecutionEvent.TransitionEvaluatedEvent tee && tee.sourceState().equals("START") && tee.targetState().equals("PROCESS"));
        boolean hasTurnCompleted = events.stream().anyMatch(e -> e instanceof ExecutionEvent.TurnCompletedEvent tce && tce.finalStateName().equals("COMPLETED"));

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

        try (OrchestrationStateMachineExecutor<SimpleContext, OrchFlowState, SimpleContext, String> executor =
                OrchestrationStateMachineBuilder.<SimpleContext, OrchFlowState, SimpleContext, String>create("OrderOrchestrator", OrchFlowState.class)
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

            assertTrue(events.stream().anyMatch(e -> e instanceof ExecutionEvent.SignalAwaitedEvent sae && sae.expectedSignal().equals("PaymentSignal")));
            assertTrue(events.stream().anyMatch(e -> e instanceof ExecutionEvent.TurnSuspendedEvent tse && tse.stateName().equals("WAIT_SIGNAL")));

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

            assertTrue(events.stream().anyMatch(e -> e instanceof ExecutionEvent.SignalDeliveredEvent sde && sde.signalName().equals("PaymentSignal")));
            assertTrue(events.stream().anyMatch(e -> e instanceof ExecutionEvent.TurnCompletedEvent tce && tce.finalStateName().equals("DONE")));

            // 3. Resume Turn with DUPLICATE CommandEnvelope -> Should trigger CommandDeduplicatedEvent
            events.clear();
            CompletableFuture<OrchestrationTurnResult<SimpleContext, OrchFlowState, String>> dupFuture =
                    executor.handleCommand(envelope);
            OrchestrationTurnResult<SimpleContext, OrchFlowState, String> turn3 = dupFuture.get();
            assertNotNull(turn3);

            assertTrue(events.stream().anyMatch(e -> e instanceof ExecutionEvent.CommandDeduplicatedEvent cde && cde.commandId().equals(commandId)));
        }
    }

    @Test
    @DisplayName("Should capture TurnCompensatedEvent during Saga compensation rollback")
    void testSagaCompensationRollbackLifecycleEvent() {
        List<ExecutionEvent> events = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger compensatedCount = new AtomicInteger(0);

        try (OrchestrationStateMachineExecutor<SimpleContext, OrchFlowState, Void, String> executor =
                OrchestrationStateMachineBuilder.<SimpleContext, OrchFlowState, Void, String>create("SagaFaultMachine", OrchFlowState.class)
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
            ExecutionEvent.TurnCompensatedEvent compEvent = events.stream()
                    .filter(e -> e instanceof ExecutionEvent.TurnCompensatedEvent)
                    .map(e -> (ExecutionEvent.TurnCompensatedEvent) e)
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
                new ExecutionEvent.TurnStartedEvent(id, "TestM", "CORR", now),
                new ExecutionEvent.TurnSuspendedEvent(id, "TestM", "WAIT", "Sig1", "CORR", dur, now),
                new ExecutionEvent.TurnCompletedEvent(id, "TestM", "DONE", "CORR", dur, now),
                new ExecutionEvent.TurnCompensatedEvent(id, "TestM", "FAIL", List.of("WAIT"), new RuntimeException(), "CORR", dur, now),
                new ExecutionEvent.StateEnteredEvent(id, "TestM", "STATE_A", now),
                new ExecutionEvent.StateExitedEvent(id, "TestM", "STATE_A", dur, now),
                new ExecutionEvent.TransitionEvaluatedEvent(id, "TestM", "STATE_A", "STATE_B", now),
                new ExecutionEvent.SignalAwaitedEvent(id, "TestM", "WAIT", "Sig1", "CORR", now),
                new ExecutionEvent.SignalDeliveredEvent(id, "TestM", "WAIT", "Sig1", "CORR", now),
                new ExecutionEvent.CommandDeduplicatedEvent(id, "TestM", id, "CORR", now),
                new ExecutionEvent.BatchBarrierReachedEvent(id, "TestM", "item-1", "BATCH_WAIT", now),
                new ExecutionEvent.BatchBarrierUnlockedEvent(id, "TestM", "BATCH_WAIT", 10, now)
        );

        for (ExecutionEvent event : testEvents) {
            // Java 25 exhaustive switch without default branch
            String description = switch (event) {
                case ExecutionEvent.TurnStartedEvent tse -> "STARTED: " + tse.machineName();
                case ExecutionEvent.TurnSuspendedEvent tse -> "SUSPENDED: " + tse.stateName();
                case ExecutionEvent.TurnCompletedEvent tce -> "COMPLETED: " + tce.finalStateName();
                case ExecutionEvent.TurnCompensatedEvent tce -> "COMPENSATED: " + tce.failedStateName();
                case ExecutionEvent.StateEnteredEvent see -> "ENTERED: " + see.stateName();
                case ExecutionEvent.StateExitedEvent see -> "EXITED: " + see.stateName();
                case ExecutionEvent.TransitionEvaluatedEvent tee -> "TRANSITION: " + tee.sourceState() + "->" + tee.targetState();
                case ExecutionEvent.SignalAwaitedEvent sae -> "AWAITING: " + sae.expectedSignal();
                case ExecutionEvent.SignalDeliveredEvent sde -> "DELIVERED: " + sde.signalName();
                case ExecutionEvent.CommandDeduplicatedEvent cde -> "DEDUP: " + cde.commandId();
                case ExecutionEvent.BatchBarrierReachedEvent bbe -> "BARRIER: " + bbe.itemKey();
                case ExecutionEvent.BatchBarrierUnlockedEvent bue -> "UNLOCKED: " + bue.itemCount();
            };
            assertNotNull(description);
        }
    }
}
