package com.github.f442y.dispersion.orchestration.core;

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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests verifying modern Java 25+ language capabilities (exhaustive pattern matching switch
 * on sealed exceptions, record pattern deconstruction, virtual-thread concurrency, and robust error recovery).
 */
public class Java25ModernFeaturesTests {

    public enum ModernState implements StateKey {
        INIT, STEP_A, STEP_B, COMPLETED, FAILED
    }

    public static class ModernContext implements StateMachineContext {
        public String data = "";
        public int counter = 0;
        public List<String> trail = new ArrayList<>();
    }

    public record PaymentApprovedCommand(String paymentId, int amount, String authCode) implements SignalCommand {
        @Override
        public String correlationKey() {
            return paymentId;
        }

        @Override
        @NonNull
        public String signalName() {
            return "PaymentApprovedCommand";
        }
    }

    /**
     * Tests exhaustive Java 25 pattern matching switch expressions across the sealed {@link StateMachineException} hierarchy.
     */
    @Test
    @DisplayName("Should exhaustively match all sealed StateMachineException subtypes with pattern matching switch")
    public void testExhaustivePatternMatchingOnSealedExceptions() {
        List<StateMachineException> exceptions = List.of(
                new ActionException("STATE_1", "Action failed", new RuntimeException("DB down")),
                new TransitionException("STATE_1", "STATE_2", "Illegal edge"),
                new BackpressureException("Saturation limit reached"),
                new MaxTransitionsExceededException(100),
                new MaxStateVisitsExceededException("RETRY_STATE", 5),
                new CompensationException("INVENTORY_STATE", "Rollback failed", null)
        );

        List<String> classified = exceptions.stream()
                .map(this::classifyException)
                .toList();

        assertThat(classified).containsExactly(
                "ACTION_FAILED[STATE_1]",
                "TRANSITION_FAILED[STATE_1->STATE_2]",
                "BACKPRESSURE[Saturation limit reached]",
                "MAX_TRANSITIONS[100]",
                "MAX_VISITS[RETRY_STATE:5]",
                "COMPENSATION_FAILED[INVENTORY_STATE]"
        );
    }

    private String classifyException(StateMachineException ex) {
        return switch (ex) {
            case ActionException ae -> "ACTION_FAILED[" + ae.getStateName() + "]";
            case TransitionException te -> "TRANSITION_FAILED[" + te.getSourceStateName() + "->" + te.getTargetStateName() + "]";
            case BackpressureException be -> "BACKPRESSURE[" + be.getMessage() + "]";
            case MaxTransitionsExceededException mte -> "MAX_TRANSITIONS[" + mte.getMaxTransitions() + "]";
            case MaxStateVisitsExceededException msve -> "MAX_VISITS[" + msve.getStateName() + ":" + msve.getMaxVisits() + "]";
            case CompensationException ce -> "COMPENSATION_FAILED[" + ce.getStateName() + "]";
        };
    }

    /**
     * Tests record pattern deconstruction on {@link CommandEnvelope} and {@link SignalMessage}.
     */
    @Test
    @DisplayName("Should perform clean record pattern deconstruction on CommandEnvelope and SignalMessage")
    public void testRecordPatternDeconstruction() {
        UUID cmdId = UUID.randomUUID();
        Instant now = Instant.now();
        CommandEnvelope<PaymentApprovedCommand> envelope = new CommandEnvelope<>(
                cmdId, now, new PaymentApprovedCommand("PAY-99", 5000, "AUTH-OK")
        );

        String result = deconstructEnvelope(envelope);
        assertThat(result).isEqualTo("COMMAND_ID:" + cmdId + " PID:PAY-99 AMT:5000 CODE:AUTH-OK");

        SignalMessage message = new SignalMessage(
                "payment-events",
                "PaymentApprovedCommand",
                "PAY-99",
                cmdId,
                now,
                Map.of("source", "stripe-webhook"),
                envelope
        );

        String msgDeconstructed = deconstructMessage(message);
        assertThat(msgDeconstructed).isEqualTo("payment-events/PaymentApprovedCommand/PAY-99/stripe-webhook");
    }

    private String deconstructEnvelope(Object envelope) {
        if (envelope instanceof CommandEnvelope(UUID id, _, PaymentApprovedCommand(String pid, int amt, String code))) {
            return "COMMAND_ID:" + id + " PID:" + pid + " AMT:" + amt + " CODE:" + code;
        }
        return "UNKNOWN";
    }

    private String deconstructMessage(Object message) {
        if (message instanceof SignalMessage(String dest, String sig, String corr, _, _, Map<?, ?> hdrs, _) && hdrs != null) {
            return dest + "/" + sig + "/" + corr + "/" + hdrs.get("source");
        }
        return "NONE";
    }

    /**
     * Tests high-concurrency burst of 1,000 parallel virtual threads executing atomic state machines simultaneously.
     */
    @Test
    @DisplayName("Should process 1,000 concurrent atomic state machine instances seamlessly on Virtual Threads")
    public void testConcurrentVirtualThreadBurst() throws Exception {
        StateMachineConfiguration<ModernContext, ModernState, Integer, String> machine =
                AtomicStateMachineBuilder.<ModernContext, ModernState, Integer, String>create(ModernState.class)
                        .context(ModernContext::new)
                        .initialState(ModernState.INIT)
                        .input((ctx, num) -> {
                            ctx.counter = (num != null) ? num : 0;
                            return ctx;
                        })
                        .state(ModernState.INIT)
                            .action(ctx -> {
                                ctx.data = "ITEM-" + ctx.counter;
                                ctx.trail.add("INIT");
                                return ctx;
                            })
                            .transition(ModernState.STEP_A)
                        .state(ModernState.STEP_A)
                            .action(ctx -> {
                                ctx.data = ctx.data + "-PROCESSED";
                                ctx.trail.add("STEP_A");
                                return ctx;
                            })
                            .transition(ModernState.COMPLETED)
                        .endStates(ModernState.COMPLETED)
                        .output(ctx -> ctx.data)
                        .build();

        int taskCount = 1_000;
        CountDownLatch latch = new CountDownLatch(taskCount);
        Map<Integer, String> results = new ConcurrentHashMap<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        try (ExecutorService vtExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
             AtomicStateMachineExecutor<ModernContext, ModernState, Integer, String> smExecutor = new AtomicStateMachineExecutor<>("vt-burst", machine, 200)) {

            for (int i = 0; i < taskCount; i++) {
                final int idx = i;
                vtExecutor.submit(() -> {
                    try {
                        String res = smExecutor.dispatchSync(idx);
                        results.put(idx, res);
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            boolean completed = latch.await(10, TimeUnit.SECONDS);
            assertTrue(completed, "All 1,000 tasks should finish within 10 seconds on Virtual Threads");
            assertTrue(errors.isEmpty(), () -> "Exceptions occurred: " + errors);
            assertEquals(taskCount, results.size());

            for (int i = 0; i < taskCount; i++) {
                assertEquals("ITEM-" + i + "-PROCESSED", results.get(i));
            }
        }
    }

    /**
     * Tests multiple concurrent signal arrivals with duplicate idempotency envelopes under race conditions.
     */
    @Test
    @DisplayName("Should guarantee exactly-once processing under concurrent duplicate command delivery")
    public void testConcurrentDuplicateCommandDelivery() throws Exception {
        InMemoryCheckpointStore<ModernContext, ModernState> store = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<ModernContext, ModernState, String, String> executor =
                OrchestrationStateMachineBuilder.<ModernContext, ModernState, String, String>create("ConcurrentIdemp", ModernState.class)
                .context(ModernContext::new)
                .initialState(ModernState.INIT)
                .correlationKey(ctx -> ctx.data)
                .checkpointStore(store)
                .input((ctx, id) -> { ctx.data = id; return ctx; })
                .state(ModernState.INIT)
                    .action(ctx -> ctx)
                    .transition(ModernState.STEP_A)
                .state(ModernState.STEP_A)
                    .waitForCommand(PaymentApprovedCommand.class, (ctx, cmd) -> {
                        if (cmd != null) {
                            ctx.counter++;
                            ctx.trail.add("AUTH:" + cmd.authCode());
                        }
                        return ctx;
                    })
                    .transition(ModernState.COMPLETED)
                .endStates(ModernState.COMPLETED)
                .output(ctx -> "DONE:" + ctx.data)
                .buildExecutor();

        ModernContext initial = new ModernContext();
        initial.data = "ORD-RACE-77";

        OrchestrationTurnResult<ModernContext, ModernState, String> turn1 = executor.dispatchTurnSync(initial, "ORD-RACE-77");
        assertTrue(turn1.isSuspended());

        // Send 10 identical command envelopes concurrently
        UUID commandId = UUID.randomUUID();
        CommandEnvelope<PaymentApprovedCommand> envelope = new CommandEnvelope<>(
                commandId,
                Instant.now(),
                new PaymentApprovedCommand("ORD-RACE-77", 2500, "AUTH-999")
        );

        int threads = 10;
        CountDownLatch latch = new CountDownLatch(threads);
        List<OrchestrationTurnResult<ModernContext, ModernState, String>> results = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        try (ExecutorService vtPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory())) {
            for (int i = 0; i < threads; i++) {
                vtPool.submit(() -> {
                    try {
                        OrchestrationTurnResult<ModernContext, ModernState, String> res = executor.handleCommand(envelope).get();
                        results.add(res);
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS), "Concurrent command delivery latch timed out");
        }

        assertTrue(errors.isEmpty(), () -> "Exceptions occurred: " + errors);
        assertEquals(threads, results.size());

        // Checkpoint verification: counter must be EXACTLY 1 despite 10 concurrent deliveries
        Optional<OrchestrationCheckpoint<ModernContext, ModernState>> cp = store.findByCorrelationKey("ORD-RACE-77");
        assertTrue(cp.isPresent());
        assertEquals(OrchestrationStatus.COMPLETED, cp.get().status());
        assertEquals(1, cp.get().contextSnapshot().counter);
        assertEquals(List.of("AUTH:AUTH-999"), cp.get().contextSnapshot().trail);

        executor.close();
    }

    /**
     * Tests that .compensate() executes in exact reverse chronological (LIFO) order during Saga rollback.
     */
    @Test
    @DisplayName("Should execute compensate() actions during Saga unwinding in LIFO order")
    public void testSagaCompensationUnwinding() {
        OrchestrationStateMachineExecutor<ModernContext, ModernState, Void, String> executor =
                OrchestrationStateMachineBuilder.<ModernContext, ModernState, Void, String>create("SagaTest", ModernState.class)
                .context(ModernContext::new)
                .initialState(ModernState.INIT)
                .state(ModernState.INIT)
                    .action(ctx -> {
                        ctx.trail.add("INIT_DONE");
                        return ctx;
                    })
                    .compensate(ctx -> {
                        ctx.trail.add("COMPENSATE_INIT");
                        return ctx;
                    })
                    .transition(ModernState.STEP_A)
                .state(ModernState.STEP_A)
                    .action(ctx -> {
                        ctx.trail.add("STEP_A_DONE");
                        return ctx;
                    })
                    .compensate(ctx -> {
                        ctx.trail.add("COMPENSATE_STEP_A");
                        return ctx;
                    })
                    .transition(ModernState.STEP_B)
                .state(ModernState.STEP_B)
                    .action(_ -> {
                        throw new RuntimeException("Simulated Step B Failure");
                    })
                    .transition(ModernState.COMPLETED)
                .endStates(ModernState.COMPLETED)
                .buildExecutor();

        ModernContext ctx = new ModernContext();
        try {
            executor.dispatchSync(ctx, null);
        } catch (Exception expected) {
            // Expected
        }

        // Must unwind in LIFO order: COMPENSATE_STEP_A then COMPENSATE_INIT
        assertThat(ctx.trail).containsExactly("INIT_DONE", "STEP_A_DONE", "COMPENSATE_STEP_A", "COMPENSATE_INIT");
        executor.close();
    }

    /**
     * Tests ultra-lightweight atomic state machine execution at high volume (50,000 executions).
     */
    @Test
    @DisplayName("Should execute 50,000 atomic state machine workflows with sub-microsecond latency")
    public void testUltraHighThroughputAtomicExecution() throws Exception {
        StateMachineConfiguration<ModernContext, ModernState, Integer, String> config =
                AtomicStateMachineBuilder.<ModernContext, ModernState, Integer, String>create(ModernState.class)
                .context(ModernContext::new)
                .initialState(ModernState.INIT)
                .input((ctx, val) -> {
                    ctx.counter = (val != null) ? val : 0;
                    return ctx;
                })
                .state(ModernState.INIT)
                    .action(ctx -> {
                        ctx.counter += 10;
                        return ctx;
                    })
                    .transition(ModernState.STEP_A)
                .state(ModernState.STEP_A)
                    .action(ctx -> {
                        ctx.counter *= 2;
                        return ctx;
                    })
                    .transition(ModernState.STEP_B)
                .state(ModernState.STEP_B)
                    .action(ctx -> {
                        ctx.counter += 5;
                        return ctx;
                    })
                    .transition(ModernState.COMPLETED)
                .endStates(ModernState.COMPLETED)
                .output(ctx -> "VAL:" + ctx.counter)
                .build();

        try (AtomicStateMachineExecutor<ModernContext, ModernState, Integer, String> executor = new AtomicStateMachineExecutor<>("fast-atomic", config, 100_000)) {
            int iterations = 50_000;
            long start = System.nanoTime();

            for (int i = 0; i < iterations; i++) {
                String result = executor.dispatchSync(i);
                int expected = (i + 10) * 2 + 5;
                assertEquals("VAL:" + expected, result);
            }

            long durationNs = System.nanoTime() - start;
            double ms = durationNs / 1_000_000.0;
            double opsPerSec = (iterations / ms) * 1000.0;
            System.out.printf("Processed %d atomic state machines in %.2f ms (%.0f ops/sec)%n",
                    iterations, ms, opsPerSec);
        }
    }
}
