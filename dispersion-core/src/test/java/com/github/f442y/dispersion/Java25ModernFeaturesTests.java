package com.github.f442y.dispersion;

import com.github.f442y.dispersion.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.ActionException;
import com.github.f442y.dispersion.exception.BackpressureException;
import com.github.f442y.dispersion.exception.CompensationException;
import com.github.f442y.dispersion.exception.MaxStateVisitsExceededException;
import com.github.f442y.dispersion.exception.MaxTransitionsExceededException;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.exception.TransitionException;
import com.github.f442y.dispersion.orchestration.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineBuilder;
import com.github.f442y.dispersion.orchestration.OrchestrationStatus;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SagaCommand;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.messaging.SignalMessage;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

        List<String> classified = new ArrayList<>();

        for (StateMachineException ex : exceptions) {
            String classification = switch (ex) {
                case ActionException ae -> "ACTION_FAILED[" + ae.getStateName() + "]";
                case TransitionException te -> "TRANSITION_FAILED[" + te.getSourceStateName() + "->" + te.getTargetStateName() + "]";
                case BackpressureException be -> "BACKPRESSURE[" + be.getMessage() + "]";
                case MaxTransitionsExceededException mte -> "MAX_TRANSITIONS[" + mte.getMaxTransitions() + "]";
                case MaxStateVisitsExceededException msve -> "MAX_VISITS[" + msve.getStateName() + ":" + msve.getMaxVisits() + "]";
                case CompensationException ce -> "COMPENSATION_FAILED[" + ce.getStateName() + "]";
            };
            classified.add(classification);
        }

        assertThat(classified).containsExactly(
                "ACTION_FAILED[STATE_1]",
                "TRANSITION_FAILED[STATE_1->STATE_2]",
                "BACKPRESSURE[Saturation limit reached]",
                "MAX_TRANSITIONS[100]",
                "MAX_VISITS[RETRY_STATE:5]",
                "COMPENSATION_FAILED[INVENTORY_STATE]"
        );
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

        String result;
        if (envelope instanceof CommandEnvelope(UUID id, Instant ts, PaymentApprovedCommand(String pid, int amt, String code))) {
            result = "COMMAND_ID:" + id + " PID:" + pid + " AMT:" + amt + " CODE:" + code;
        } else {
            result = "UNKNOWN";
        }

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

        String msgDeconstructed;
        if (message instanceof SignalMessage(String dest, String sig, String corr, UUID msgId, Instant msgTs, Map<String, String> hdrs, Object payload)) {
            msgDeconstructed = dest + "/" + sig + "/" + corr + "/" + hdrs.get("source");
        } else {
            msgDeconstructed = "NONE";
        }

        assertThat(msgDeconstructed).isEqualTo("payment-events/PaymentApprovedCommand/PAY-99/stripe-webhook");
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

        try (ExecutorService vtExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());
             var smExecutor = new com.github.f442y.dispersion.atomic.AtomicStateMachineExecutor<>("vt-burst", machine, 200)) {

            for (int i = 0; i < taskCount; i++) {
                final int idx = i;
                vtExecutor.submit(() -> {
                    try {
                        String res = smExecutor.dispatchSync(idx);
                        results.put(idx, res);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            boolean completed = latch.await(10, TimeUnit.SECONDS);
            assertTrue(completed, "All 1,000 tasks should finish within 10 seconds on Virtual Threads");
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

        var executor = OrchestrationStateMachineBuilder.<ModernContext, ModernState, String, String>create("ConcurrentIdemp", ModernState.class)
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
                        ctx.counter++;
                        ctx.trail.add("AUTH:" + cmd.authCode());
                        return ctx;
                    })
                    .transition(ModernState.COMPLETED)
                .endStates(ModernState.COMPLETED)
                .output(ctx -> "Processed: " + ctx.counter)
                .buildExecutor();

        ModernContext ctx = new ModernContext();
        ctx.data = "ORD-RACE-77";

        OrchestrationTurnResult<ModernContext, ModernState, String> turn1 = executor.dispatchTurnSync(null, "ORD-RACE-77");
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

        try (ExecutorService vtPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory())) {
            for (int i = 0; i < threads; i++) {
                vtPool.submit(() -> {
                    try {
                        var res = executor.handleCommand(envelope).get();
                        results.add(res);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(5, TimeUnit.SECONDS);
        }

        assertEquals(threads, results.size());

        // Checkpoint verification: counter must be EXACTLY 1 despite 10 concurrent deliveries
        var cp = store.findByCorrelationKey("ORD-RACE-77");
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
    public void testSagaCompensationUnwinding() throws Exception {
        var executor = OrchestrationStateMachineBuilder.<ModernContext, ModernState, Void, String>create("SagaTest", ModernState.class)
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
                    .action(ctx -> {
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
}
