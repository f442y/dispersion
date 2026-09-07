package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SignalAndRehydrationOrchestrationTests {

    public enum OrderState implements StateKey {
        VALIDATE,
        RESERVE_INVENTORY,
        AWAIT_PAYMENT_SIGNAL,
        FULFILL,
        COMPLETED,
        FAILED
    }

    public static class OrderContext implements StateMachineContext {
        public String orderId;
        public int amount;
        public String transactionId;
        public boolean inventoryReserved = false;
        public boolean inventoryReleased = false;
        public boolean orderFulfilled = false;
        public List<String> log = new ArrayList<>();
    }

    public record PaymentSignalPayload(String transactionId, int amountPaid) {}

    /**
     * Tests turn-based execution suspending at a signal state, saving checkpoint, and rehydrating by machine ID.
     */
    @Test
    public void testSuspendAndResumeWithSignalByMachineId() throws Exception {
        InMemoryCheckpointStore<OrderContext, OrderState> store = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<OrderContext, OrderState, OrderContext, String> executor =
                OrchestrationStateMachineBuilder.<OrderContext, OrderState, OrderContext, String>create("OrderOrchestrator", OrderState.class)
                .context(OrderContext::new)
                .initialState(OrderState.VALIDATE)
                .checkpointStore(store)
                .input((ctx, input) -> {
                    if (input != null) {
                        ctx.orderId = input.orderId;
                        ctx.amount = input.amount;
                    }
                    return ctx;
                })
                .state(OrderState.VALIDATE)
                    .action(ctx -> {
                        ctx.log.add("VALIDATE");
                        return ctx;
                    })
                    .transition(OrderState.RESERVE_INVENTORY)
                .state(OrderState.RESERVE_INVENTORY)
                    .action(ctx -> {
                        ctx.inventoryReserved = true;
                        ctx.log.add("RESERVE_INVENTORY");
                        return ctx;
                    })
                    .transition(OrderState.AWAIT_PAYMENT_SIGNAL)
                .state(OrderState.AWAIT_PAYMENT_SIGNAL)
                    .waitForSignal("PAYMENT_CONFIRMED", PaymentSignalPayload.class, (ctx, payload) -> {
                        Objects.requireNonNull(payload, "payload must not be null");
                        ctx.transactionId = payload.transactionId();
                        ctx.log.add("PAYMENT_CONFIRMED:" + payload.transactionId());
                        return ctx;
                    })
                    .transition(OrderState.FULFILL)
                .state(OrderState.FULFILL)
                    .action(ctx -> {
                        ctx.orderFulfilled = true;
                        ctx.log.add("FULFILL");
                        return ctx;
                    })
                    .transition(OrderState.COMPLETED)
                .endStates(OrderState.COMPLETED, OrderState.FAILED)
                .output(ctx -> "Order " + ctx.orderId + " fulfilled successfully with tx " + ctx.transactionId)
                .buildExecutor();

        OrderContext input = new OrderContext();
        input.orderId = "ORD-90210";
        input.amount = 450;

        // --- Turn 1: Starts workflow, executes VALIDATE & RESERVE_INVENTORY, pauses at AWAIT_PAYMENT_SIGNAL ---
        OrchestrationTurnResult<OrderContext, OrderState, String> turn1Result = executor.dispatchTurnSync(null, input);

        assertNotNull(turn1Result.machineId());
        assertTrue(turn1Result.isSuspended());
        assertEquals(OrderState.AWAIT_PAYMENT_SIGNAL, turn1Result.currentStateKey());
        assertEquals("PAYMENT_CONFIRMED", turn1Result.expectedSignal());
        assertTrue(turn1Result.context().inventoryReserved);
        assertFalse(turn1Result.context().orderFulfilled);

        // Verify checkpoint was persisted into store
        Optional<OrchestrationCheckpoint<OrderContext, OrderState>> savedCp = store.findById(turn1Result.machineId());
        assertTrue(savedCp.isPresent());
        assertEquals(OrchestrationStatus.SUSPENDED, savedCp.get().status());
        assertEquals(OrderState.AWAIT_PAYMENT_SIGNAL, savedCp.get().currentStateKey());

        // --- Turn 2: External signal arrives with payment confirmation ---
        CompletableFuture<OrchestrationTurnResult<OrderContext, OrderState, String>> resumeFuture =
                executor.sendSignal(turn1Result.machineId(), "PAYMENT_CONFIRMED", new PaymentSignalPayload("TX-4482-OK", 450));

        OrchestrationTurnResult<OrderContext, OrderState, String> turn2Result = resumeFuture.get();

        assertTrue(turn2Result.isCompleted());
        assertEquals(OrderState.COMPLETED, turn2Result.currentStateKey());
        assertEquals("Order ORD-90210 fulfilled successfully with tx TX-4482-OK", turn2Result.output());
        assertTrue(turn2Result.context().orderFulfilled);
        assertEquals("TX-4482-OK", turn2Result.context().transactionId);
        assertEquals(
                List.of("VALIDATE", "RESERVE_INVENTORY", "PAYMENT_CONFIRMED:TX-4482-OK", "FULFILL"),
                turn2Result.context().log
        );

        // Verify final checkpoint state is COMPLETED
        Optional<OrchestrationCheckpoint<OrderContext, OrderState>> finalCp = store.findById(turn1Result.machineId());
        assertTrue(finalCp.isPresent());
        assertEquals(OrchestrationStatus.COMPLETED, finalCp.get().status());

        executor.close();
    }

    /**
     * Tests signal delivery correlated via domain business key (e.g. orderId).
     */
    @Test
    public void testSignalDeliveryByCorrelationKey() throws Exception {
        InMemoryCheckpointStore<OrderContext, OrderState> store = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<OrderContext, OrderState, OrderContext, String> executor =
                OrchestrationStateMachineBuilder.<OrderContext, OrderState, OrderContext, String>create("CorrelatedOrderOrchestrator", OrderState.class)
                .context(OrderContext::new)
                .initialState(OrderState.VALIDATE)
                .correlationKey(ctx -> ctx.orderId)
                .checkpointStore(store)
                .input((ctx, input) -> {
                    if (input != null) {
                        ctx.orderId = input.orderId;
                        ctx.amount = input.amount;
                    }
                    return ctx;
                })
                .state(OrderState.VALIDATE)
                    .action(ctx -> ctx)
                    .transition(OrderState.AWAIT_PAYMENT_SIGNAL)
                .state(OrderState.AWAIT_PAYMENT_SIGNAL)
                    .waitForSignal("PAYMENT_CONFIRMED", PaymentSignalPayload.class, (ctx, payload) -> {
                        Objects.requireNonNull(payload, "payload must not be null");
                        ctx.transactionId = payload.transactionId();
                        return ctx;
                    })
                    .transition(OrderState.COMPLETED)
                .endStates(OrderState.COMPLETED)
                .output(ctx -> "Processed " + ctx.orderId)
                .buildExecutor();

        OrderContext input = new OrderContext();
        input.orderId = "ORDER-BIZ-KEY-123";

        OrchestrationTurnResult<OrderContext, OrderState, String> turn1 = executor.dispatchTurnSync(null, input);
        assertTrue(turn1.isSuspended());

        // Signal delivered by domain correlationKey, not machineId
        CompletableFuture<OrchestrationTurnResult<OrderContext, OrderState, String>> future =
                executor.sendSignalByCorrelationKey("ORDER-BIZ-KEY-123", "PAYMENT_CONFIRMED", new PaymentSignalPayload("TX-CORR-1", 100));

        OrchestrationTurnResult<OrderContext, OrderState, String> turn2 = future.get();
        assertTrue(turn2.isCompleted());
        assertEquals("Processed ORDER-BIZ-KEY-123", turn2.output());
        assertEquals("TX-CORR-1", turn2.context().transactionId);

        executor.close();
    }

    /**
     * Tests Saga compensation rollbacks spanning multiple turns across dehydration/rehydration boundaries.
     */
    @Test
    public void testSagaCompensationRollbackAcrossMultipleTurns() throws Exception {
        InMemoryCheckpointStore<OrderContext, OrderState> store = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<OrderContext, OrderState, OrderContext, String> executor =
                OrchestrationStateMachineBuilder.<OrderContext, OrderState, OrderContext, String>create("SagaRollbackOrchestrator", OrderState.class)
                .context(OrderContext::new)
                .initialState(OrderState.VALIDATE)
                .checkpointStore(store)
                .input((ctx, input) -> {
                    if (input != null) {
                        ctx.orderId = input.orderId;
                    }
                    return ctx;
                })
                .state(OrderState.VALIDATE)
                    .action(ctx -> {
                        ctx.log.add("VALIDATE");
                        return ctx;
                    })
                    .transition(OrderState.RESERVE_INVENTORY)
                .state(OrderState.RESERVE_INVENTORY)
                    .action(ctx -> {
                        ctx.inventoryReserved = true;
                        ctx.log.add("RESERVE_INVENTORY");
                        return ctx;
                    })
                    .compensate(ctx -> {
                        ctx.inventoryReleased = true;
                        ctx.log.add("COMPENSATE_INVENTORY");
                        return ctx;
                    })
                    .transition(OrderState.AWAIT_PAYMENT_SIGNAL)
                .state(OrderState.AWAIT_PAYMENT_SIGNAL)
                    .waitForSignal("PAYMENT_CONFIRMED", PaymentSignalPayload.class, (ctx, payload) -> {
                        Objects.requireNonNull(payload, "payload must not be null");
                        ctx.transactionId = payload.transactionId();
                        ctx.log.add("PAYMENT_APPLIED");
                        return ctx;
                    })
                    .transition(OrderState.FULFILL)
                .state(OrderState.FULFILL)
                    .action(ctx -> {
                        ctx.log.add("FULFILL_ATTEMPT");
                        throw new IllegalStateException("Warehouse fulfillment failed permanently!");
                    })
                    .transition(OrderState.COMPLETED)
                .endStates(OrderState.COMPLETED, OrderState.FAILED)
                .buildExecutor();

        OrderContext input = new OrderContext();
        input.orderId = "ORD-SAGA-FAIL";

        // Turn 1: runs VALIDATE -> RESERVE_INVENTORY -> pauses at AWAIT_PAYMENT_SIGNAL
        OrchestrationTurnResult<OrderContext, OrderState, String> turn1 = executor.dispatchTurnSync(null, input);
        assertTrue(turn1.isSuspended());
        assertTrue(turn1.context().inventoryReserved);
        assertFalse(turn1.context().inventoryReleased);

        // Turn 2: resume with signal -> advances to FULFILL -> Fails -> Unwinds RESERVE_INVENTORY compensation!
        CompletableFuture<OrchestrationTurnResult<OrderContext, OrderState, String>> resumeFuture =
                executor.sendSignal(turn1.machineId(), "PAYMENT_CONFIRMED", new PaymentSignalPayload("TX-999", 500));

        assertThrows(Exception.class, resumeFuture::get);

        // Verify compensation occurred
        Optional<OrchestrationCheckpoint<OrderContext, OrderState>> lastCp = store.findById(turn1.machineId());
        assertTrue(lastCp.isPresent());
        assertEquals(OrchestrationStatus.COMPENSATED, lastCp.get().status());
        assertTrue(lastCp.get().contextSnapshot().inventoryReleased);
        assertEquals(
                List.of("VALIDATE", "RESERVE_INVENTORY", "PAYMENT_APPLIED", "FULFILL_ATTEMPT", "COMPENSATE_INVENTORY"),
                lastCp.get().contextSnapshot().log
        );

        executor.close();
    }
}
