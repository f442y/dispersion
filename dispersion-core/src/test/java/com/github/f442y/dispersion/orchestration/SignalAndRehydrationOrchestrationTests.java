package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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

        var executor = OrchestrationStateMachineBuilder.<OrderContext, OrderState, OrderContext, String>create("OrderOrchestrator", OrderState.class)
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
                    .compensate(ctx -> {
                        ctx.inventoryReleased = true;
                        ctx.log.add("COMPENSATE_INVENTORY");
                        return ctx;
                    })
                    .transition(OrderState.AWAIT_PAYMENT_SIGNAL)
                .state(OrderState.AWAIT_PAYMENT_SIGNAL)
                    .waitForSignal("PAYMENT_CONFIRMED", PaymentSignalPayload.class, (ctx, payload) -> {
                        ctx.transactionId = payload.transactionId();
                        ctx.log.add("PAYMENT_PROCESSED:" + payload.transactionId());
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
                .output(ctx -> "Order " + ctx.orderId + " completed with tx " + ctx.transactionId)
                .buildExecutor();

        // 1. Dispatch initial turn
        OrderContext input = new OrderContext();
        input.orderId = "ORD-001";
        input.amount = 5000;

        OrchestrationTurnResult<OrderContext, OrderState, String> firstTurn = executor.dispatchTurnSync(null, input);

        // Verify it paused at AWAIT_PAYMENT_SIGNAL
        assertTrue(firstTurn.isSuspended());
        assertEquals(OrderState.AWAIT_PAYMENT_SIGNAL, firstTurn.currentStateKey());
        assertEquals("PAYMENT_CONFIRMED", firstTurn.expectedSignal());
        assertNotNull(firstTurn.context());
        assertTrue(firstTurn.context().inventoryReserved);
        assertFalse(firstTurn.context().orderFulfilled);

        UUID machineId = firstTurn.machineId();

        // Verify checkpoint was persisted in store
        Optional<OrchestrationCheckpoint<OrderContext, OrderState>> savedCheckpoint = store.findById(machineId);
        assertTrue(savedCheckpoint.isPresent());
        assertEquals(OrchestrationStatus.SUSPENDED, savedCheckpoint.get().status());
        assertEquals(OrderState.AWAIT_PAYMENT_SIGNAL, savedCheckpoint.get().currentStateKey());
        assertEquals("PAYMENT_CONFIRMED", savedCheckpoint.get().expectedSignal());

        // 2. Deliver external signal (e.g. webhook)
        PaymentSignalPayload paymentPayload = new PaymentSignalPayload("TX-9988", 5000);
        CompletableFuture<OrchestrationTurnResult<OrderContext, OrderState, String>> resumeFuture =
                executor.sendSignal(machineId, "PAYMENT_CONFIRMED", paymentPayload);

        OrchestrationTurnResult<OrderContext, OrderState, String> secondTurn = resumeFuture.get();

        // Verify it resumed and completed successfully
        assertTrue(secondTurn.isCompleted());
        assertEquals("Order ORD-001 completed with tx TX-9988", secondTurn.output());
        assertNotNull(secondTurn.context());
        assertTrue(secondTurn.context().orderFulfilled);
        assertEquals("TX-9988", secondTurn.context().transactionId);
        assertEquals(List.of("VALIDATE", "RESERVE_INVENTORY", "PAYMENT_PROCESSED:TX-9988", "FULFILL"), secondTurn.context().log);

        executor.close();
    }

    /**
     * Tests delivering external signal using a domain correlation key (e.g. orderId).
     */
    @Test
    public void testSignalDeliveryByCorrelationKey() throws Exception {
        InMemoryCheckpointStore<OrderContext, OrderState> store = new InMemoryCheckpointStore<>();

        var executor = OrchestrationStateMachineBuilder.<OrderContext, OrderState, OrderContext, String>create("CorrelatedOrderOrchestrator", OrderState.class)
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
                    .waitForSignal("PAYMENT_RECEIVED", PaymentSignalPayload.class, (ctx, payload) -> {
                        ctx.transactionId = payload.transactionId();
                        return ctx;
                    })
                    .transition(OrderState.COMPLETED)
                .endStates(OrderState.COMPLETED)
                .output(ctx -> "Processed " + ctx.orderId)
                .buildExecutor();

        OrderContext initial = new OrderContext();
        initial.orderId = "ORDER-CORR-42";
        initial.amount = 1000;

        OrchestrationTurnResult<OrderContext, OrderState, String> turn1 = executor.dispatchTurnSync(null, initial);
        assertTrue(turn1.isSuspended());

        // Check correlation index
        Optional<OrchestrationCheckpoint<OrderContext, OrderState>> cp = store.findByCorrelationKey("ORDER-CORR-42");
        assertTrue(cp.isPresent());
        assertEquals(turn1.machineId(), cp.get().machineId());
        assertEquals("ORDER-CORR-42", cp.get().correlationKey());

        // Deliver signal using correlation key
        CompletableFuture<OrchestrationTurnResult<OrderContext, OrderState, String>> future =
                executor.sendSignalByCorrelationKey("ORDER-CORR-42", "PAYMENT_RECEIVED", new PaymentSignalPayload("TX-42", 1000));

        OrchestrationTurnResult<OrderContext, OrderState, String> turn2 = future.get();
        assertTrue(turn2.isCompleted());
        assertEquals("Processed ORDER-CORR-42", turn2.output());

        executor.close();
    }

    /**
     * Tests Saga compensation rollbacks spanning multiple turns across dehydration/rehydration boundaries.
     */
    @Test
    public void testSagaCompensationRollbackAcrossMultipleTurns() throws Exception {
        InMemoryCheckpointStore<OrderContext, OrderState> store = new InMemoryCheckpointStore<>();

        var executor = OrchestrationStateMachineBuilder.<OrderContext, OrderState, OrderContext, String>create("SagaRollbackOrchestrator", OrderState.class)
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
