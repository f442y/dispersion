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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UnifiedOrchestrationStateMachineTests {

    // 1. Orchestration (Macro) State Key Enum
    public enum OrderOrchState implements StateKey {
        VALIDATE_ORDER,
        RESERVE_INVENTORY,
        CHARGE_PAYMENT,
        VIP_FAST_TRACK,
        FAILED_COMPENSATION,
        COMPLETED,
        FAILED
    }

    // 2. Atomic (Micro) State Key Enums
    public enum InventoryAtomicState implements StateKey { CHECK_STOCK, DEDUCT_STOCK, DONE }
    public enum PaymentAtomicState implements StateKey { TOKENIZE, CHARGE, DONE }

    // 3. Contexts
    public static class AtomicContext implements StateMachineContext {
        public String payload;
    }

    public static class OrderOrchestrationContext implements StateMachineContext {
        public String orderId;
        public boolean isVip = false;
        public boolean inventoryReserved = false;
        public boolean paymentCharged = false;
        public List<String> executionHistory = new ArrayList<>();
        public List<String> compensationHistory = new ArrayList<>();
    }

    /**
     * Tests full directed-graph branching at the macro orchestration level (skipping states for VIPs).
     */
    @Test
    public void testOrchestrationGraphWithDynamicBranching() throws Exception {
        // Atomic Inventory Machine
        StateMachineConfiguration<AtomicContext, InventoryAtomicState, String, String> inventoryMachine =
                AtomicStateMachineBuilder.<AtomicContext, InventoryAtomicState, String, String>create(InventoryAtomicState.class)
                        .context(AtomicContext::new)
                        .initialState(InventoryAtomicState.CHECK_STOCK)
                        .input((ctx, in) -> { ctx.payload = in; return ctx; })
                        .state(InventoryAtomicState.CHECK_STOCK)
                            .action(ctx -> { ctx.payload = "INV_RESERVED_" + ctx.payload; return ctx; })
                            .transition(InventoryAtomicState.DEDUCT_STOCK)
                        .state(InventoryAtomicState.DEDUCT_STOCK)
                            .action(ctx -> ctx)
                            .transition(InventoryAtomicState.DONE)
                        .endStates(InventoryAtomicState.DONE)
                        .output(ctx -> ctx.payload)
                        .build();

        // Atomic Payment Machine
        StateMachineConfiguration<AtomicContext, PaymentAtomicState, String, String> paymentMachine =
                AtomicStateMachineBuilder.<AtomicContext, PaymentAtomicState, String, String>create(PaymentAtomicState.class)
                        .context(AtomicContext::new)
                        .initialState(PaymentAtomicState.TOKENIZE)
                        .input((ctx, in) -> { ctx.payload = in; return ctx; })
                        .state(PaymentAtomicState.TOKENIZE)
                            .action(ctx -> { ctx.payload = "TOKEN_" + ctx.payload; return ctx; })
                            .transition(PaymentAtomicState.CHARGE)
                        .state(PaymentAtomicState.CHARGE)
                            .action(ctx -> { ctx.payload = "PAID_" + ctx.payload; return ctx; })
                            .transition(PaymentAtomicState.DONE)
                        .endStates(PaymentAtomicState.DONE)
                        .output(ctx -> ctx.payload)
                        .build();

        List<OrchestrationCheckpoint<OrderOrchestrationContext, OrderOrchState>> checkpoints = new ArrayList<>();

        // Macro Orchestration State Machine
        try (OrchestrationStateMachineExecutor<OrderOrchestrationContext, OrderOrchState, OrderOrchestrationContext, String> orchestrationMachine =
                OrchestrationStateMachineBuilder.<OrderOrchestrationContext, OrderOrchState, OrderOrchestrationContext, String>create("OrderOrchestrator", OrderOrchState.class)
                        .context(OrderOrchestrationContext::new)
                        .initialState(OrderOrchState.VALIDATE_ORDER)
                        .onCheckpoint(checkpoints::add)
                        .input((ctx, in) -> {
                            if (in != null) {
                                ctx.orderId = in.orderId;
                                ctx.isVip = in.isVip;
                            }
                            ctx.executionHistory.add("START:" + ctx.orderId);
                            return ctx;
                        })
                        // Macro State 1: Validation (Direct Action)
                        .state(OrderOrchState.VALIDATE_ORDER)
                            .action(ctx -> {
                                ctx.executionHistory.add("VALIDATED");
                                return ctx;
                            })
                            // Branch to Inventory
                            .transition(OrderOrchState.RESERVE_INVENTORY)

                        // Macro State 2: Inventory (Executes Atomic Machine)
                        .state(OrderOrchState.RESERVE_INVENTORY)
                            .atomicMachine(inventoryMachine)
                            .input(ctx -> ctx.orderId)
                            .output((ctx, out) -> {
                                ctx.inventoryReserved = true;
                                ctx.executionHistory.add(out);
                                return ctx;
                            })
                            .compensate(ctx -> {
                                ctx.inventoryReserved = false;
                                ctx.compensationHistory.add("RELEASE_INVENTORY");
                                return ctx;
                            })
                            // Dynamic Graph Branching: VIPs skip standard payment directly to VIP_FAST_TRACK!
                            .transitionsTo(
                                    Set.of(OrderOrchState.VIP_FAST_TRACK, OrderOrchState.CHARGE_PAYMENT),
                                    ctx -> ctx.isVip ? OrderOrchState.VIP_FAST_TRACK : OrderOrchState.CHARGE_PAYMENT
                            )

                        // Macro State 3: VIP Fast Track
                        .state(OrderOrchState.VIP_FAST_TRACK)
                            .action(ctx -> {
                                ctx.executionHistory.add("VIP_EXPRESS_SETTLEMENT");
                                return ctx;
                            })
                            .transition(OrderOrchState.COMPLETED)

                        // Macro State 4: Standard Payment
                        .state(OrderOrchState.CHARGE_PAYMENT)
                            .atomicMachine(paymentMachine)
                            .input(ctx -> ctx.orderId)
                            .output((ctx, out) -> {
                                ctx.paymentCharged = true;
                                ctx.executionHistory.add(out);
                                return ctx;
                            })
                            .compensate(ctx -> {
                                ctx.paymentCharged = false;
                                ctx.compensationHistory.add("REFUND_PAYMENT");
                                return ctx;
                            })
                            .transition(OrderOrchState.COMPLETED)

                        .endStates(OrderOrchState.COMPLETED, OrderOrchState.FAILED)
                        .output(ctx -> String.join(" -> ", ctx.executionHistory))
                        .buildExecutor()) {

            // 1. Test Regular Order Flow: VALIDATE -> RESERVE_INVENTORY -> CHARGE_PAYMENT -> COMPLETED
            OrderOrchestrationContext regular = new OrderOrchestrationContext();
            regular.orderId = "ORD-REG-1";
            regular.isVip = false;

            String regResult = orchestrationMachine.dispatchSync(regular);
            assertEquals("START:ORD-REG-1 -> VALIDATED -> INV_RESERVED_ORD-REG-1 -> PAID_TOKEN_ORD-REG-1", regResult);

            // 2. Test VIP Order Flow (Branching/Skip): VALIDATE -> RESERVE_INVENTORY -> VIP_FAST_TRACK -> COMPLETED
            OrderOrchestrationContext vip = new OrderOrchestrationContext();
            vip.orderId = "ORD-VIP-99";
            vip.isVip = true;

            String vipResult = orchestrationMachine.dispatchSync(vip);
            assertEquals("START:ORD-VIP-99 -> VALIDATED -> INV_RESERVED_ORD-VIP-99 -> VIP_EXPRESS_SETTLEMENT", vipResult);

            assertFalse(checkpoints.isEmpty());
        }
    }

    /**
     * Tests ContextRecoverer recreating clean inputs on retry on fresh virtual threads.
     */
    @Test
    public void testContextRecoveryOnVirtualThreadRetry() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);

        // Flaky machine that crashes on attempt 1, succeeds on attempt 2
        StateMachineConfiguration<AtomicContext, InventoryAtomicState, String, String> flakyMachine =
                AtomicStateMachineBuilder.<AtomicContext, InventoryAtomicState, String, String>create(InventoryAtomicState.class)
                        .context(AtomicContext::new)
                        .initialState(InventoryAtomicState.CHECK_STOCK)
                        .input((ctx, in) -> { ctx.payload = in; return ctx; })
                        .state(InventoryAtomicState.CHECK_STOCK)
                            .action(ctx -> {
                                int att = attempts.incrementAndGet();
                                if (att == 1) {
                                    ctx.payload = "DIRTY_CORRUPTED";
                                    throw new RuntimeException("Simulated transient socket timeout");
                                }
                                ctx.payload = "CLEAN_SUCCESS_" + ctx.payload;
                                return ctx;
                            })
                            .transition(InventoryAtomicState.DONE)
                        .endStates(InventoryAtomicState.DONE)
                        .output(ctx -> ctx.payload)
                        .build();

        try (OrchestrationStateMachineExecutor<OrderOrchestrationContext, OrderOrchState, String, String> orchestrationMachine =
                OrchestrationStateMachineBuilder.<OrderOrchestrationContext, OrderOrchState, String, String>create("RecoveryOrchestrator", OrderOrchState.class)
                        .context(OrderOrchestrationContext::new)
                        .initialState(OrderOrchState.RESERVE_INVENTORY)
                        .input((ctx, id) -> { ctx.orderId = id; return ctx; })
                        .state(OrderOrchState.RESERVE_INVENTORY)
                            .atomicMachine(flakyMachine)
                            // ContextRecoverer provides clean input for attempt 2 on a fresh virtual thread:
                            .recoverer((orchCtx, _, attempt) -> orchCtx.orderId + "_ATTEMPT_" + attempt)
                            .retry(RetryPolicy.fixed(3, Duration.ofMillis(10)))
                            .output((ctx, out) -> {
                                ctx.executionHistory.add(out);
                                return ctx;
                            })
                            .transition(OrderOrchState.COMPLETED)
                        .endStates(OrderOrchState.COMPLETED)
                        .output(ctx -> ctx.executionHistory.getFirst())
                        .buildExecutor()) {

            String result = orchestrationMachine.dispatchSync("ORD-888");
            assertEquals("CLEAN_SUCCESS_ORD-888_ATTEMPT_2", result);
            assertEquals(2, attempts.get());
        }
    }

    /**
     * Tests automated Saga compensation rollback in reverse order (LIFO) upon permanent failure.
     */
    @Test
    public void testAutomatedSagaCompensationRollback() {
        StateMachineConfiguration<AtomicContext, InventoryAtomicState, Void, Void> dummyMachine =
                AtomicStateMachineBuilder.<AtomicContext, InventoryAtomicState, Void, Void>create(InventoryAtomicState.class)
                        .context(AtomicContext::new)
                        .initialState(InventoryAtomicState.DONE)
                        .endStates(InventoryAtomicState.DONE)
                        .build();

        List<String> compensationLog = new ArrayList<>();
        List<OrchestrationCheckpoint<OrderOrchestrationContext, OrderOrchState>> checkpoints = new ArrayList<>();

        try (OrchestrationStateMachineExecutor<OrderOrchestrationContext, OrderOrchState, Void, Void> orchestrationMachine =
                OrchestrationStateMachineBuilder.<OrderOrchestrationContext, OrderOrchState, Void, Void>create("SagaOrchestrator", OrderOrchState.class)
                        .context(OrderOrchestrationContext::new)
                        .initialState(OrderOrchState.VALIDATE_ORDER)
                        .onCheckpoint(checkpoints::add)
                        // State 1
                        .state(OrderOrchState.VALIDATE_ORDER)
                            .atomicMachine(dummyMachine)
                            .compensate(ctx -> {
                                compensationLog.add("COMPENSATE_VALIDATION");
                                return ctx;
                            })
                            .transition(OrderOrchState.RESERVE_INVENTORY)

                        // State 2
                        .state(OrderOrchState.RESERVE_INVENTORY)
                            .atomicMachine(dummyMachine)
                            .compensate(ctx -> {
                                compensationLog.add("COMPENSATE_INVENTORY");
                                return ctx;
                            })
                            .transition(OrderOrchState.CHARGE_PAYMENT)

                        // State 3 (Fatal Failure)
                        .state(OrderOrchState.CHARGE_PAYMENT)
                            .action(_ -> { throw new RuntimeException("Fatal Payment Processing Outage"); })
                            .transition(OrderOrchState.COMPLETED)

                        .endStates(OrderOrchState.COMPLETED, OrderOrchState.FAILED)
                        .buildExecutor()) {

            assertThrows(Exception.class, () -> orchestrationMachine.dispatchSync(null));

            // Verify LIFO order: State 2 (Inventory) compensated before State 1 (Validation)
            assertEquals(2, compensationLog.size());
            assertEquals("COMPENSATE_INVENTORY", compensationLog.getFirst());
            assertEquals("COMPENSATE_VALIDATION", compensationLog.getLast());

            // Verify checkpoint status
            assertEquals(OrchestrationStatus.COMPENSATED, checkpoints.getLast().status());
        }
    }
}
