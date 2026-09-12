package com.github.f442y.dispersion.examples;

import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineExecutor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Demonstrates an e-commerce checkout workflow using a macro Orchestration State Machine
 * that embeds atomic child state machines and coordinates automated Saga compensation rollbacks.
 */
public class OrderProcessingSagaExampleTests {

    public enum OrderWorkflowState implements StateKey {
        VALIDATE_ORDER,
        RESERVE_INVENTORY,
        PROCESS_PAYMENT,
        CONFIRM_ORDER,
        COMPLETED,
        FAILED
    }

    public enum InventoryStep implements StateKey {
        CHECK_STOCK, ALLOCATE_ITEM, FINISHED
    }

    public record OrderRequest(String orderId, String customerId, int amountCents, boolean forcePaymentFailure) {}

    public static class InventoryContext implements StateMachineContext {
        public String sku;
        public boolean allocated;
    }

    public static class OrderWorkflowContext implements StateMachineContext {
        public String orderId;
        public String customerId;
        public int amountCents;
        public boolean forcePaymentFailure;
        public boolean inventoryAllocated;
        public boolean paymentSettled;
        public List<String> auditTrail = new ArrayList<>();
        public List<String> compensationTrail = new ArrayList<>();
    }

    @Test
    @DisplayName("Should successfully complete end-to-end checkout Saga")
    public void testSuccessfulCheckout() throws Exception {
        // 1. Child Atomic Machine for Inventory
        StateMachineConfiguration<InventoryContext, InventoryStep, String, Boolean> inventoryMachine =
                AtomicStateMachineBuilder.<InventoryContext, InventoryStep, String, Boolean>create(InventoryStep.class)
                        .context(InventoryContext::new)
                        .initialState(InventoryStep.CHECK_STOCK)
                        .input((ctx, sku) -> { ctx.sku = sku; return ctx; })
                        .state(InventoryStep.CHECK_STOCK)
                            .action(ctx -> ctx)
                            .transition(InventoryStep.ALLOCATE_ITEM)
                        .state(InventoryStep.ALLOCATE_ITEM)
                            .action(ctx -> { ctx.allocated = true; return ctx; })
                            .transition(InventoryStep.FINISHED)
                        .endStates(InventoryStep.FINISHED)
                        .output(ctx -> ctx.allocated)
                        .build();

        // 2. Parent Orchestration Machine
        try (OrchestrationStateMachineExecutor<OrderWorkflowContext, OrderWorkflowState, OrderRequest, String> executor =
                OrchestrationStateMachineBuilder.<OrderWorkflowContext, OrderWorkflowState, OrderRequest, String>create("CheckoutSaga", OrderWorkflowState.class)
                        .context(OrderWorkflowContext::new)
                        .initialState(OrderWorkflowState.VALIDATE_ORDER)
                        .input((ctx, req) -> {
                            if (req != null) {
                                ctx.orderId = req.orderId();
                                ctx.customerId = req.customerId();
                                ctx.amountCents = req.amountCents();
                                ctx.forcePaymentFailure = req.forcePaymentFailure();
                            }
                            ctx.auditTrail.add("ORDER_VALIDATED");
                            return ctx;
                        })
                        // State 1: Validation
                        .state(OrderWorkflowState.VALIDATE_ORDER)
                            .action(ctx -> ctx)
                            .transition(OrderWorkflowState.RESERVE_INVENTORY)

                        // State 2: Inventory (Child machine + Saga compensation)
                        .state(OrderWorkflowState.RESERVE_INVENTORY)
                            .atomicMachine(inventoryMachine)
                            .input(ctx -> "SKU-" + ctx.orderId)
                            .output((ctx, allocated) -> {
                                ctx.inventoryAllocated = allocated;
                                ctx.auditTrail.add("INVENTORY_RESERVED");
                                return ctx;
                            })
                            .compensate(ctx -> {
                                ctx.inventoryAllocated = false;
                                ctx.compensationTrail.add("RELEASE_INVENTORY");
                                return ctx;
                            })
                            .transition(OrderWorkflowState.PROCESS_PAYMENT)

                        // State 3: Payment
                        .state(OrderWorkflowState.PROCESS_PAYMENT)
                            .action(ctx -> {
                                if (ctx.forcePaymentFailure) {
                                    throw new IllegalStateException("Payment gateway card declined: insufficient funds");
                                }
                                ctx.paymentSettled = true;
                                ctx.auditTrail.add("PAYMENT_SETTLED");
                                return ctx;
                            })
                            .compensate(ctx -> {
                                ctx.paymentSettled = false;
                                ctx.compensationTrail.add("REFUND_PAYMENT");
                                return ctx;
                            })
                            .transition(OrderWorkflowState.CONFIRM_ORDER)

                        // State 4: Confirmation
                        .state(OrderWorkflowState.CONFIRM_ORDER)
                            .action(ctx -> {
                                ctx.auditTrail.add("ORDER_CONFIRMED");
                                return ctx;
                            })
                            .transition(OrderWorkflowState.COMPLETED)

                        .endStates(OrderWorkflowState.COMPLETED, OrderWorkflowState.FAILED)
                        .output(ctx -> "Order " + ctx.orderId + " completed successfully with audit: " + String.join(" -> ", ctx.auditTrail))
                        .buildExecutor()) {

            OrderRequest request = new OrderRequest("ORD-1001", "CUST-42", 5999, false);
            String summary = executor.dispatchSync(request);

            assertThat(summary).contains("Order ORD-1001 completed successfully");
            assertThat(summary).contains("ORDER_VALIDATED -> INVENTORY_RESERVED -> PAYMENT_SETTLED -> ORDER_CONFIRMED");
        }
    }

    @Test
    @DisplayName("Should execute LIFO Saga compensations when payment fails downstream")
    public void testFailedPaymentTriggersSagaRollback() {
        // 1. Child Atomic Machine for Inventory
        StateMachineConfiguration<InventoryContext, InventoryStep, String, Boolean> inventoryMachine =
                AtomicStateMachineBuilder.<InventoryContext, InventoryStep, String, Boolean>create(InventoryStep.class)
                        .context(InventoryContext::new)
                        .initialState(InventoryStep.CHECK_STOCK)
                        .input((ctx, sku) -> { ctx.sku = sku; return ctx; })
                        .state(InventoryStep.CHECK_STOCK)
                            .action(ctx -> ctx)
                            .transition(InventoryStep.ALLOCATE_ITEM)
                        .state(InventoryStep.ALLOCATE_ITEM)
                            .action(ctx -> { ctx.allocated = true; return ctx; })
                            .transition(InventoryStep.FINISHED)
                        .endStates(InventoryStep.FINISHED)
                        .output(ctx -> ctx.allocated)
                        .build();

        List<String> compensationAudit = new ArrayList<>();

        // 2. Parent Orchestration Machine
        try (OrchestrationStateMachineExecutor<OrderWorkflowContext, OrderWorkflowState, OrderRequest, String> executor =
                OrchestrationStateMachineBuilder.<OrderWorkflowContext, OrderWorkflowState, OrderRequest, String>create("FailingCheckoutSaga", OrderWorkflowState.class)
                        .context(OrderWorkflowContext::new)
                        .initialState(OrderWorkflowState.VALIDATE_ORDER)
                        .input((ctx, req) -> {
                            if (req != null) {
                                ctx.orderId = req.orderId();
                                ctx.forcePaymentFailure = req.forcePaymentFailure();
                            }
                            return ctx;
                        })
                        .state(OrderWorkflowState.VALIDATE_ORDER)
                            .action(ctx -> ctx)
                            .compensate(ctx -> {
                                compensationAudit.add("COMPENSATE_VALIDATION");
                                return ctx;
                            })
                            .transition(OrderWorkflowState.RESERVE_INVENTORY)

                        .state(OrderWorkflowState.RESERVE_INVENTORY)
                            .atomicMachine(inventoryMachine)
                            .compensate(ctx -> {
                                compensationAudit.add("RELEASE_INVENTORY");
                                return ctx;
                            })
                            .transition(OrderWorkflowState.PROCESS_PAYMENT)

                        .state(OrderWorkflowState.PROCESS_PAYMENT)
                            .action(_ -> {
                                throw new RuntimeException("Card expired or declined");
                            })
                            .transition(OrderWorkflowState.COMPLETED)

                        .endStates(OrderWorkflowState.COMPLETED, OrderWorkflowState.FAILED)
                        .buildExecutor()) {

            OrderRequest request = new OrderRequest("ORD-9999", "CUST-00", 1200, true);

            assertThatThrownBy(() -> executor.dispatchSync(request))
                    .hasMessageContaining("Card expired or declined");

            // Verify LIFO compensation order: Reserve Inventory was compensated before Validate Order
            assertThat(compensationAudit).containsExactly("RELEASE_INVENTORY", "COMPENSATE_VALIDATION");
        }
    }
}
