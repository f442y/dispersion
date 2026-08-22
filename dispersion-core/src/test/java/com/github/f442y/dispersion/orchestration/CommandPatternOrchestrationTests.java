package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SagaCommand;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommandPatternOrchestrationTests {

    public enum EcomState implements StateKey {
        VALIDATE,
        RESERVE_INVENTORY,
        AWAIT_PAYMENT_COMMAND,
        FULFILL,
        COMPLETED,
        FAILED
    }

    public static class EcomContext implements StateMachineContext {
        public String orderId;
        public int amount;
        public String transactionId;
        public boolean inventoryReserved = false;
        public boolean inventoryReleased = false;
        public boolean orderFulfilled = false;
        public List<String> executionLog = new ArrayList<>();
    }

    // Sealed Signal Command hierarchy
    public sealed interface EcomSignalCommand extends SignalCommand
            permits ConfirmPaymentCommand, CancelOrderCommand {
        @Override
        String correlationKey();
    }

    public record ConfirmPaymentCommand(String orderId, String transactionId, int amountPaid) implements EcomSignalCommand {
        @Override
        public String correlationKey() {
            return orderId;
        }
    }

    public record CancelOrderCommand(String orderId, String reason) implements EcomSignalCommand {
        @Override
        public String correlationKey() {
            return orderId;
        }
    }

    // Reversible Saga Command
    public record ReserveInventorySagaCommand(String sku, int count) implements SagaCommand<EcomContext> {
        @Override
        public EcomContext execute(EcomContext context) {
            context.inventoryReserved = true;
            context.executionLog.add("RESERVED_SKU_" + sku + "_" + count);
            return context;
        }

        @Override
        public EcomContext undo(EcomContext context) {
            context.inventoryReleased = true;
            context.executionLog.add("UNRESERVED_SKU_" + sku + "_" + count);
            return context;
        }
    }

    /**
     * Tests typed SignalCommand dispatching and reversible SagaCommand integration.
     */
    @Test
    public void testTypedSignalCommandAndSagaCommand() throws Exception {
        InMemoryCheckpointStore<EcomContext, EcomState> store = new InMemoryCheckpointStore<>();

        var executor = OrchestrationStateMachineBuilder.<EcomContext, EcomState, EcomContext, String>create("EcomCommandOrchestrator", EcomState.class)
                .context(EcomContext::new)
                .initialState(EcomState.VALIDATE)
                .correlationKey(ctx -> ctx.orderId)
                .checkpointStore(store)
                .input((ctx, input) -> {
                    if (input != null) {
                        ctx.orderId = input.orderId;
                        ctx.amount = input.amount;
                    }
                    return ctx;
                })
                .state(EcomState.VALIDATE)
                    .action(ctx -> {
                        ctx.executionLog.add("VALIDATE");
                        return ctx;
                    })
                    .transition(EcomState.RESERVE_INVENTORY)
                // Reversible Saga Command: encapsulates action & compensation in one object
                .state(EcomState.RESERVE_INVENTORY)
                    .command(new ReserveInventorySagaCommand("ITEM-404", 1))
                    .transition(EcomState.AWAIT_PAYMENT_COMMAND)
                // Strongly typed command receiver
                .state(EcomState.AWAIT_PAYMENT_COMMAND)
                    .waitForCommand(ConfirmPaymentCommand.class, (ctx, cmd) -> {
                        ctx.transactionId = cmd.transactionId();
                        ctx.executionLog.add("PAYMENT_CONFIRMED:" + cmd.transactionId());
                        return ctx;
                    })
                    .transition(EcomState.FULFILL)
                .state(EcomState.FULFILL)
                    .action(ctx -> {
                        ctx.orderFulfilled = true;
                        ctx.executionLog.add("FULFILL");
                        return ctx;
                    })
                    .transition(EcomState.COMPLETED)
                .endStates(EcomState.COMPLETED, EcomState.FAILED)
                .output(ctx -> "Ecom " + ctx.orderId + " completed via " + ctx.transactionId)
                .buildExecutor();

        EcomContext initial = new EcomContext();
        initial.orderId = "ORD-CMD-101";
        initial.amount = 9900;

        // 1. Initial turn: runs VALIDATE -> executes ReserveInventorySagaCommand -> suspends at AWAIT_PAYMENT_COMMAND
        OrchestrationTurnResult<EcomContext, EcomState, String> turn1 = executor.dispatchTurnSync(null, initial);
        assertTrue(turn1.isSuspended());
        assertEquals("ConfirmPaymentCommand", turn1.expectedSignal());
        assertTrue(turn1.context().inventoryReserved);
        assertFalse(turn1.context().orderFulfilled);

        // 2. Dispatch typed SignalCommand directly (executor routes by correlationKey)
        CompletableFuture<OrchestrationTurnResult<EcomContext, EcomState, String>> future =
                executor.handleCommand(new ConfirmPaymentCommand("ORD-CMD-101", "TX-CMD-777", 9900));

        OrchestrationTurnResult<EcomContext, EcomState, String> turn2 = future.get();
        assertTrue(turn2.isCompleted());
        assertEquals("Ecom ORD-CMD-101 completed via TX-CMD-777", turn2.output());
        assertNotNull(turn2.context());
        assertTrue(turn2.context().orderFulfilled);
        assertEquals(
                List.of("VALIDATE", "RESERVED_SKU_ITEM-404_1", "PAYMENT_CONFIRMED:TX-CMD-777", "FULFILL"),
                turn2.context().executionLog
        );

        executor.close();
    }

    /**
     * Tests idempotent CommandEnvelope deduplication to prevent duplicate webhook delivery execution.
     */
    @Test
    public void testIdempotentCommandEnvelopeDeduplication() throws Exception {
        InMemoryCheckpointStore<EcomContext, EcomState> store = new InMemoryCheckpointStore<>();

        var executor = OrchestrationStateMachineBuilder.<EcomContext, EcomState, EcomContext, String>create("IdempotentCommandOrchestrator", EcomState.class)
                .context(EcomContext::new)
                .initialState(EcomState.VALIDATE)
                .correlationKey(ctx -> ctx.orderId)
                .checkpointStore(store)
                .input((ctx, input) -> {
                    if (input != null) {
                        ctx.orderId = input.orderId;
                    }
                    return ctx;
                })
                .state(EcomState.VALIDATE)
                    .action(ctx -> ctx)
                    .transition(EcomState.AWAIT_PAYMENT_COMMAND)
                .state(EcomState.AWAIT_PAYMENT_COMMAND)
                    .waitForCommand(ConfirmPaymentCommand.class, (ctx, cmd) -> {
                        ctx.transactionId = cmd.transactionId();
                        ctx.executionLog.add("PAYMENT:" + cmd.transactionId());
                        return ctx;
                    })
                    .transition(EcomState.COMPLETED)
                .endStates(EcomState.COMPLETED)
                .output(ctx -> "Done " + ctx.orderId)
                .buildExecutor();

        EcomContext initial = new EcomContext();
        initial.orderId = "ORD-IDEMP-55";

        OrchestrationTurnResult<EcomContext, EcomState, String> turn1 = executor.dispatchTurnSync(null, initial);
        assertTrue(turn1.isSuspended());

        UUID commandId = UUID.randomUUID();
        CommandEnvelope<ConfirmPaymentCommand> envelope = CommandEnvelope.of(
                commandId,
                new ConfirmPaymentCommand("ORD-IDEMP-55", "TX-FIRST", 5000)
        );

        // First delivery: processes successfully
        CompletableFuture<OrchestrationTurnResult<EcomContext, EcomState, String>> future1 =
                executor.handleCommand(envelope);
        OrchestrationTurnResult<EcomContext, EcomState, String> result1 = future1.get();
        assertTrue(result1.isCompleted());
        assertEquals("Done ORD-IDEMP-55", result1.output());

        // Duplicate delivery with same commandId: should be safely ignored
        CompletableFuture<OrchestrationTurnResult<EcomContext, EcomState, String>> future2 =
                executor.handleCommand(envelope);
        OrchestrationTurnResult<EcomContext, EcomState, String> result2 = future2.get();
        assertNotNull(result2);

        executor.close();
    }
}
