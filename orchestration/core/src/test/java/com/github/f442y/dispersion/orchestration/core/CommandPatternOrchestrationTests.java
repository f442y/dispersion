package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SagaCommand;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        public boolean orderFulfilled = false;
        public List<String> executionLog = new ArrayList<>();
    }

    public record ConfirmPaymentCommand(
            String correlationKey,
            String transactionId,
            int amount
    ) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "ConfirmPaymentCommand";
        }
    }

    public record ReserveInventorySagaCommand(
            String sku,
            int quantity
    ) implements SagaCommand<EcomContext> {
        @Override
        @NonNull
        public EcomContext execute(@NonNull EcomContext context) {
            context.inventoryReserved = true;
            context.executionLog.add("RESERVED_SKU_" + sku + "_" + quantity);
            return context;
        }

        @Override
        @NonNull
        public EcomContext compensate(@NonNull EcomContext context) {
            context.inventoryReserved = false;
            context.executionLog.add("RELEASED_SKU_" + sku + "_" + quantity);
            return context;
        }
    }

    /**
     * Tests typed SignalCommand dispatching and reversible SagaCommand integration.
     */
    @Test
    public void testTypedSignalCommandAndSagaCommand() throws Exception {
        InMemoryCheckpointStore<EcomContext, EcomState> store = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<EcomContext, EcomState, EcomContext, String> executor =
                OrchestrationStateMachineBuilder.<EcomContext, EcomState, EcomContext, String>create("EcomCommandOrchestrator", EcomState.class)
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
                        Objects.requireNonNull(cmd, "cmd must not be null");
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
                .output(ctx -> "Success: " + ctx.orderId)
                .buildExecutor();

        EcomContext order = new EcomContext();
        order.orderId = "ORDER-CMD-001";
        order.amount = 250;

        // Turn 1: Validates and executes Saga command, then halts waiting for ConfirmPaymentCommand
        OrchestrationTurnResult<EcomContext, EcomState, String> turn1 = executor.dispatchTurnSync(null, order);
        assertTrue(turn1.isSuspended());
        assertTrue(turn1.context().inventoryReserved);
        assertEquals(EcomState.AWAIT_PAYMENT_COMMAND, turn1.currentStateKey());

        // Turn 2: Dispatch strongly typed SignalCommand directly to executor
        ConfirmPaymentCommand command = new ConfirmPaymentCommand("ORDER-CMD-001", "TX-SECURE-999", 250);
        CompletableFuture<OrchestrationTurnResult<EcomContext, EcomState, String>> turn2Future =
                executor.handleCommand(command);

        OrchestrationTurnResult<EcomContext, EcomState, String> turn2 = turn2Future.get();
        assertTrue(turn2.isCompleted());
        assertEquals("Success: ORDER-CMD-001", turn2.output());
        assertTrue(turn2.context().orderFulfilled);
        assertEquals("TX-SECURE-999", turn2.context().transactionId);
        assertEquals(
                List.of("VALIDATE", "RESERVED_SKU_ITEM-404_1", "PAYMENT_CONFIRMED:TX-SECURE-999", "FULFILL"),
                turn2.context().executionLog
        );

        executor.close();
    }

    /**
     * Tests idempotent delivery via CommandEnvelope: Duplicate deliveries with the same commandId are deduplicated.
     */
    @Test
    public void testIdempotentCommandEnvelopeDeduplication() throws Exception {
        InMemoryCheckpointStore<EcomContext, EcomState> store = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<EcomContext, EcomState, EcomContext, String> executor =
                OrchestrationStateMachineBuilder.<EcomContext, EcomState, EcomContext, String>create("IdempotentCommandOrchestrator", EcomState.class)
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
                        Objects.requireNonNull(cmd, "cmd must not be null");
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
