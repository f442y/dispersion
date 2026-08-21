package com.github.f442y.dispersion.atomic;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.ActionException;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NestedStateMachineTests {

    // Parent States & Context
    public enum ParentState implements StateKey {
        VALIDATE, EXECUTE_CHILD, FINALIZE, DONE
    }

    public static class ParentContext implements StateMachineContext {
        public String orderId;
        public int amount;
        public String paymentTxId;
        public List<String> logs = new ArrayList<>();
    }

    // Child States & Context
    public enum ChildState implements StateKey {
        AUTH, CAPTURE, COMPLETED
    }

    public static class ChildContext implements StateMachineContext {
        public String txId;
        public boolean authorized;
    }

    public record PaymentRequest(String orderId, int amount) {}
    public record PaymentResponse(String txId, boolean success) {}

    @Test
    public void testNestedSubStateMachineExecution() throws Exception {
        // Define Child State Machine
        StateMachineConfiguration<ChildContext, ChildState, PaymentRequest, PaymentResponse> childStateMachine =
                AtomicStateMachineBuilder.<ChildContext, ChildState, PaymentRequest, PaymentResponse>create(ChildState.class)
                        .context(ChildContext::new)
                        .initialState(ChildState.AUTH)
                        .input((ctx, req) -> {
                            ctx.txId = "TX-" + req.orderId();
                            return ctx;
                        })
                        .state(ChildState.AUTH)
                            .action(ctx -> {
                                ctx.authorized = true;
                                return ctx;
                            })
                            .transition(ctx -> ChildState.CAPTURE)
                        .state(ChildState.CAPTURE)
                            .action(ctx -> ctx)
                            .transition(ctx -> ChildState.COMPLETED)
                        .endStates(ChildState.COMPLETED)
                        .output(ctx -> new PaymentResponse(ctx.txId, ctx.authorized))
                        .build();

        // Define Parent State Machine embedding Child
        StateMachineConfiguration<ParentContext, ParentState, PaymentRequest, String> parentStateMachine =
                AtomicStateMachineBuilder.<ParentContext, ParentState, PaymentRequest, String>create(ParentState.class)
                        .context(ParentContext::new)
                        .initialState(ParentState.VALIDATE)
                        .input((ctx, req) -> {
                            ctx.orderId = req.orderId();
                            ctx.amount = req.amount();
                            ctx.logs.add("PARENT:VALIDATE");
                            return ctx;
                        })
                        .state(ParentState.VALIDATE)
                            .action(ctx -> {
                                ctx.logs.add("VALIDATED");
                                return ctx;
                            })
                            .transition(ctx -> ParentState.EXECUTE_CHILD)
                        .state(ParentState.EXECUTE_CHILD)
                            .subStateMachine(
                                    childStateMachine,
                                    parentCtx -> new PaymentRequest(parentCtx.orderId, parentCtx.amount),
                                    (parentCtx, childResult) -> {
                                        parentCtx.paymentTxId = childResult.txId();
                                        parentCtx.logs.add("CHILD_RESULT:" + childResult.txId());
                                        return parentCtx;
                                    }
                            )
                            .transition(ctx -> ParentState.FINALIZE)
                        .state(ParentState.FINALIZE)
                            .action(ctx -> {
                                ctx.logs.add("FINALIZED");
                                return ctx;
                            })
                            .transition(ctx -> ParentState.DONE)
                        .endStates(ParentState.DONE)
                        .output(ctx -> "Processed order=" + ctx.orderId + ", tx=" + ctx.paymentTxId)
                        .build();

        AtomicStateMachineExecutor<ParentContext, ParentState, PaymentRequest, String> executor =
                new AtomicStateMachineExecutor<>("nested-test", parentStateMachine, 10);

        String result = executor.dispatchSync(new PaymentRequest("ORD-12345", 999));
        assertEquals("Processed order=ORD-12345, tx=TX-ORD-12345", result);
    }

    @Test
    public void testChildExceptionBubblesUpToParent() {
        // Child that throws an exception
        StateMachineConfiguration<ChildContext, ChildState, Void, Void> failingChildMachine =
                AtomicStateMachineBuilder.<ChildContext, ChildState, Void, Void>create(ChildState.class)
                        .context(ChildContext::new)
                        .initialState(ChildState.AUTH)
                        .state(ChildState.AUTH)
                            .action(ctx -> {
                                throw new IllegalStateException("Payment gateway unreachable");
                            })
                            .transition(ctx -> ChildState.COMPLETED)
                        .endStates(ChildState.COMPLETED)
                        .output(ctx -> null)
                        .build();

        // Parent embedding failing child
        StateMachineConfiguration<ParentContext, ParentState, Void, String> parentStateMachine =
                AtomicStateMachineBuilder.<ParentContext, ParentState, Void, String>create(ParentState.class)
                        .context(ParentContext::new)
                        .initialState(ParentState.EXECUTE_CHILD)
                        .state(ParentState.EXECUTE_CHILD)
                            .subStateMachine(
                                    failingChildMachine,
                                    ctx -> null,
                                    (ctx, res) -> ctx
                            )
                            .transition(ctx -> ParentState.DONE)
                        .endStates(ParentState.DONE)
                        .output(ctx -> "OK")
                        .build();

        AtomicStateMachineExecutor<ParentContext, ParentState, Void, String> executor =
                new AtomicStateMachineExecutor<>("failing-nested-test", parentStateMachine, 10);

        assertThrows(ActionException.class, () -> executor.dispatchSync(null));
    }
}
