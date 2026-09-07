package com.github.f442y.dispersion.atomic;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.MaxStateVisitsExceededException;
import com.github.f442y.dispersion.exception.MaxTransitionsExceededException;
import com.github.f442y.dispersion.exception.TransitionException;
import com.github.f442y.dispersion.state.Action;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GraphTraversalAndValidationTests {

    public enum GraphState implements StateKey {
        START, VALIDATE, RETRY, SKIP_FORWARD, PROCESS, SUCCESS, FAILED, TIMEOUT_EXCEEDED
    }

    public static class OrderContext implements StateMachineContext {
        public int retryCount = 0;
        public boolean isVip = false;
        public boolean isFraud = false;
        public List<String> history = new ArrayList<>();
    }

    /**
     * Tests cyclic loop back / revisits and forward skipping using dynamic Transition functions.
     */
    @Test
    public void testMultiDirectionalTraversalWithCyclesAndSkips() throws Exception {
        StateMachineConfiguration<OrderContext, GraphState, OrderContext, String> stateMachine =
                AtomicStateMachineBuilder.<OrderContext, GraphState, OrderContext, String>create(GraphState.class)
                        .context(OrderContext::new)
                        .initialState(GraphState.START)
                        .input((ctx, input) -> {
                            if (input != null) {
                                ctx.isVip = input.isVip;
                                ctx.isFraud = input.isFraud;
                            }
                            return ctx;
                        })
                        .state(GraphState.START)
                            .action(ctx -> {
                                ctx.history.add("START");
                                return ctx;
                            })
                            .transition(GraphState.VALIDATE)
                        .state(GraphState.VALIDATE)
                            .action(ctx -> {
                                ctx.history.add("VALIDATE (attempt " + ctx.retryCount + ")");
                                return ctx;
                            })
                            // Direct dynamic Transition inspecting context:
                            .transitionsTo(
                                    Set.of(GraphState.SKIP_FORWARD, GraphState.RETRY, GraphState.PROCESS),
                                    ctx -> {
                                        if (ctx.isVip) return GraphState.SKIP_FORWARD;
                                        if (ctx.retryCount < 2) return GraphState.RETRY;
                                        return GraphState.PROCESS;
                                    }
                            )
                        .state(GraphState.RETRY)
                            .action(ctx -> {
                                ctx.retryCount++;
                                ctx.history.add("RETRY -> looping back to VALIDATE");
                                return ctx;
                            })
                            .transition(GraphState.VALIDATE)
                        .state(GraphState.SKIP_FORWARD)
                            .action(ctx -> {
                                ctx.history.add("VIP_FAST_TRACK");
                                return ctx;
                            })
                            .transition(GraphState.SUCCESS)
                        .state(GraphState.PROCESS)
                            .action(ctx -> {
                                ctx.history.add("PROCESS");
                                return ctx;
                            })
                            .transitionsTo(
                                    Set.of(GraphState.SUCCESS, GraphState.FAILED),
                                    ctx -> ctx.isFraud ? GraphState.FAILED : GraphState.SUCCESS
                                )
                        .endStates(GraphState.SUCCESS, GraphState.FAILED)
                        .output(ctx -> String.join(" -> ", ctx.history))
                        .build();

        try (AtomicStateMachineExecutor<OrderContext, GraphState, OrderContext, String> executor =
                new AtomicStateMachineExecutor<>("graph-traversal-test", stateMachine, 10)) {

            // 1. Standard non-VIP with 2 retry cycles before succeeding
            OrderContext standard = new OrderContext();
            String standardResult = executor.dispatchSync(standard, standard);
            assertEquals(
                    "START -> VALIDATE (attempt 0) -> RETRY -> looping back to VALIDATE -> " +
                    "VALIDATE (attempt 1) -> RETRY -> looping back to VALIDATE -> " +
                    "VALIDATE (attempt 2) -> PROCESS",
                    standardResult
            );

            // 2. VIP skips validation retries directly to SKIP_FORWARD -> SUCCESS
            OrderContext vip = new OrderContext();
            vip.isVip = true;
            String vipResult = executor.dispatchSync(vip, vip);
            assertEquals(
                    "START -> VALIDATE (attempt 0) -> VIP_FAST_TRACK",
                    vipResult
            );

            // 3. Test Mermaid Diagram Generation
            String mermaid = stateMachine.getStateMap().toMermaid();
            assertTrue(mermaid.contains("stateDiagram-v2"));
            assertTrue(mermaid.contains("VALIDATE --> RETRY"));
            assertTrue(mermaid.contains("RETRY --> VALIDATE"));
            assertTrue(mermaid.contains("VALIDATE --> SKIP_FORWARD"));
        }
    }

    /**
     * Tests per-state visit limiter automatically diverting to fallback state when exceeded.
     */
    @Test
    public void testMaxVisitsDivertsToFallbackState() throws Exception {
        StateMachineConfiguration<OrderContext, GraphState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<OrderContext, GraphState, Void, String>create(GraphState.class)
                        .context(OrderContext::new)
                        .initialState(GraphState.START)
                        .state(GraphState.START)
                            .action(ctx -> {
                                ctx.history.add("START");
                                return ctx;
                            })
                            .transition(GraphState.RETRY)
                        .state(GraphState.RETRY)
                            .action(ctx -> {
                                ctx.history.add("RETRY");
                                return ctx;
                            })
                            .maxVisits(2, GraphState.TIMEOUT_EXCEEDED)
                            .transition(GraphState.RETRY)
                        .state(GraphState.TIMEOUT_EXCEEDED)
                            .action(ctx -> {
                                ctx.history.add("TIMEOUT_EXCEEDED");
                                return ctx;
                            })
                            .transition(GraphState.FAILED)
                        .endStates(GraphState.FAILED)
                        .output(ctx -> String.join(" -> ", ctx.history))
                        .build();

        try (AtomicStateMachineExecutor<OrderContext, GraphState, Void, String> executor =
                new AtomicStateMachineExecutor<>("max-visits-fallback-test", stateMachine, 10)) {

            String result = executor.dispatchSync(null);
            assertEquals("START -> RETRY -> RETRY -> TIMEOUT_EXCEEDED", result);
        }
    }

    /**
     * Tests per-state visit limiter throwing MaxStateVisitsExceededException when no fallback is configured.
     */
    @Test
    public void testMaxVisitsThrowsExceptionWhenNoFallbackConfigured() {
        StateMachineConfiguration<OrderContext, GraphState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<OrderContext, GraphState, Void, String>create(GraphState.class)
                        .context(OrderContext::new)
                        .initialState(GraphState.START)
                        .state(GraphState.START)
                            .transition(GraphState.RETRY)
                        .state(GraphState.RETRY)
                            .maxVisits(2)
                            .transition(GraphState.RETRY)
                        .endStates(GraphState.FAILED)
                        .build();

        try (AtomicStateMachineExecutor<OrderContext, GraphState, Void, String> executor =
                new AtomicStateMachineExecutor<>("max-visits-exception-test", stateMachine, 10)) {

            assertThrows(MaxStateVisitsExceededException.class, () -> executor.dispatchSync(null));
        }
    }

    /**
     * Tests global max transitions circuit breaker halting runaway infinite loops.
     */
    @Test
    public void testGlobalMaxTransitionsCircuitBreaker() {
        StateMachineConfiguration<OrderContext, GraphState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<OrderContext, GraphState, Void, String>create(GraphState.class)
                        .context(OrderContext::new)
                        .initialState(GraphState.START)
                        .maxTransitions(5)
                        .state(GraphState.START)
                            .transition(GraphState.VALIDATE)
                        .state(GraphState.VALIDATE)
                            .transition(GraphState.RETRY)
                        .state(GraphState.RETRY)
                            .transition(GraphState.VALIDATE)
                        .endStates(GraphState.SUCCESS)
                        .build();

        try (AtomicStateMachineExecutor<OrderContext, GraphState, Void, String> executor =
                new AtomicStateMachineExecutor<>("global-max-transitions-test", stateMachine, 10)) {

            assertThrows(MaxTransitionsExceededException.class, () -> executor.dispatchSync(null));
        }
    }

    /**
     * Tests build-time verification failing when a state declares an edge to an unregistered state.
     */
    @Test
    public void testBuildTimeGraphIntegrityFailsOnUnregisteredTarget() {
        assertThrows(IllegalStateException.class, () ->
            AtomicStateMachineBuilder.<OrderContext, GraphState, Void, Void>create(GraphState.class)
                    .context(OrderContext::new)
                    .initialState(GraphState.START)
                    .addState(GraphState.START, State.of(Action.identity(), Set.of(GraphState.PROCESS), _ -> GraphState.PROCESS))
                    .endStates(GraphState.SUCCESS)
                    .build()
        );
    }

    /**
     * Tests runtime validation failing when transition function returns an undeclared target state.
     */
    @Test
    public void testRuntimeAdjacencyGuardRejectsUndeclaredTransition() {
        StateMachineConfiguration<OrderContext, GraphState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<OrderContext, GraphState, Void, String>create(GraphState.class)
                        .context(OrderContext::new)
                        .initialState(GraphState.START)
                        .addState(GraphState.START, State.of(
                                Action.identity(),
                                Set.of(GraphState.VALIDATE),
                                _ -> GraphState.PROCESS
                        ))
                        .addState(GraphState.VALIDATE, State.of(Action.identity(), GraphState.SUCCESS))
                        .addState(GraphState.PROCESS, State.of(Action.identity(), GraphState.SUCCESS))
                        .endStates(GraphState.SUCCESS)
                        .output(_ -> "OK")
                        .build();

        try (AtomicStateMachineExecutor<OrderContext, GraphState, Void, String> executor =
                new AtomicStateMachineExecutor<>("illegal-edge-test", stateMachine, 10)) {

            assertThrows(TransitionException.class, () -> executor.dispatchSync(null));
        }
    }
}
