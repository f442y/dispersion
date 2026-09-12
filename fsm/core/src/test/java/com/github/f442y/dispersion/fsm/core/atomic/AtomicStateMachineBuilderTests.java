package com.github.f442y.dispersion.fsm.core.atomic;

import com.github.f442y.dispersion.fsm.StateMachineFuture;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class AtomicStateMachineBuilderTests {

    public enum FlowState implements StateKey {
        INIT, STEP_ONE, STEP_TWO, DONE
    }

    public static class FlowContext implements StateMachineContext {
        public int counter = 0;
        public List<String> trail = new ArrayList<>();
    }

    @Test
    public void testAtomicStateMachineExecution() throws Exception {
        StateMachineConfiguration<FlowContext, FlowState, Integer, String> stateMachine =
                AtomicStateMachineBuilder.<FlowContext, FlowState, Integer, String>create(FlowState.class)
                        .context(FlowContext::new)
                        .initialState(FlowState.INIT)
                        .input((ctx, input) -> {
                            ctx.counter = (input != null) ? input : 0;
                            ctx.trail.add("INPUT:" + input);
                            return ctx;
                        })
                        .state(FlowState.INIT)
                            .action(ctx -> {
                                ctx.counter += 10;
                                ctx.trail.add("INIT");
                                return ctx;
                            })
                            .transition(FlowState.STEP_ONE)
                        .state(FlowState.STEP_ONE)
                            .action(ctx -> {
                                ctx.counter *= 2;
                                ctx.trail.add("STEP_ONE");
                                return ctx;
                            })
                            .transition(FlowState.STEP_TWO)
                        .state(FlowState.STEP_TWO)
                            .action(ctx -> {
                                ctx.counter += 5;
                                ctx.trail.add("STEP_TWO");
                                return ctx;
                            })
                            .transition(FlowState.DONE)
                        .endStates(FlowState.DONE)
                        .output(ctx -> "Result=" + ctx.counter)
                        .build();

        try (AtomicStateMachineExecutor<FlowContext, FlowState, Integer, String> executor =
                new AtomicStateMachineExecutor<>("atomic-test", stateMachine, 10)) {

            // (5 + 10) * 2 + 5 = 35
            String result = executor.dispatchSync(5);
            assertEquals("Result=35", result);

            StateMachineFuture<String> future = executor.dispatchAsync(10);
            // (10 + 10) * 2 + 5 = 45
            assertEquals("Result=45", future.get());
        }
    }

    @Test
    public void testCustomInitialContext() throws Exception {
        StateMachineConfiguration<FlowContext, FlowState, Void, List<String>> stateMachine =
                AtomicStateMachineBuilder.<FlowContext, FlowState, Void, List<String>>create(FlowState.class)
                        .context(FlowContext::new)
                        .initialState(FlowState.INIT)
                        .state(FlowState.INIT)
                            .action(ctx -> {
                                ctx.trail.add("VISITED_INIT");
                                return ctx;
                            })
                            .transition(FlowState.DONE)
                        .endStates(FlowState.DONE)
                        .output(ctx -> ctx.trail)
                        .build();

        try (AtomicStateMachineExecutor<FlowContext, FlowState, Void, List<String>> executor =
                new AtomicStateMachineExecutor<>("custom-ctx-test", stateMachine, 10)) {

            FlowContext initialCtx = new FlowContext();
            initialCtx.trail.add("PRE_SEEDED");

            List<String> result = executor.dispatchSync(initialCtx, null);
            assertIterableEquals(List.of("PRE_SEEDED", "VISITED_INIT"), result);
        }
    }
}
