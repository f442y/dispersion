package com.github.f442y.dispersion.fsm.core.fixture;

import com.github.f442y.dispersion.fsm.config.InputFunction;
import com.github.f442y.dispersion.fsm.config.OutputFunction;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.context.StateMachineContextFactory;
import com.github.f442y.dispersion.fsm.core.fixture.states.StateA;
import com.github.f442y.dispersion.fsm.core.fixture.states.StateB;
import com.github.f442y.dispersion.fsm.core.fixture.states.StateC;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CountDownLatch;

public class TestStateMachine
        extends StateMachineConfiguration<TestStateMachine.TestStateMachineContext, TestStateMachine.StateKeys,
        TestStateMachine.WrappedInput, Integer> {

    public enum StateKeys implements StateKey {
        A, B, C, END
    }

    private static final TestStateMachineContext CONTEXT_FACTORY = new TestStateMachineContext();

    public record WrappedInput(CountDownLatch latch, Integer input) {}

    public TestStateMachine() {
        super(
                StateMap.<TestStateMachineContext, StateKeys>builder(StateKeys.class)
                        .addState(StateKeys.A, new StateA())
                        .addState(StateKeys.B, new StateB())
                        .addState(StateKeys.C, new StateC())
                        .initialState(StateKeys.A)
                        .endState(StateKeys.END)
                        .build()
        );
    }

    @NonNull
    @Override
    public StateMachineContextFactory<TestStateMachineContext> stateMachineContextFactory() {
        return CONTEXT_FACTORY;
    }

    @Nullable
    @Override
    public InputFunction<TestStateMachineContext, WrappedInput> inputFunction() {
        return (testStateMachineContext, wrappedInput) -> {
            testStateMachineContext.num = 10;
            if (wrappedInput != null && wrappedInput.latch != null) {
                testStateMachineContext.latch = wrappedInput.latch;
            }
            return testStateMachineContext;
        };
    }

    @Nullable
    @Override
    public OutputFunction<TestStateMachineContext, Integer> outputFunction() {
        return testStateMachineContext -> {
            if (testStateMachineContext.latch != null) {
                testStateMachineContext.latch.countDown();
            }
            return testStateMachineContext.num;
        };
    }

    public static class TestStateMachineContext implements StateMachineContext,
            StateMachineContextFactory<TestStateMachineContext> {
        public String string = "Simple Contextual String (Test)";
        public int num = 0;
        public CountDownLatch latch;

        @NonNull
        @Override
        public TestStateMachineContext newInstance() {
            return new TestStateMachineContext();
        }
    }
}
