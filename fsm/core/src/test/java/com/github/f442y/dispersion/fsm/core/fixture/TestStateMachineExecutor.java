package com.github.f442y.dispersion.fsm.core.fixture;

import com.github.f442y.dispersion.fsm.StateMachineFuture;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.core.executor.AdmissionController;
import com.github.f442y.dispersion.fsm.core.executor.BufferedStateMachineExecutor;
import com.github.f442y.dispersion.fsm.exception.BackpressureException;

public class TestStateMachineExecutor
        extends BufferedStateMachineExecutor<TestStateMachine.TestStateMachineContext, TestStateMachine.StateKeys,
        TestStateMachine.WrappedInput, Integer> {

    public static final StateMachineConfiguration<TestStateMachine.TestStateMachineContext,
            TestStateMachine.StateKeys, TestStateMachine.WrappedInput, Integer>
            TEST_STATE_MACHINE = new TestStateMachine();

    public TestStateMachineExecutor() {
        super(
                "buffered-blocking",
                TEST_STATE_MACHINE,
                new AdmissionController(Runtime.getRuntime().availableProcessors() * 3)
        );
    }

    public StateMachineFuture<Integer> dispatch() throws InterruptedException, BackpressureException {
        return super.dispatchAsync(new TestStateMachine.WrappedInput(null, null));
    }

    public StateMachineFuture<Integer> dispatchWithInput(TestStateMachine.WrappedInput wrappedInput)
            throws InterruptedException, BackpressureException {
        return super.dispatchAsync(wrappedInput);
    }
}
