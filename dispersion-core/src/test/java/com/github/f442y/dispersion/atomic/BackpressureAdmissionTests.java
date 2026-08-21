package com.github.f442y.dispersion.atomic;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.BackpressureException;
import com.github.f442y.dispersion.executor.AdmissionController;
import com.github.f442y.dispersion.executor.BackpressureStrategy;
import com.github.f442y.dispersion.state.StateKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class BackpressureAdmissionTests {

    public enum SimpleState implements StateKey {
        RUN, DONE
    }

    public static class SimpleContext implements StateMachineContext {
        public int value = 0;
    }

    /**
     * Tests that when buffer permits are exhausted with REJECT_IMMEDIATELY, dispatch throws BackpressureException.
     */
    @Test
    public void testAdmissionControllerPermitExhaustionThrowsBackpressure() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);

        StateMachineConfiguration<SimpleContext, SimpleState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<SimpleContext, SimpleState, Void, String>create(SimpleState.class)
                        .context(SimpleContext::new)
                        .initialState(SimpleState.RUN)
                        .state(SimpleState.RUN)
                            .action(ctx -> {
                                try {
                                    blockLatch.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return ctx;
                            })
                            .transition(SimpleState.DONE)
                        .endStates(SimpleState.DONE)
                        .output(ctx -> "OK")
                        .build();

        // Executor with REJECT_IMMEDIATELY strategy and only 1 permit
        AdmissionController admissionController = new AdmissionController(1, BackpressureStrategy.REJECT_IMMEDIATELY);
        AtomicStateMachineExecutor<SimpleContext, SimpleState, Void, String> executor =
                new AtomicStateMachineExecutor<>("backpressure-test", stateMachine, admissionController);

        // Occupy the only available permit
        executor.dispatchAsync(null);

        try {
            // Second dispatch should immediately fail admission
            assertThrows(BackpressureException.class, () -> executor.dispatchAsync(null));
        } finally {
            blockLatch.countDown();
        }
    }

    /**
     * Tests that timeout permit acquisition fails with BackpressureException when permit is not released in time.
     */
    @Test
    public void testAdmissionTimeoutThrowsBackpressure() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);

        StateMachineConfiguration<SimpleContext, SimpleState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<SimpleContext, SimpleState, Void, String>create(SimpleState.class)
                        .context(SimpleContext::new)
                        .initialState(SimpleState.RUN)
                        .state(SimpleState.RUN)
                            .action(ctx -> {
                                try {
                                    blockLatch.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return ctx;
                            })
                            .transition(SimpleState.DONE)
                        .endStates(SimpleState.DONE)
                        .output(ctx -> "OK")
                        .build();

        AdmissionController controller = new AdmissionController(1, BackpressureStrategy.WAIT_WITH_TIMEOUT, Duration.ofMillis(50));
        AtomicStateMachineExecutor<SimpleContext, SimpleState, Void, String> executor =
                new AtomicStateMachineExecutor<>("timeout-test", stateMachine, controller);

        // Occupy permit
        executor.dispatchAsync(null);

        try {
            // Should timeout waiting for permit and throw BackpressureException
            assertThrows(BackpressureException.class, () -> executor.tryDispatchAsync(null, Duration.ofMillis(50)));
        } finally {
            blockLatch.countDown();
        }
    }
}
