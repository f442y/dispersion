package com.github.f442y.dispersion.fsm.core.atomic;

import com.github.f442y.dispersion.fsm.StateMachineFuture;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.executor.AdmissionController;
import com.github.f442y.dispersion.fsm.exception.BackpressureException;
import com.github.f442y.dispersion.fsm.executor.BackpressureStrategy;
import com.github.f442y.dispersion.fsm.state.StateKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    public void testAdmissionControllerPermitExhaustionThrowsBackpressure() {
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
                        .output(_ -> "OK")
                        .build();

        // Executor with REJECT_IMMEDIATELY strategy and only 1 permit
        AdmissionController admissionController = new AdmissionController(1, BackpressureStrategy.REJECT_IMMEDIATELY);
        try (AtomicStateMachineExecutor<SimpleContext, SimpleState, Void, String> executor =
                new AtomicStateMachineExecutor<>("backpressure-test", stateMachine, admissionController)) {

            // Occupy the only available permit
            executor.dispatchAsync(null);

            try {
                // Second dispatch should immediately fail admission
                assertThrows(BackpressureException.class, () -> executor.dispatchAsync(null));
            } finally {
                blockLatch.countDown();
            }
        }
    }

    /**
     * Tests that timeout permit acquisition fails with BackpressureException when permit is not released in time.
     */
    @Test
    public void testAdmissionTimeoutThrowsBackpressure() throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        CountDownLatch blockLatch = new CountDownLatch(1);

        StateMachineConfiguration<SimpleContext, SimpleState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<SimpleContext, SimpleState, Void, String>create(SimpleState.class)
                        .context(SimpleContext::new)
                        .initialState(SimpleState.RUN)
                        .state(SimpleState.RUN)
                            .action(ctx -> {
                                startedLatch.countDown();
                                try {
                                    blockLatch.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return ctx;
                            })
                            .transition(SimpleState.DONE)
                        .endStates(SimpleState.DONE)
                        .output(_ -> "OK")
                        .build();

        AdmissionController controller = new AdmissionController(1, BackpressureStrategy.WAIT_WITH_TIMEOUT, Duration.ofMillis(50));
        try (AtomicStateMachineExecutor<SimpleContext, SimpleState, Void, String> executor =
                new AtomicStateMachineExecutor<>("timeout-test", stateMachine, controller)) {

            // Occupy permit and wait until task has actually acquired permit and entered RUN
            executor.dispatchAsync(null);
            assertTrue(startedLatch.await(5, TimeUnit.SECONDS));

            try {
                // Should timeout waiting for permit and throw BackpressureException
                assertThrows(BackpressureException.class, () -> executor.tryDispatchAsync(null, Duration.ofMillis(50)));
            } finally {
                blockLatch.countDown();
            }
        }
    }

    /**
     * Tests that with BLOCK strategy, dispatchAsync returns a StateMachineFuture immediately without
     * blocking the calling thread, parking permit acquisition on the dedicated Virtual Thread instead.
     */
    @Test
    public void testDispatchAsyncWithBlockStrategyDoesNotBlockCallingThread() throws Exception {
        CountDownLatch task1RunningLatch = new CountDownLatch(1);
        CountDownLatch task1BlockLatch = new CountDownLatch(1);

        StateMachineConfiguration<SimpleContext, SimpleState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<SimpleContext, SimpleState, Void, String>create(SimpleState.class)
                        .context(SimpleContext::new)
                        .initialState(SimpleState.RUN)
                        .state(SimpleState.RUN)
                            .action(ctx -> {
                                task1RunningLatch.countDown();
                                try {
                                    task1BlockLatch.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return ctx;
                            })
                            .transition(SimpleState.DONE)
                        .endStates(SimpleState.DONE)
                        .output(_ -> "OK")
                        .build();

        // 1 permit with BLOCK strategy
        AdmissionController controller = new AdmissionController(1, BackpressureStrategy.BLOCK);
        try (AtomicStateMachineExecutor<SimpleContext, SimpleState, Void, String> executor =
                new AtomicStateMachineExecutor<>("async-block-test", stateMachine, controller)) {

            // Task 1 occupies the permit
            StateMachineFuture<String> future1 = executor.dispatchAsync(null);
            assertTrue(task1RunningLatch.await(5, TimeUnit.SECONDS), "Task 1 should start running");

            // Task 2 is dispatched while permits are exhausted.
            // Under non-blocking async backpressure, this call MUST return immediately without blocking the caller!
            long startTime = System.nanoTime();
            StateMachineFuture<String> future2 = executor.dispatchAsync(null);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

            // Caller thread returned immediately (well under 500ms)
            assertThat(elapsedMillis).isLessThan(500);
            assertFalse(future2.isDone(), "Task 2 should be waiting for permit on its virtual thread, not done");

            // Unblock task 1 so permit is released
            task1BlockLatch.countDown();

            // Both tasks should now complete successfully
            assertThat(future1.get(5, TimeUnit.SECONDS)).isEqualTo("OK");
            assertThat(future2.get(5, TimeUnit.SECONDS)).isEqualTo("OK");
        }
    }

    /**
     * Tests that with WAIT_WITH_TIMEOUT strategy and dispatchAsync(null), the calling thread returns immediately
     * and the StateMachineFuture completes exceptionally if the timeout expires while waiting on the virtual thread.
     */
    @Test
    public void testDispatchAsyncWithWaitWithTimeoutFailsFutureWhenPermitTimesOut() throws Exception {
        CountDownLatch task1RunningLatch = new CountDownLatch(1);
        CountDownLatch task1BlockLatch = new CountDownLatch(1);

        StateMachineConfiguration<SimpleContext, SimpleState, Void, String> stateMachine =
                AtomicStateMachineBuilder.<SimpleContext, SimpleState, Void, String>create(SimpleState.class)
                        .context(SimpleContext::new)
                        .initialState(SimpleState.RUN)
                        .state(SimpleState.RUN)
                            .action(ctx -> {
                                task1RunningLatch.countDown();
                                try {
                                    task1BlockLatch.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return ctx;
                            })
                            .transition(SimpleState.DONE)
                        .endStates(SimpleState.DONE)
                        .output(_ -> "OK")
                        .build();

        // 1 permit with WAIT_WITH_TIMEOUT (100ms)
        AdmissionController controller = new AdmissionController(1, BackpressureStrategy.WAIT_WITH_TIMEOUT, Duration.ofMillis(100));
        try (AtomicStateMachineExecutor<SimpleContext, SimpleState, Void, String> executor =
                new AtomicStateMachineExecutor<>("async-timeout-future-test", stateMachine, controller)) {

            // Task 1 occupies the permit
            StateMachineFuture<String> future1 = executor.dispatchAsync(null);
            assertTrue(task1RunningLatch.await(5, TimeUnit.SECONDS));

            // Task 2 dispatched with standard dispatchAsync (timeout=null).
            // It should return immediately without blocking caller thread, and fail the future when 100ms expires.
            long startTime = System.nanoTime();
            StateMachineFuture<String> future2 = executor.dispatchAsync(null);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

            assertThat(elapsedMillis).isLessThan(500);

            // Future 2 should fail with ExecutionException whose cause is BackpressureException
            ExecutionException ex = assertThrows(ExecutionException.class, () -> future2.get(5, TimeUnit.SECONDS));
            assertThat(ex.getCause()).isInstanceOf(BackpressureException.class);

            // Clean up task 1
            task1BlockLatch.countDown();
            assertThat(future1.get(5, TimeUnit.SECONDS)).isEqualTo("OK");
        }
    }
}
