package com.github.f442y.dispersion.executor;

import com.github.f442y.dispersion.StateMachineCallable;
import com.github.f442y.dispersion.StateMachineFuture;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.BackpressureException;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Executes Finite State Machines on Java Virtual Threads with admission control and backpressure management.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by the state machine
 * @param <OUTPUT>    The type of output result produced upon state machine completion
 */
public abstract class BufferedStateMachineExecutor<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        implements StateMachineExecutor<CONTEXT, INPUT, OUTPUT> {

    @NonNull
    private final ExecutorService executorService;

    @NonNull
    private final StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachineConfiguration;

    @NonNull
    private final AdmissionController admissionController;

    /**
     * Constructs a {@link BufferedStateMachineExecutor} with the specified concurrency buffer limit.
     *
     * @param serviceThreadName         Thread name prefix for spawned virtual threads
     * @param stateMachineConfiguration Configuration defining the state machine structure
     * @param bufferSize                Maximum concurrent state machines allowed before backpressure activates
     */
    protected BufferedStateMachineExecutor(
            @NonNull String serviceThreadName,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachineConfiguration,
            int bufferSize
    ) {
        this(serviceThreadName, stateMachineConfiguration, new AdmissionController(bufferSize));
    }

    /**
     * Constructs a {@link BufferedStateMachineExecutor} with a custom {@link AdmissionController}.
     *
     * @param serviceThreadName         Thread name prefix for spawned virtual threads
     * @param stateMachineConfiguration Configuration defining the state machine structure
     * @param admissionController       Custom admission controller managing concurrency permits
     */
    protected BufferedStateMachineExecutor(
            @NonNull String serviceThreadName,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachineConfiguration,
            @NonNull AdmissionController admissionController
    ) {
        this.executorService = Executors.newThreadPerTaskExecutor(Thread
                .ofVirtual()
                .name(String.format("%s-bv-", serviceThreadName), 0)
                .factory());
        this.stateMachineConfiguration = Objects.requireNonNull(stateMachineConfiguration, "stateMachineConfiguration must not be null");
        this.admissionController = Objects.requireNonNull(admissionController, "admissionController must not be null");
    }

    /**
     * Dispatches a state machine asynchronously on a dedicated virtual thread.
     *
     * @param input The input payload for the state machine
     * @return A {@link StateMachineFuture} representing the pending execution result
     * @throws InterruptedException If the calling thread is interrupted while waiting for an admission permit
     * @throws BackpressureException If the execution request is rejected by backpressure admission control
     */
    @NonNull
    @Override
    public StateMachineFuture<OUTPUT> dispatchAsync(@Nullable INPUT input) throws InterruptedException, BackpressureException {
        return dispatchAsync(null, input, null);
    }

    /**
     * Dispatches a state machine asynchronously with a pre-populated initial context.
     *
     * @param initialContext Pre-populated context instance
     * @param input          The input payload
     * @return A {@link StateMachineFuture} representing the pending execution result
     * @throws InterruptedException If interrupted while waiting for an admission permit
     * @throws BackpressureException If rejected by backpressure admission control
     */
    @NonNull
    @Override
    public StateMachineFuture<OUTPUT> dispatchAsync(@Nullable CONTEXT initialContext, @Nullable INPUT input)
            throws InterruptedException, BackpressureException {
        return dispatchAsync(initialContext, input, null);
    }

    /**
     * Dispatches a state machine asynchronously with a custom timeout for permit acquisition.
     *
     * @param input   The input payload
     * @param timeout The maximum duration to wait for an admission permit
     * @return A {@link StateMachineFuture} representing the pending execution result
     * @throws InterruptedException If interrupted while waiting for an admission permit
     * @throws BackpressureException If permit acquisition times out or is rejected
     */
    @NonNull
    public StateMachineFuture<OUTPUT> tryDispatchAsync(@Nullable INPUT input, @NonNull Duration timeout)
            throws InterruptedException, BackpressureException {
        return dispatchAsync(null, input, timeout);
    }

    /**
     * Dispatches a state machine asynchronously with initial context and timeout configuration.
     *
     * @param initialContext Pre-populated context instance
     * @param input          The input payload
     * @param timeout        Custom timeout duration for permit acquisition
     * @return A {@link StateMachineFuture} representing the pending execution result
     * @throws InterruptedException If interrupted while waiting for an admission permit
     * @throws BackpressureException If permit acquisition times out or is rejected
     */
    @NonNull
    public StateMachineFuture<OUTPUT> dispatchAsync(
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input,
            @Nullable Duration timeout
    ) throws InterruptedException, BackpressureException {
        admissionController.acquirePermit(timeout);

        StateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> stateMachineCallable =
                new StateMachineCallable.StateMachineCallableBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT>()
                        .onReleasePermit(admissionController::releasePermit)
                        .initialContext(initialContext)
                        .input(input)
                        .stateMachine(stateMachineConfiguration);

        Future<OUTPUT> future = executorService.submit(stateMachineCallable);
        return new StateMachineFuture<>(stateMachineCallable, future);
    }

    /**
     * Dispatches a state machine synchronously on a virtual thread and blocks the caller until completion.
     *
     * @param input The input payload for the state machine
     * @return The final output result produced by the state machine
     * @throws Exception If execution encounters a state machine fault or error
     */
    @Nullable
    @Override
    public OUTPUT dispatchSync(@Nullable INPUT input) throws Exception {
        return dispatchSync(null, input);
    }

    /**
     * Dispatches a state machine synchronously with a pre-populated initial context.
     *
     * @param initialContext Pre-populated context instance
     * @param input          The input payload
     * @return The final output result produced by the state machine
     * @throws Exception If execution encounters a state machine fault or error
     */
    @Nullable
    @Override
    public OUTPUT dispatchSync(@Nullable CONTEXT initialContext, @Nullable INPUT input)
            throws Exception {
        try {
            return dispatchAsync(initialContext, input).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StateMachineException sme) {
                throw sme;
            } else if (e.getCause() instanceof RuntimeException re) {
                throw re;
            } else if (e.getCause() instanceof Exception ex) {
                throw ex;
            } else {
                throw new RuntimeException("State machine execution failed", e.getCause());
            }
        }
    }

    /**
     * Returns the underlying {@link AdmissionController} managing concurrency permits.
     *
     * @return The active {@link AdmissionController}
     */
    @NonNull
    public AdmissionController getAdmissionController() {
        return admissionController;
    }

    /**
     * Closes the underlying virtual thread executor.
     */
    @Override
    public void close() {
        executorService.close();
    }
}
