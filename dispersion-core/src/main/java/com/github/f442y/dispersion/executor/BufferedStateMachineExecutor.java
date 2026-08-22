package com.github.f442y.dispersion.executor;

import com.github.f442y.dispersion.AbstractStateMachineCallable;
import com.github.f442y.dispersion.StateMachineFuture;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.BackpressureException;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Ultra-fast Virtual Thread-backed state machine executor with zero-sync direct execution
 * and admission control for high-throughput micro-state machines.
 *
 * <h2>Execution Modes &amp; Performance Profile</h2>
 * <ul>
 *   <li><b>Synchronous Fast-Path ({@link #dispatchSync(Object)}):</b> Runs directly within the caller's virtual thread.
 *       Acquires an admission permit via {@link AdmissionController}, executes {@link AbstractStateMachineCallable#executeDirect},
 *       and releases the permit in a {@code finally} block. This mode incurs zero thread-hopping, zero {@link CompletableFuture}
 *       allocations, and zero context switching.</li>
 *   <li><b>Asynchronous Virtual Threads ({@link #dispatchAsync(Object)}):</b> Asynchronously acquires a permit and dispatches
 *       the execution to an unpinned, lightweight Java 25 Virtual Thread via a thread-per-task executor. Returns a strongly-typed
 *       {@link StateMachineFuture} carrying the unique execution {@link UUID}.</li>
 *   <li><b>Adaptive Admission Control &amp; Backpressure:</b> Protects downstream services and system memory by throttling
 *       concurrent runs according to configurable policies (immediate rejection, blocking, or timed timeouts).</li>
 * </ul>
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
 * @param <STATE_KEY> The state identifier enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output return type
 */
public class BufferedStateMachineExecutor<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> implements StateMachineExecutor<CONTEXT, INPUT, OUTPUT> {

    private static final Logger log = LoggerFactory.getLogger(BufferedStateMachineExecutor.class);

    private final String name;
    private final StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;
    private final AdmissionController admissionController;
    private final ExecutorService virtualThreadExecutor;

    public BufferedStateMachineExecutor(
            @NonNull String name,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull AdmissionController admissionController
    ) {
        this(
                name,
                configuration,
                admissionController,
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(name + "-", 0).factory())
        );
    }

    public BufferedStateMachineExecutor(
            @NonNull String name,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull AdmissionController admissionController,
            @NonNull ExecutorService virtualThreadExecutor
    ) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.admissionController = Objects.requireNonNull(admissionController, "admissionController must not be null");
        this.virtualThreadExecutor = Objects.requireNonNull(virtualThreadExecutor, "virtualThreadExecutor must not be null");
    }

    /**
     * Ultra-low-overhead synchronous direct execution path avoiding thread hops and future allocations.
     */
    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable INPUT input) throws Exception {
        return dispatchSync(null, input);
    }

    /**
     * Ultra-low-overhead synchronous direct execution path with explicit context.
     */
    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable CONTEXT initialContext, @Nullable INPUT input) throws Exception {
        try {
            admissionController.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BackpressureException("Interrupted while acquiring execution permit", e);
        }

        try {
            return AbstractStateMachineCallable.executeDirect(null, configuration, initialContext, input);
        } finally {
            admissionController.release();
        }
    }

    @Override
    @NonNull
    public StateMachineFuture<OUTPUT> dispatchAsync(@Nullable INPUT input) {
        return dispatchAsyncInternal(null, input, null);
    }

    @Override
    @NonNull
    public StateMachineFuture<OUTPUT> dispatchAsync(@NonNull CONTEXT initialContext, @Nullable INPUT input) {
        Objects.requireNonNull(initialContext, "initialContext must not be null");
        return dispatchAsyncInternal(initialContext, input, null);
    }

    @NonNull
    public StateMachineFuture<OUTPUT> tryDispatchAsync(@Nullable INPUT input, @NonNull Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        return dispatchAsyncInternal(null, input, timeout);
    }

    @NonNull
    private StateMachineFuture<OUTPUT> dispatchAsyncInternal(
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input,
            @Nullable Duration timeout
    ) {
        try {
            if (timeout != null) {
                admissionController.acquire(timeout);
            } else {
                admissionController.acquire();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BackpressureException("Interrupted while acquiring execution permit", e);
        }

        UUID executionId = UUID.randomUUID();
        CompletableFuture<OUTPUT> future = new CompletableFuture<>();

        virtualThreadExecutor.submit(() -> {
            try {
                OUTPUT result = AbstractStateMachineCallable.executeDirect(executionId, configuration, initialContext, input);
                future.complete(result);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                admissionController.release();
            }
        });

        return new StateMachineFuture<>(executionId, future);
    }

    @NonNull
    public String getName() {
        return name;
    }

    @NonNull
    public StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> getConfiguration() {
        return configuration;
    }

    @NonNull
    public AdmissionController getAdmissionController() {
        return admissionController;
    }

    @Override
    public void close() {
        virtualThreadExecutor.close();
    }
}
