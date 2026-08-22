package com.github.f442y.dispersion.executor;

import com.github.f442y.dispersion.StateMachineFuture;
import com.github.f442y.dispersion.atomic.AtomicStateMachineCallable;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Virtual Thread-backed state machine executor with admission control and backpressure management.
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

    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable INPUT input) throws Exception {
        try {
            return dispatchAsync(input).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception ex) {
                throw ex;
            }
            throw e;
        }
    }

    @Override
    @Nullable
    public OUTPUT dispatchSync(@NonNull CONTEXT initialContext, @Nullable INPUT input) throws Exception {
        try {
            return dispatchAsync(initialContext, input).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception ex) {
                throw ex;
            }
            throw e;
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
        UUID executionId = UUID.randomUUID();

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

        CompletableFuture<OUTPUT> future = new CompletableFuture<>();
        AtomicStateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> callable =
                new AtomicStateMachineCallable<>(executionId, configuration, initialContext, input);

        virtualThreadExecutor.submit(() -> {
            try {
                OUTPUT result = callable.call();
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
