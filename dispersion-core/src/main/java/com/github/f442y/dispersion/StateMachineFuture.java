package com.github.f442y.dispersion;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Handle wrapping an active state machine execution and its underlying {@link Future},
 * providing cooperative cancellation and status inspection.
 *
 * @param <OUTPUT> The type of output result produced upon state machine completion
 */
public final class StateMachineFuture<OUTPUT> implements Future<OUTPUT> {

    @NonNull
    private final UUID uuid;

    @NonNull
    private final Future<OUTPUT> future;

    @Nullable
    private final Runnable onCancel;

    public StateMachineFuture(
            @NonNull StateMachineCallable<?, ?, ?, ?> stateMachineCallable,
            @NonNull Future<OUTPUT> future
    ) {
        this(
                Objects.requireNonNull(stateMachineCallable, "stateMachineCallable must not be null").uuid(),
                future,
                stateMachineCallable::cancel
        );
    }

    public StateMachineFuture(
            @NonNull UUID uuid,
            @NonNull Future<OUTPUT> future,
            @Nullable Runnable onCancel
    ) {
        this.uuid = Objects.requireNonNull(uuid, "uuid must not be null");
        this.future = Objects.requireNonNull(future, "future must not be null");
        this.onCancel = onCancel;
    }

    /**
     * Returns the unique identifier of the executing state machine.
     *
     * @return The execution {@link UUID}
     */
    @NonNull
    public UUID uuid() {
        return uuid;
    }

    /**
     * Cancels the state machine execution cooperatively and attempts to cancel the underlying task.
     *
     * @param mayInterruptIfRunning Whether to interrupt the virtual thread if running
     * @return {@code true} if successfully cancelled; otherwise {@code false}
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (onCancel != null) {
            try {
                onCancel.run();
            } catch (Exception ignored) {
                // Ignore failure during cooperative cancellation callback
            }
        }
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Nullable
    @Override
    public OUTPUT get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Nullable
    @Override
    public OUTPUT get(long timeout, @NonNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    /**
     * Returns the underlying raw {@link Future}.
     *
     * @return The underlying {@link Future} instance
     */
    @NonNull
    public Future<OUTPUT> future() {
        return future;
    }

    /**
     * Adapts this future into a standard {@link CompletableFuture}.
     *
     * @return A {@link CompletableFuture} completing with the state machine result
     */
    @NonNull
    public CompletableFuture<OUTPUT> toCompletableFuture() {
        if (future instanceof CompletableFuture<OUTPUT> cf) {
            return cf;
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                return future.get();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }
}
