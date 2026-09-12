package com.github.f442y.dispersion.fsm;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Handle wrapping the asynchronous execution of a Finite State Machine on a dedicated Virtual Thread.
 *
 * @param <OUTPUT> The return result type
 */
public class StateMachineFuture<OUTPUT> implements Future<OUTPUT> {

    private final UUID uuid;
    private final CompletableFuture<OUTPUT> future;

    public StateMachineFuture(@NonNull UUID uuid, @NonNull CompletableFuture<OUTPUT> future) {
        this.uuid = Objects.requireNonNull(uuid, "uuid must not be null");
        this.future = Objects.requireNonNull(future, "future must not be null");
    }

    @NonNull
    public UUID uuid() {
        return uuid;
    }

    @NonNull
    public CompletableFuture<OUTPUT> future() {
        return future;
    }

    @NonNull
    public CompletableFuture<OUTPUT> toCompletableFuture() {
        return future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
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

    @Override
    @Nullable
    public OUTPUT get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    @Nullable
    public OUTPUT get(long timeout, @NonNull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }
}
