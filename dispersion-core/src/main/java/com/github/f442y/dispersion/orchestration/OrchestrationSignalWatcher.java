package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Signal ingestion and rehydration watcher correlating inbound signals and commands
 * with active checkpoints in the {@link CheckpointStore} and resuming execution on Virtual Threads.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 * @param <INPUT>     The input type
 * @param <OUTPUT>    The output type
 */
public class OrchestrationSignalWatcher<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> {

    private static final int STRIPE_COUNT = 512;
    private static final int STRIPE_MASK = STRIPE_COUNT - 1;

    private final OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;
    private final ExecutorService virtualThreadExecutor;
    private final ReentrantLock[] locks;

    public OrchestrationSignalWatcher(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull ExecutorService virtualThreadExecutor
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.virtualThreadExecutor = Objects.requireNonNull(virtualThreadExecutor, "virtualThreadExecutor must not be null");
        this.locks = new ReentrantLock[STRIPE_COUNT];
        for (int i = 0; i < STRIPE_COUNT; i++) {
            this.locks[i] = new ReentrantLock();
        }
    }

    private ReentrantLock lockFor(UUID machineId) {
        int hash = machineId.hashCode();
        int index = Math.abs(hash & STRIPE_MASK);
        return locks[index];
    }

    private ReentrantLock lockFor(String correlationKey) {
        int hash = correlationKey.hashCode();
        int index = Math.abs(hash & STRIPE_MASK);
        return locks[index];
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> deliverSignal(
            @NonNull UUID machineId,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        return sendSignal(machineId, signalName, signalPayload);
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> sendSignal(
            @NonNull UUID machineId,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        return executeUnderLock(lockFor(machineId), () -> {
            CheckpointStore<CONTEXT, STATE_KEY> store = configuration.getCheckpointStore();
            if (store == null) {
                throw new IllegalStateException("CheckpointStore must be configured to process signals via OrchestrationSignalWatcher");
            }

            Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> cpOpt = store.findById(machineId);
            if (cpOpt.isEmpty()) {
                throw new NoSuchElementException("No checkpoint found for machineId: " + machineId);
            }

            return OrchestrationStepDriver.resumeTurn(configuration, cpOpt.get(), signalPayload, virtualThreadExecutor);
        });
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> deliverSignalByCorrelationKey(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        return sendSignalByCorrelationKey(correlationKey, signalName, signalPayload);
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> sendSignalByCorrelationKey(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        return executeUnderLock(lockFor(correlationKey), () -> {
            CheckpointStore<CONTEXT, STATE_KEY> store = configuration.getCheckpointStore();
            if (store == null) {
                throw new IllegalStateException("CheckpointStore must be configured to process signals via OrchestrationSignalWatcher");
            }

            Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> cpOpt = store.findByCorrelationKey(correlationKey);
            if (cpOpt.isEmpty()) {
                throw new NoSuchElementException("No checkpoint found for correlationKey: " + correlationKey);
            }

            return OrchestrationStepDriver.resumeTurn(configuration, cpOpt.get(), signalPayload, virtualThreadExecutor);
        });
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull SignalCommand command
    ) {
        Objects.requireNonNull(command, "command must not be null");
        return handleCommand(new CommandEnvelope<>(UUID.randomUUID(), Instant.now(), command));
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull CommandEnvelope<? extends SignalCommand> envelope
    ) {
        Objects.requireNonNull(envelope, "envelope must not be null");
        SignalCommand cmd = envelope.command();
        String corrKey = cmd.correlationKey();

        if (corrKey == null || corrKey.isBlank()) {
            CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException("SignalCommand must have a non-empty correlationKey"));
            return failed;
        }

        return sendSignalByCorrelationKey(corrKey, cmd.signalName(), envelope);
    }

    private CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> executeUnderLock(
            ReentrantLock lock,
            CallableSupplier<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> action
    ) {
        CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> future = new CompletableFuture<>();

        try {
            virtualThreadExecutor.submit(() -> {
                lock.lock();
                try {
                    future.complete(action.get());
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                } finally {
                    lock.unlock();
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }

        return future;
    }

    @FunctionalInterface
    private interface CallableSupplier<T> {
        T get() throws Exception;
    }
}
