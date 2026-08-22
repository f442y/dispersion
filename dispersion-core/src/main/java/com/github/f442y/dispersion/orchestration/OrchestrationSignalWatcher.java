package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

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

    private static final Logger log = LoggerFactory.getLogger(OrchestrationSignalWatcher.class);

    private final OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;
    private final ExecutorService virtualThreadExecutor;
    private final ConcurrentHashMap<String, Object> executionLocks = new ConcurrentHashMap<>();

    public OrchestrationSignalWatcher(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull ExecutorService virtualThreadExecutor
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.virtualThreadExecutor = Objects.requireNonNull(virtualThreadExecutor, "virtualThreadExecutor must not be null");
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> deliverSignal(
            @NonNull UUID machineId,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        var store = configuration.getCheckpointStore();
        if (store == null) {
            CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("CheckpointStore is required for signal delivery"));
            return failed;
        }

        return resumeOnVirtualThread(machineId.toString(), () -> store.findById(machineId), signalPayload);
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> deliverSignalByCorrelationKey(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        var store = configuration.getCheckpointStore();
        if (store == null) {
            CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("CheckpointStore is required for signal delivery"));
            return failed;
        }

        return resumeOnVirtualThread(correlationKey, () -> store.findByCorrelationKey(correlationKey), signalPayload);
    }

    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull SignalCommand command
    ) {
        Objects.requireNonNull(command, "command must not be null");
        String corrKey = command.correlationKey();
        if (corrKey == null || corrKey.isBlank()) {
            CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalArgumentException("SignalCommand must provide a non-empty correlationKey"));
            return failed;
        }
        return deliverSignalByCorrelationKey(corrKey, command.signalName(), command);
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
            failed.completeExceptionally(new IllegalArgumentException("Command envelope command must provide a non-empty correlationKey"));
            return failed;
        }
        return deliverSignalByCorrelationKey(corrKey, cmd.signalName(), envelope);
    }

    private CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> resumeOnVirtualThread(
            String lockKey,
            Supplier<Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>>> checkpointSupplier,
            Object signalPayload
    ) {
        CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> future = new CompletableFuture<>();

        virtualThreadExecutor.submit(() -> {
            Object lock = executionLocks.computeIfAbsent(lockKey, k -> new Object());
            synchronized (lock) {
                try {
                    Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> cpOpt = checkpointSupplier.get();
                    if (cpOpt.isEmpty()) {
                        future.completeExceptionally(new NoSuchElementException("No active checkpoint found for key: " + lockKey));
                        return;
                    }

                    OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint = cpOpt.get();
                    OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> result =
                            OrchestrationStepDriver.resumeTurn(configuration, checkpoint, signalPayload, virtualThreadExecutor);
                    future.complete(result);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            }
        });

        return future;
    }
}
