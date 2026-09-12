package com.github.f442y.dispersion.orchestration.core.batch;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.batch.BatchTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.ItemSignalCommand;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.batch.BatchOrchestrationStepDriver.BatchConfiguration;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Unified execution coordinator for singular items and batch collections on Java 25 Virtual Threads.
 *
 * @param <BATCH_CONTEXT> The batch-level context type
 * @param <ITEM_CONTEXT>  The item-level context type
 * @param <STATE_KEY>     The state key enum type
 * @param <OUTPUT>        The output type
 */
public class BatchOrchestrationExecutor<
        BATCH_CONTEXT extends StateMachineContext,
        ITEM_CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        OUTPUT> implements AutoCloseable {

    private final BatchConfiguration<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> configuration;
    private final Supplier<BATCH_CONTEXT> batchContextSupplier;
    private final ExecutorService executorService;
    private final Map<String, BatchOrchestrationCheckpoint<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY>> activeCheckpoints = new ConcurrentHashMap<>();

    public BatchOrchestrationExecutor(
            @NonNull BatchConfiguration<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> configuration,
            @NonNull Supplier<BATCH_CONTEXT> batchContextSupplier
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.batchContextSupplier = Objects.requireNonNull(batchContextSupplier, "batchContextSupplier must not be null");
        this.executorService = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("batch-exec-" + configuration.batchName + "-", 0).factory()
        );
    }

    /**
     * Dispatches a singular item context through the orchestration.
     */
    @NonNull
    public BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> dispatchSync(
            @NonNull ITEM_CONTEXT singleItem
    ) {
        return dispatchBatchSync(batchContextSupplier.get(), List.of(singleItem));
    }

    /**
     * Dispatches a singular item context with an explicit batch context.
     */
    @NonNull
    public BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> dispatchSync(
            @NonNull BATCH_CONTEXT initialBatchContext,
            @NonNull ITEM_CONTEXT singleItem
    ) {
        return dispatchBatchSync(initialBatchContext, List.of(singleItem));
    }

    /**
     * Asynchronously dispatches a singular item context on a Virtual Thread.
     */
    @NonNull
    public CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> dispatchAsync(
            @NonNull ITEM_CONTEXT singleItem
    ) {
        CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> future = new CompletableFuture<>();
        try {
            executorService.submit(() -> {
                try {
                    future.complete(dispatchSync(singleItem));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * Synchronously dispatches a batch of item contexts.
     */
    @NonNull
    public BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> dispatchBatchSync(
            @NonNull List<ITEM_CONTEXT> items
    ) {
        return dispatchBatchSync(batchContextSupplier.get(), items);
    }

    /**
     * Synchronously dispatches a batch of item contexts with an explicit initial batch context.
     */
    @NonNull
    public BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> dispatchBatchSync(
            @NonNull BATCH_CONTEXT initialBatchContext,
            @NonNull List<ITEM_CONTEXT> items
    ) {
        UUID batchId = UUID.randomUUID();
        BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> result =
                BatchOrchestrationStepDriver.executeBatchTurn(
                        batchId,
                        configuration,
                        initialBatchContext,
                        items,
                        executorService
                );

        saveCheckpointFromResult(result);
        return result;
    }

    /**
     * Asynchronously dispatches a batch of item contexts on a Virtual Thread.
     */
    @NonNull
    public CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> dispatchBatchAsync(
            @NonNull List<ITEM_CONTEXT> items
    ) {
        CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> future = new CompletableFuture<>();
        try {
            executorService.submit(() -> {
                try {
                    future.complete(dispatchBatchSync(items));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @NonNull
    public CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> sendItemSignal(
            @NonNull String batchKey,
            @NonNull String itemKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        Objects.requireNonNull(itemKey, "itemKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        BatchOrchestrationCheckpoint<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY> cp = activeCheckpoints.get(batchKey);
        if (cp == null) {
            CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new NoSuchElementException("No active batch checkpoint found for batchKey: " + batchKey));
            return failed;
        }

        CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> future = new CompletableFuture<>();
        try {
            executorService.submit(() -> {
                try {
                    BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> result =
                            BatchOrchestrationStepDriver.resumeBatchTurn(configuration, cp, itemKey, signalPayload, null, executorService);
                    saveCheckpointFromResult(result);
                    future.complete(result);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @NonNull
    public CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> sendBatchSignal(
            @NonNull String batchKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        BatchOrchestrationCheckpoint<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY> cp = activeCheckpoints.get(batchKey);
        if (cp == null) {
            CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new NoSuchElementException("No active batch checkpoint found for batchKey: " + batchKey));
            return failed;
        }

        CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> future = new CompletableFuture<>();
        try {
            executorService.submit(() -> {
                try {
                    BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> result =
                            BatchOrchestrationStepDriver.resumeBatchTurn(configuration, cp, null, null, signalPayload, executorService);
                    saveCheckpointFromResult(result);
                    future.complete(result);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @NonNull
    public CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull SignalCommand command
    ) {
        Objects.requireNonNull(command, "command must not be null");
        if (command instanceof ItemSignalCommand itemCmd) {
            return sendItemSignal(itemCmd.batchKey(), itemCmd.itemKey(), itemCmd.signalName(), itemCmd);
        }
        String corrKey = command.correlationKey();
        if (corrKey != null && !corrKey.isBlank()) {
            return sendBatchSignal(corrKey, command.signalName(), command);
        }
        CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalArgumentException("SignalCommand must be an ItemSignalCommand or provide a non-empty correlationKey"));
        return failed;
    }

    @NonNull
    public CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull CommandEnvelope<? extends SignalCommand> envelope
    ) {
        Objects.requireNonNull(envelope, "envelope must not be null");
        SignalCommand cmd = envelope.command();
        if (cmd instanceof ItemSignalCommand itemCmd) {
            return sendItemSignal(itemCmd.batchKey(), itemCmd.itemKey(), itemCmd.signalName(), envelope);
        }
        String corrKey = cmd.correlationKey();
        if (corrKey != null && !corrKey.isBlank()) {
            return sendBatchSignal(corrKey, cmd.signalName(), envelope);
        }
        CompletableFuture<BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalArgumentException("Command envelope must contain an ItemSignalCommand or correlationKey"));
        return failed;
    }

    private void saveCheckpointFromResult(BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> result) {
        BatchOrchestrationCheckpoint<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY> cp = new BatchOrchestrationCheckpoint<>(
                result.batchId(),
                configuration.batchName,
                result.batchKey(),
                result.status(),
                result.currentBatchStateKey(),
                result.itemStates(),
                result.itemContexts(),
                result.batchContext(),
                Collections.emptySet(),
                Collections.emptySet(),
                result.error(),
                Instant.now()
        );
        activeCheckpoints.put(result.batchKey(), cp);
    }

    @NonNull
    public BatchConfiguration<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> getConfiguration() {
        return configuration;
    }

    @NonNull
    public Optional<BatchOrchestrationCheckpoint<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY>> getCheckpoint(@NonNull String batchKey) {
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        return Optional.ofNullable(activeCheckpoints.get(batchKey));
    }

    @NonNull
    public Map<String, BatchOrchestrationCheckpoint<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY>> getActiveCheckpoints() {
        return Collections.unmodifiableMap(activeCheckpoints);
    }

    @Override
    public void close() {
        executorService.close();
    }
}

