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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Ingestion coordinator and event correlator for external signals and {@link SignalCommand} instances
 * destined for suspended Orchestration State Machines.
 * Retrieves checkpoints from {@link CheckpointStore}, validates expected signal contracts, and rehydrates workflows
 * on fresh Virtual Threads via {@link OrchestrationStepDriver}.
 *
 * @param <CONTEXT>   The orchestration context type
 * @param <STATE_KEY> The orchestration state key enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output result type
 */
public class OrchestrationSignalWatcher<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationSignalWatcher.class);

    @NonNull
    private final OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;

    @NonNull
    private final CheckpointStore<CONTEXT, STATE_KEY> checkpointStore;

    @NonNull
    private final ExecutorService virtualThreadExecutor;

    public OrchestrationSignalWatcher(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull CheckpointStore<CONTEXT, STATE_KEY> checkpointStore,
            @NonNull ExecutorService virtualThreadExecutor
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore must not be null");
        this.virtualThreadExecutor = Objects.requireNonNull(virtualThreadExecutor, "virtualThreadExecutor must not be null");
    }

    /**
     * Delivers an external signal to a suspended orchestration workflow by its unique machine UUID.
     *
     * @param machineId     The unique workflow machine ID
     * @param signalName    The name of the incoming signal
     * @param signalPayload The signal event data payload
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> sendSignal(
            @NonNull UUID machineId,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint = checkpointStore.findById(machineId)
                .orElseThrow(() -> new NoSuchElementException("No active orchestration checkpoint found for ID: " + machineId));

        return resumeWithCheckpoint(checkpoint, signalName, signalPayload);
    }

    /**
     * Delivers an external signal to a suspended orchestration workflow by its business correlation key.
     *
     * @param correlationKey The correlation identifier (e.g. orderId, transactionId)
     * @param signalName     The name of the incoming signal
     * @param signalPayload  The signal event data payload
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> sendSignalByCorrelationKey(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint = checkpointStore.findByCorrelationKey(correlationKey)
                .orElseThrow(() -> new NoSuchElementException("No active orchestration checkpoint found for correlation key: " + correlationKey));

        return resumeWithCheckpoint(checkpoint, signalName, signalPayload);
    }

    /**
     * Dispatches a strongly typed {@link SignalCommand} using the correlation key declared on the command.
     *
     * @param command The typed signal command
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull SignalCommand command
    ) {
        Objects.requireNonNull(command, "command must not be null");
        String corrKey = command.correlationKey();
        if (corrKey == null || corrKey.isBlank()) {
            throw new IllegalArgumentException("SignalCommand '" + command.getClass().getSimpleName() + "' must declare a non-blank correlationKey() when dispatched without machineId");
        }
        return sendSignalByCorrelationKey(corrKey, command.signalName(), command);
    }

    /**
     * Dispatches a strongly typed {@link SignalCommand} directly to a specific workflow machine UUID.
     *
     * @param machineId The workflow machine ID
     * @param command   The typed signal command
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull UUID machineId,
            @NonNull SignalCommand command
    ) {
        Objects.requireNonNull(command, "command must not be null");
        return sendSignal(machineId, command.signalName(), command);
    }

    /**
     * Dispatches an idempotent {@link CommandEnvelope} using the correlation key declared on the command.
     *
     * @param envelope The command envelope containing deduplication commandId and payload
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull CommandEnvelope<? extends SignalCommand> envelope
    ) {
        Objects.requireNonNull(envelope, "envelope must not be null");
        String corrKey = envelope.command().correlationKey();
        if (corrKey == null || corrKey.isBlank()) {
            throw new IllegalArgumentException("SignalCommand '" + envelope.command().getClass().getSimpleName() + "' must declare a non-blank correlationKey() when dispatched without machineId");
        }
        return sendSignalByCorrelationKey(corrKey, envelope.command().signalName(), envelope);
    }

    /**
     * Dispatches an idempotent {@link CommandEnvelope} directly to a specific workflow machine UUID.
     *
     * @param machineId The workflow machine ID
     * @param envelope  The command envelope containing deduplication commandId and payload
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull UUID machineId,
            @NonNull CommandEnvelope<? extends SignalCommand> envelope
    ) {
        Objects.requireNonNull(envelope, "envelope must not be null");
        return sendSignal(machineId, envelope.command().signalName(), envelope);
    }

    @NonNull
    private CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> resumeWithCheckpoint(
            @NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        // Idempotency check: if duplicate CommandEnvelope was already processed, return current result
        if (signalPayload instanceof CommandEnvelope<?> envelope && checkpoint.processedCommandIds().contains(envelope.commandId())) {
            log.info("Ignoring duplicate command [{}] for machine [{}] in status [{}]",
                    envelope.commandId(), checkpoint.machineId(), checkpoint.status());
            OUTPUT out = null;
            try {
                var outputFn = configuration.outputFunctionTrigger(null).orElse(null);
                out = (outputFn != null) ? outputFn.apply(checkpoint.contextSnapshot()) : null;
            } catch (Exception ignored) {
                // Ignore failure during duplicate output extraction
            }
            return CompletableFuture.completedFuture(
                    new OrchestrationTurnResult<>(checkpoint.machineId(), checkpoint.status(), checkpoint.currentStateKey(), checkpoint.expectedSignal(), checkpoint.contextSnapshot(), out, null)
            );
        }

        if (checkpoint.status() != OrchestrationStatus.SUSPENDED) {
            throw new IllegalStateException(String.format(
                    "Cannot deliver signal '%s' to machine '%s' in non-suspended status [%s]",
                    signalName, checkpoint.machineId(), checkpoint.status()
            ));
        }

        if (checkpoint.expectedSignal() != null && !checkpoint.expectedSignal().equals(signalName)) {
            throw new IllegalArgumentException(String.format(
                    "Signal mismatch for machine '%s': expected '%s', received '%s'",
                    checkpoint.machineId(), checkpoint.expectedSignal(), signalName
            ));
        }

        log.info("Delivering signal [{}] to suspended orchestration [{}] (State: {}) on fresh virtual thread...",
                signalName, checkpoint.machineId(), checkpoint.currentStateKey());

        CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> future = new CompletableFuture<>();

        virtualThreadExecutor.submit(() -> {
            try {
                OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> result =
                        OrchestrationStepDriver.resumeTurn(configuration, checkpoint, signalPayload, virtualThreadExecutor);
                future.complete(result);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });

        return future;
    }
}
