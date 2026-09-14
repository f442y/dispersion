package com.github.f442y.dispersion.orchestration.messaging;

import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous contract for orchestration executors capable of receiving external signals and command envelopes.
 */
public interface SignalDispatcher {

    /**
     * Routes an idempotent {@link CommandEnvelope} ensuring deduplication against network retries.
     *
     * @param envelope The command envelope
     * @return A CompletableFuture with the turn result
     */
    @NonNull
    CompletableFuture<? extends OrchestrationTurnResult<?, ?, ?>> handleCommand(
            @NonNull CommandEnvelope<?> envelope
    );

    /**
     * Delivers an external signal to a suspended orchestration by its machine UUID.
     *
     * @param machineId The machine UUID
     * @param signalName The signal name
     * @param signalPayload Optional signal payload
     * @return A CompletableFuture with the turn result
     */
    @NonNull
    CompletableFuture<? extends OrchestrationTurnResult<?, ?, ?>> sendSignal(
            @NonNull UUID machineId,
            @NonNull String signalName,
            @Nullable Object signalPayload
    );

    /**
     * Delivers an external signal to a suspended orchestration by its domain correlation key.
     *
     * @param correlationKey The correlation key
     * @param signalName The signal name
     * @param signalPayload Optional signal payload
     * @return A CompletableFuture with the turn result
     */
    @NonNull
    CompletableFuture<? extends OrchestrationTurnResult<?, ?, ?>> sendSignalByCorrelationKey(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    );
}
