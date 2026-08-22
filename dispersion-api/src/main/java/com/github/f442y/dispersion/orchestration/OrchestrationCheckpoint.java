package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Immutable snapshot capturing the progress, execution status, completed state history,
 * expected signal, correlation key, processed command IDs, and current context of an Orchestration State Machine.
 *
 * @param <ORCHESTRATION_CONTEXT>   The orchestration state machine context type
 * @param <ORCHESTRATION_STATE_KEY> The orchestration state key enum type
 */
public record OrchestrationCheckpoint<
        ORCHESTRATION_CONTEXT extends StateMachineContext,
        ORCHESTRATION_STATE_KEY extends Enum<ORCHESTRATION_STATE_KEY> & StateKey>(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull OrchestrationStatus status,
        @Nullable ORCHESTRATION_STATE_KEY currentStateKey,
        @NonNull List<ORCHESTRATION_STATE_KEY> completedStates,
        @NonNull ORCHESTRATION_CONTEXT contextSnapshot,
        @Nullable String expectedSignal,
        @Nullable String correlationKey,
        @NonNull Set<UUID> processedCommandIds,
        @Nullable Throwable lastError,
        @NonNull Instant timestamp
) {

    public OrchestrationCheckpoint {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(completedStates, "completedStates must not be null");
        Objects.requireNonNull(contextSnapshot, "contextSnapshot must not be null");
        processedCommandIds = (processedCommandIds != null) ? Set.copyOf(processedCommandIds) : Collections.emptySet();
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }

    /**
     * Convenience constructor without processedCommandIds.
     */
    public OrchestrationCheckpoint(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull OrchestrationStatus status,
            @Nullable ORCHESTRATION_STATE_KEY currentStateKey,
            @NonNull List<ORCHESTRATION_STATE_KEY> completedStates,
            @NonNull ORCHESTRATION_CONTEXT contextSnapshot,
            @Nullable String expectedSignal,
            @Nullable String correlationKey,
            @Nullable Throwable lastError,
            @NonNull Instant timestamp
    ) {
        this(machineId, machineName, status, currentStateKey, completedStates, contextSnapshot, expectedSignal, correlationKey, Collections.emptySet(), lastError, timestamp);
    }

    /**
     * Backward-compatible convenience constructor without expectedSignal and correlationKey.
     */
    public OrchestrationCheckpoint(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull OrchestrationStatus status,
            @Nullable ORCHESTRATION_STATE_KEY currentStateKey,
            @NonNull List<ORCHESTRATION_STATE_KEY> completedStates,
            @NonNull ORCHESTRATION_CONTEXT contextSnapshot,
            @Nullable Throwable lastError,
            @NonNull Instant timestamp
    ) {
        this(machineId, machineName, status, currentStateKey, completedStates, contextSnapshot, null, null, Collections.emptySet(), lastError, timestamp);
    }
}
