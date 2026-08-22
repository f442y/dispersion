package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Immutable durable snapshot of an Orchestration State Machine at a turn boundary.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 */
public record OrchestrationCheckpoint<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey>(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull OrchestrationStatus status,
        @Nullable STATE_KEY currentStateKey,
        @NonNull List<STATE_KEY> completedStates,
        @NonNull CONTEXT contextSnapshot,
        @Nullable String expectedSignal,
        @Nullable String correlationKey,
        @NonNull Set<UUID> processedCommandIds,
        @Nullable Throwable lastError,
        @NonNull Instant timestamp
) {

    public OrchestrationCheckpoint {
        completedStates = (completedStates != null) ? List.copyOf(completedStates) : Collections.emptyList();
        processedCommandIds = (processedCommandIds != null) ? Set.copyOf(processedCommandIds) : Collections.emptySet();
        timestamp = (timestamp != null) ? timestamp : Instant.now();
    }
}
