package com.github.f442y.dispersion.orchestration.batch;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.OrchestrationStatus;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Checkpoint snapshot capturing the batch orchestration state, item-level states, and barrier arrivals.
 */
public record BatchOrchestrationCheckpoint<
        BATCH_CONTEXT extends StateMachineContext,
        ITEM_CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey>(
        @NonNull UUID batchId,
        @NonNull String batchName,
        @NonNull String batchKey,
        @NonNull OrchestrationStatus status,
        @Nullable STATE_KEY currentBatchStateKey,
        @NonNull Map<String, STATE_KEY> itemStates,
        @NonNull Map<String, ITEM_CONTEXT> itemContexts,
        @NonNull BATCH_CONTEXT batchContext,
        @NonNull Set<String> arrivedBarrierItemKeys,
        @NonNull Set<UUID> processedCommandIds,
        @Nullable Throwable lastError,
        @NonNull Instant timestamp
) {

    public BatchOrchestrationCheckpoint {
        Objects.requireNonNull(batchId, "batchId must not be null");
        Objects.requireNonNull(batchName, "batchName must not be null");
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        Objects.requireNonNull(status, "status must not be null");
        itemStates = (itemStates != null) ? Map.copyOf(itemStates) : Collections.emptyMap();
        itemContexts = (itemContexts != null) ? Map.copyOf(itemContexts) : Collections.emptyMap();
        Objects.requireNonNull(batchContext, "batchContext must not be null");
        arrivedBarrierItemKeys = (arrivedBarrierItemKeys != null) ? Set.copyOf(arrivedBarrierItemKeys) : Collections.emptySet();
        processedCommandIds = (processedCommandIds != null) ? Set.copyOf(processedCommandIds) : Collections.emptySet();
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
