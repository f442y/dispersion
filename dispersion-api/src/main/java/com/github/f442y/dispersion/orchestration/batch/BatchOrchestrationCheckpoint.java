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
 * Immutable durable snapshot of a Set/Batch Orchestration across all managed item contexts.
 *
 * @param <BATCH_CONTEXT> The batch-level context type
 * @param <ITEM_CONTEXT>  The item-level context type
 * @param <STATE_KEY>     The state key enum type
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
        timestamp = (timestamp != null) ? timestamp : Instant.now();
    }
}
