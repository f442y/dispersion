package com.github.f442y.dispersion.orchestration.batch;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.OrchestrationStatus;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Result model representing the outcome of a batch execution turn across item contexts.
 */
public record BatchTurnResult<
        BATCH_CONTEXT extends StateMachineContext,
        ITEM_CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        OUTPUT>(
        @NonNull UUID batchId,
        @NonNull String batchKey,
        @NonNull OrchestrationStatus status,
        @Nullable STATE_KEY currentBatchStateKey,
        @NonNull Map<String, STATE_KEY> itemStates,
        @NonNull Map<String, ITEM_CONTEXT> itemContexts,
        @NonNull BATCH_CONTEXT batchContext,
        @Nullable OUTPUT output,
        @Nullable Throwable error
) {

    public BatchTurnResult {
        Objects.requireNonNull(batchId, "batchId must not be null");
        Objects.requireNonNull(batchKey, "batchKey must not be null");
        Objects.requireNonNull(status, "status must not be null");
        itemStates = (itemStates != null) ? Map.copyOf(itemStates) : Collections.emptyMap();
        itemContexts = (itemContexts != null) ? Map.copyOf(itemContexts) : Collections.emptyMap();
        Objects.requireNonNull(batchContext, "batchContext must not be null");
    }

    public boolean isCompleted() {
        return status == OrchestrationStatus.COMPLETED;
    }

    public boolean isSuspended() {
        return status == OrchestrationStatus.SUSPENDED;
    }

    public boolean isFailed() {
        return status == OrchestrationStatus.COMPENSATED || error != null;
    }
}
