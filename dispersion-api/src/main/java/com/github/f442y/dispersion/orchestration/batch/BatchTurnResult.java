package com.github.f442y.dispersion.orchestration.batch;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.OrchestrationStatus;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable record capturing the outcome of a batch orchestration turn execution.
 *
 * @param <BATCH_CONTEXT> The batch-level context type
 * @param <ITEM_CONTEXT>  The item-level context type
 * @param <STATE_KEY>     The state key enum type
 * @param <OUTPUT>        The output type
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
        Objects.requireNonNull(batchContext, "batchContext must not be null");
        itemStates = Map.copyOf(Objects.requireNonNull(itemStates, "itemStates must not be null"));
        itemContexts = Map.copyOf(Objects.requireNonNull(itemContexts, "itemContexts must not be null"));
    }

    public boolean isCompleted() {
        return status == OrchestrationStatus.COMPLETED;
    }

    public boolean isSuspended() {
        return status == OrchestrationStatus.SUSPENDED;
    }

    public boolean isFailed() {
        return status == OrchestrationStatus.FAILED;
    }

    public boolean isCompensated() {
        return status == OrchestrationStatus.COMPENSATED;
    }
}
