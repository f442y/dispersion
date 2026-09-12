package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.UUID;

/**
 * Immutable record capturing the outcome of an orchestration turn execution.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 * @param <OUTPUT>    The output type
 */
public record OrchestrationTurnResult<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        OUTPUT>(
        @NonNull UUID machineId,
        @NonNull OrchestrationStatus status,
        @Nullable STATE_KEY currentStateKey,
        @Nullable String expectedSignal,
        @NonNull CONTEXT context,
        @Nullable OUTPUT output,
        @Nullable Throwable error
) {

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
