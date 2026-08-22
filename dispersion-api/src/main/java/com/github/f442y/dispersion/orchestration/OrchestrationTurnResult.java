package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates the outcome of executing an orchestration turn.
 *
 * @param <CONTEXT>   The orchestration context type
 * @param <STATE_KEY> The orchestration state key enum type
 * @param <OUTPUT>    The output result type
 */
public record OrchestrationTurnResult<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        OUTPUT>(
        @NonNull UUID machineId,
        @NonNull OrchestrationStatus status,
        @Nullable STATE_KEY currentStateKey,
        @Nullable String expectedSignal,
        @Nullable CONTEXT context,
        @Nullable OUTPUT output,
        @Nullable Throwable error
) {

    public OrchestrationTurnResult {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(status, "status must not be null");
    }

    /**
     * Checks whether the orchestration finished in a terminal end-state.
     *
     * @return {@code true} if completed; otherwise {@code false}
     */
    public boolean isCompleted() {
        return status == OrchestrationStatus.COMPLETED;
    }

    /**
     * Checks whether the orchestration suspended at a signal wait state.
     *
     * @return {@code true} if suspended; otherwise {@code false}
     */
    public boolean isSuspended() {
        return status == OrchestrationStatus.SUSPENDED;
    }

    /**
     * Checks whether the orchestration failed or was compensated.
     *
     * @return {@code true} if failed or compensated; otherwise {@code false}
     */
    public boolean isFailed() {
        return status == OrchestrationStatus.FAILED || status == OrchestrationStatus.COMPENSATED;
    }

    public static <C extends StateMachineContext, S extends Enum<S> & StateKey, O>
    OrchestrationTurnResult<C, S, O> completed(@NonNull UUID machineId, @NonNull C context, @Nullable O output) {
        return new OrchestrationTurnResult<>(machineId, OrchestrationStatus.COMPLETED, null, null, context, output, null);
    }

    public static <C extends StateMachineContext, S extends Enum<S> & StateKey, O>
    OrchestrationTurnResult<C, S, O> suspended(@NonNull UUID machineId, @NonNull S stateKey, @NonNull String expectedSignal, @NonNull C context) {
        return new OrchestrationTurnResult<>(machineId, OrchestrationStatus.SUSPENDED, stateKey, expectedSignal, context, null, null);
    }

    public static <C extends StateMachineContext, S extends Enum<S> & StateKey, O>
    OrchestrationTurnResult<C, S, O> failed(@NonNull UUID machineId, @NonNull OrchestrationStatus status, @Nullable S stateKey, @Nullable C context, @NonNull Throwable error) {
        return new OrchestrationTurnResult<>(machineId, status, stateKey, null, context, null, error);
    }
}
