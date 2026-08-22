package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Functional interface responsible for reconstructing or sanitizing clean input data
 * when retrying a child state machine or action on a fresh Virtual Thread after a failure.
 *
 * @param <PARENT_CONTEXT> The parent orchestration context type
 * @param <CHILD_INPUT>    The input type required by the child execution
 */
@FunctionalInterface
public interface ContextRecoverer<PARENT_CONTEXT extends StateMachineContext, CHILD_INPUT> {

    /**
     * Produces a fresh input payload for a retry attempt.
     *
     * @param parentContext The current parent orchestration context
     * @param error         The exception thrown in the previous failed attempt
     * @param attemptNumber The current retry attempt index (1-based)
     * @return Fresh, clean child input payload
     * @throws Exception If input recovery fails
     */
    @Nullable
    CHILD_INPUT recover(
            @NonNull PARENT_CONTEXT parentContext,
            @NonNull Throwable error,
            int attemptNumber
    ) throws Exception;
}
