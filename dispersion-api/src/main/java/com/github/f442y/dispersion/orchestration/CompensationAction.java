package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

/**
 * Functional contract for executing compensating rollback logic in a Saga pattern.
 * <p>
 * If a downstream state in an orchestration state machine fails permanently, previously completed states
 * have their compensation actions executed in reverse chronological order (LIFO).
 *
 * @param <ORCHESTRATION_CONTEXT> The encompassing orchestration state machine context type
 */
@FunctionalInterface
public interface CompensationAction<ORCHESTRATION_CONTEXT extends StateMachineContext> {

    /**
     * Executes the rollback / compensating transaction logic to undo the effects of a completed state.
     *
     * @param orchestrationContext The current orchestration context
     * @return The updated orchestration context following compensation
     * @throws Exception If compensation fails
     */
    @NonNull
    ORCHESTRATION_CONTEXT compensate(@NonNull ORCHESTRATION_CONTEXT orchestrationContext) throws Exception;

    /**
     * Returns a no-op compensation action that leaves the context unchanged.
     *
     * @param <CONTEXT_TYPE> The orchestration context type
     * @return A no-op {@link CompensationAction}
     */
    @NonNull
    static <CONTEXT_TYPE extends StateMachineContext> CompensationAction<CONTEXT_TYPE> noop() {
        return ctx -> ctx;
    }
}
