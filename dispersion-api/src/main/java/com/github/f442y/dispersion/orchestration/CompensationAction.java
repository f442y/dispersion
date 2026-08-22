package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

/**
 * Functional interface defining a compensating undo action executed during automated LIFO Saga rollback.
 *
 * @param <CONTEXT> The context type
 */
@FunctionalInterface
public interface CompensationAction<CONTEXT extends StateMachineContext> {

    /**
     * Executes compensating undo logic to reverse the effects of a previously completed state.
     *
     * @param context The current state machine context
     * @return The updated context following compensation
     * @throws Exception If compensation fails
     */
    @NonNull
    CONTEXT compensate(@NonNull CONTEXT context) throws Exception;

    /**
     * Returns a no-op compensation action that returns the context unchanged.
     *
     * @param <C> The context type
     * @return A no-op {@link CompensationAction}
     */
    @NonNull
    static <C extends StateMachineContext> CompensationAction<C> noop() {
        return context -> context;
    }
}
