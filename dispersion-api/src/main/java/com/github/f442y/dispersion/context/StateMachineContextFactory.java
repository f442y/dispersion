package com.github.f442y.dispersion.context;

import org.jspecify.annotations.NonNull;

/**
 * Factory contract responsible for instantiating clean, isolated {@link StateMachineContext} instances
 * for each state machine execution.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext} produced by this factory
 * @author Faizaan Ahmed
 * @see StateMachineContext
 */
@FunctionalInterface
public interface StateMachineContextFactory<CONTEXT extends StateMachineContext> {

    /**
     * Instantiates a fresh, isolated state machine context instance.
     *
     * @return A new {@link StateMachineContext} instance
     */
    @NonNull
    CONTEXT newInstance();
}
