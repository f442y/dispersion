package com.github.f442y.dispersion.context;

import org.jspecify.annotations.NonNull;

import java.util.function.Supplier;

/**
 * Factory contract responsible for instantiating fresh, clean {@link StateMachineContext} instances
 * for state machine executions. Extends {@link Supplier} for seamless compatibility with method references.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext}
 */
@FunctionalInterface
public interface StateMachineContextFactory<CONTEXT extends StateMachineContext> extends Supplier<CONTEXT> {

    /**
     * Creates and returns a new {@link StateMachineContext} instance.
     *
     * @return A newly initialized context instance
     */
    @NonNull
    CONTEXT newInstance();

    @Override
    @NonNull
    default CONTEXT get() {
        return newInstance();
    }
}
