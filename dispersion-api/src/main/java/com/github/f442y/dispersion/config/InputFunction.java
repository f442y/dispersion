package com.github.f442y.dispersion.config;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.StateMachineException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Functional interface responsible for populating or mutating a state machine context
 * with an incoming input payload before execution begins.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <INPUT>   The type of input payload accepted by the state machine
 */
@FunctionalInterface
public interface InputFunction<CONTEXT extends StateMachineContext, INPUT> {

    /**
     * Applies the incoming input payload to the state machine context.
     *
     * @param context The newly created initial context
     * @param input   The input payload
     * @return The updated context
     * @throws StateMachineException If input validation or mapping fails
     */
    @NonNull
    CONTEXT apply(@NonNull CONTEXT context, @Nullable INPUT input) throws StateMachineException;
}
