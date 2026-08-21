package com.github.f442y.dispersion.config;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.StateMachineException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Functional interface responsible for extracting the final output result payload
 * from the completed state machine context.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <OUTPUT>  The type of output result produced upon completion
 */
@FunctionalInterface
public interface OutputFunction<CONTEXT extends StateMachineContext, OUTPUT> {

    /**
     * Extracts the output result payload from the final state machine context.
     *
     * @param context The completed state machine context
     * @return The extracted result
     * @throws StateMachineException If output mapping or transformation fails
     */
    @Nullable
    OUTPUT apply(@NonNull CONTEXT context) throws StateMachineException;
}
