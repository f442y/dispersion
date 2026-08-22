package com.github.f442y.dispersion.config;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.StateMachineException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.function.Function;

/**
 * Functional interface responsible for extracting a domain output result from a completed
 * {@link StateMachineContext} after reaching a terminal state.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext}
 * @param <OUTPUT>  The return result payload type
 */
@FunctionalInterface
public interface OutputFunction<CONTEXT extends StateMachineContext, OUTPUT> extends Function<CONTEXT, OUTPUT> {

    /**
     * Extracts or constructs the output from the final state machine context.
     *
     * @param context The completed state machine context
     * @return The domain output result
     * @throws StateMachineException If output transformation fails
     */
    @Override
    @Nullable
    OUTPUT apply(@NonNull CONTEXT context) throws StateMachineException;

    /**
     * Creates an output function that returns the entire context as the output.
     *
     * @param <C> The context type
     * @return An output function returning the context directly
     */
    @NonNull
    static <C extends StateMachineContext> OutputFunction<C, C> context() {
        return context -> context;
    }
}
