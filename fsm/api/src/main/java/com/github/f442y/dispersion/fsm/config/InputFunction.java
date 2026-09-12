package com.github.f442y.dispersion.fsm.config;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.exception.StateMachineException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Functional interface responsible for populating a newly instantiated {@link StateMachineContext}
 * with an external input payload before graph execution begins.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext}
 * @param <INPUT>   The input payload type
 */
@FunctionalInterface
public interface InputFunction<CONTEXT extends StateMachineContext, INPUT> {

    /**
     * Ingests the given input object into the provided state machine context.
     *
     * @param context The newly created, clean state machine context
     * @param input   The incoming external input payload (may be {@code null})
     * @return The populated context instance ready for initial state execution
     * @throws StateMachineException If input validation or mapping fails
     */
    @NonNull
    CONTEXT apply(@NonNull CONTEXT context, @Nullable INPUT input) throws StateMachineException;

    /**
     * Creates an identity input function that returns the context unchanged regardless of input.
     *
     * @param <CONTEXT> The context type
     * @param <INPUT>   The input type
     * @return An identity {@link InputFunction}
     */
    @NonNull
    static <CONTEXT extends StateMachineContext, INPUT> InputFunction<CONTEXT, INPUT> identity() {
        return (context, _) -> context;
    }
}
