package com.github.f442y.dispersion.state;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

/**
 * Functional interface representing an isolated unit of business execution logic within a state machine state.
 * <p>
 * Actions operate directly on a thread-confined {@link StateMachineContext}. Since each state machine
 * instance executes on its own dedicated virtual thread, actions do not require thread synchronization
 * or internal locking to mutate or transform state context.
 *
 * @param <CONTEXT> The concrete type of {@link StateMachineContext} managed by this state machine
 */
@FunctionalInterface
public interface Action<CONTEXT extends StateMachineContext> {

    /**
     * Executes business logic against the provided state machine context and returns the
     * mutated or updated context for subsequent transitions and states.
     *
     * @param context The current thread-confined state machine context instance
     * @return The updated or mutated state machine context instance
     * @throws Exception If an unhandled business error or system fault occurs during action execution
     */
    @NonNull
    CONTEXT execute(@NonNull CONTEXT context) throws Exception;

    /**
     * Creates a no-op identity action that returns the incoming state machine context unchanged.
     *
     * @param <CONTEXT_TYPE> The concrete type of {@link StateMachineContext}
     * @return An identity {@link Action} instance
     */
    @NonNull
    static <CONTEXT_TYPE extends StateMachineContext> Action<CONTEXT_TYPE> identity() {
        return context -> context;
    }
}
