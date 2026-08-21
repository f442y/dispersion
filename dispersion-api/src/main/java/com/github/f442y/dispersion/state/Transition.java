package com.github.f442y.dispersion.state;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Functional interface responsible for evaluating the next target state key within a state machine
 * based on the state context resulting from the current state's {@link Action}.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 */
@FunctionalInterface
public interface Transition<CONTEXT extends StateMachineContext, STATE_KEY extends StateKey> {

    /**
     * Determines the next target state identifier by evaluating the given state machine context.
     *
     * @param context The state machine context produced by the state's action
     * @return The next {@link StateKey} to transition to, or {@code null} to terminate the state machine
     * @throws Exception If an error occurs while computing or resolving the target state
     */
    @Nullable
    STATE_KEY nextState(@NonNull CONTEXT context) throws Exception;

    /**
     * Creates an unconditional transition that always resolves to the specified target state key.
     *
     * @param <CONTEXT_TYPE>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY_TYPE> The enum type representing state identifiers
     * @param nextState        The deterministic target state key
     * @return A {@link Transition} always resolving to {@code nextState}
     */
    @NonNull
    static <CONTEXT_TYPE extends StateMachineContext, STATE_KEY_TYPE extends StateKey>
    Transition<CONTEXT_TYPE, STATE_KEY_TYPE> to(@NonNull STATE_KEY_TYPE nextState) {
        return context -> nextState;
    }

    /**
     * Creates a terminal transition that returns {@code null}, indicating completion of the state machine.
     *
     * @param <CONTEXT_TYPE>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY_TYPE> The enum type representing state identifiers
     * @return A terminal {@link Transition} instance
     */
    @NonNull
    @SuppressWarnings("unchecked")
    static <CONTEXT_TYPE extends StateMachineContext, STATE_KEY_TYPE extends StateKey>
    Transition<CONTEXT_TYPE, STATE_KEY_TYPE> terminal() {
        return (Transition<CONTEXT_TYPE, STATE_KEY_TYPE>) TerminalTransition.INSTANCE;
    }

    /**
     * Singleton terminal transition implementation.
     */
    class TerminalTransition implements Transition<StateMachineContext, StateKey> {
        public static final TerminalTransition INSTANCE = new TerminalTransition();

        private TerminalTransition() {}

        @Override
        public @Nullable StateKey nextState(@NonNull StateMachineContext context) {
            return null;
        }
    }
}
