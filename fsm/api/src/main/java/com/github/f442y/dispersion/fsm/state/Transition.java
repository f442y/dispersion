package com.github.f442y.dispersion.fsm.state;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Functional interface responsible for evaluating the next target state key within a state machine
 * based on the state context resulting from the current state's {@link Action}.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The state identifier key type
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
     * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY> The state identifier type
     * @param nextState   The deterministic target state key
     * @return A {@link Transition} always resolving to {@code nextState}
     */
    @NonNull
    static <CONTEXT extends StateMachineContext, STATE_KEY extends StateKey> Transition<CONTEXT, STATE_KEY> to(@NonNull STATE_KEY nextState) {
        Objects.requireNonNull(nextState, "nextState must not be null");
        return _ -> nextState;
    }

    /**
     * Creates a conditional binary branching transition based on a predicate over the context.
     *
     * @param <CONTEXT>   The context type
     * @param <STATE_KEY> The state key type
     * @param condition   Predicate evaluated against the context
     * @param trueTarget  State to transition to if condition is true
     * @param falseTarget State to transition to if condition is false
     * @return A conditional {@link Transition}
     */
    @NonNull
    static <CONTEXT extends StateMachineContext, STATE_KEY extends StateKey> Transition<CONTEXT, STATE_KEY> branch(
            @NonNull Predicate<CONTEXT> condition,
            @NonNull STATE_KEY trueTarget,
            @NonNull STATE_KEY falseTarget
    ) {
        Objects.requireNonNull(condition, "condition must not be null");
        Objects.requireNonNull(trueTarget, "trueTarget must not be null");
        Objects.requireNonNull(falseTarget, "falseTarget must not be null");
        return context -> condition.test(context) ? trueTarget : falseTarget;
    }

    /**
     * Creates a terminal transition that returns {@code null}, indicating completion of the state machine.
     *
     * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY> The state identifier type
     * @return A terminal {@link Transition} instance
     */
    @NonNull
    @SuppressWarnings("unchecked")
    static <CONTEXT extends StateMachineContext, STATE_KEY extends StateKey> Transition<CONTEXT, STATE_KEY> terminal() {
        return (Transition<CONTEXT, STATE_KEY>) TerminalTransition.INSTANCE;
    }

    /**
     * Singleton terminal transition implementation.
     */
    enum TerminalTransition implements Transition<StateMachineContext, StateKey> {
        INSTANCE;

        @Override
        public @Nullable StateKey nextState(@NonNull StateMachineContext ignored) {
            return null;
        }
    }
}
