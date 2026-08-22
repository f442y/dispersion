package com.github.f442y.dispersion.state;

import com.github.f442y.dispersion.context.StateMachineContext;
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
     * @param <C>       The concrete type of {@link StateMachineContext}
     * @param <S>       The state identifier type
     * @param nextState The deterministic target state key
     * @return A {@link Transition} always resolving to {@code nextState}
     */
    @NonNull
    static <C extends StateMachineContext, S extends StateKey> Transition<C, S> to(@NonNull S nextState) {
        Objects.requireNonNull(nextState, "nextState must not be null");
        return context -> nextState;
    }

    /**
     * Creates a conditional binary branching transition based on a predicate over the context.
     *
     * @param <C>         The context type
     * @param <S>         The state key type
     * @param condition   Predicate evaluated against the context
     * @param trueTarget  State to transition to if condition is true
     * @param falseTarget State to transition to if condition is false
     * @return A conditional {@link Transition}
     */
    @NonNull
    static <C extends StateMachineContext, S extends StateKey> Transition<C, S> branch(
            @NonNull Predicate<C> condition,
            @NonNull S trueTarget,
            @NonNull S falseTarget
    ) {
        Objects.requireNonNull(condition, "condition must not be null");
        Objects.requireNonNull(trueTarget, "trueTarget must not be null");
        Objects.requireNonNull(falseTarget, "falseTarget must not be null");
        return context -> condition.test(context) ? trueTarget : falseTarget;
    }

    /**
     * Creates a terminal transition that returns {@code null}, indicating completion of the state machine.
     *
     * @param <C> The concrete type of {@link StateMachineContext}
     * @param <S> The state identifier type
     * @return A terminal {@link Transition} instance
     */
    @NonNull
    @SuppressWarnings("unchecked")
    static <C extends StateMachineContext, S extends StateKey> Transition<C, S> terminal() {
        return (Transition<C, S>) TerminalTransition.INSTANCE;
    }

    /**
     * Singleton terminal transition implementation.
     */
    enum TerminalTransition implements Transition<StateMachineContext, StateKey> {
        INSTANCE;

        @Override
        public @Nullable StateKey nextState(@NonNull StateMachineContext context) {
            return null;
        }
    }
}
