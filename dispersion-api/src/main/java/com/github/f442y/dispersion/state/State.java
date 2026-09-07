package com.github.f442y.dispersion.state;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a single node contract within a Finite State Machine graph, encapsulating an {@link Action}
 * (business execution logic), a {@link Transition} (routing decision logic), an explicit set
 * of permitted target states (outgoing graph edges), and optional loop / retry safeguards.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 */
public interface State<CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey> {

    /**
     * Returns the business logic action associated with this state.
     *
     * @return The {@link Action} instance
     */
    @NonNull
    Action<CONTEXT> action();

    /**
     * Returns the transition decision logic associated with this state.
     *
     * @return The {@link Transition} instance
     */
    @NonNull
    Transition<CONTEXT, STATE_KEY> transition();

    /**
     * Returns the unmodifiable set of all permitted target states (outgoing graph edges) from this state.
     *
     * @return Immutable set of permitted next {@link StateKey} targets
     */
    @NonNull
    Set<STATE_KEY> permittedTargets();

    /**
     * Returns true if this state is configured as an explicit terminal end-state.
     *
     * @return {@code true} if this is a terminal state; otherwise {@code false}
     */
    boolean isTerminal();

    /**
     * Returns the maximum allowed visit count for this state in a single state machine execution.
     *
     * @return The maximum visits, or a non-positive integer if unlimited
     */
    int maxVisits();

    /**
     * Returns the optional fallback state key to divert to when the maximum visit limit is reached.
     *
     * @return The fallback {@link StateKey}, or {@code null} if an exception should be thrown instead
     */
    @Nullable
    STATE_KEY maxVisitsFallback();

    /**
     * Creates an intermediate state pairing the given action with explicit permitted targets and a dynamic transition.
     *
     * @param <C>              The context type
     * @param <S>              The state key type
     * @param action           The action business logic
     * @param permittedTargets The explicit set of valid next states
     * @param transition       The transition routing logic
     * @return A new {@link State} instance
     */
    @NonNull
    static <C extends StateMachineContext, S extends Enum<S> & StateKey> State<C, S> of(
            @NonNull Action<C> action,
            @NonNull Set<S> permittedTargets,
            @NonNull Transition<C, S> transition
    ) {
        return new SimpleState<>(action, permittedTargets, transition, false, -1, null);
    }

    /**
     * Creates an intermediate state pairing the given action with explicit permitted targets, transition, and visit limits.
     *
     * @param <C>                 The context type
     * @param <S>                 The state key type
     * @param action              The action business logic
     * @param permittedTargets    The explicit set of valid next states
     * @param transition          The transition routing logic
     * @param maxVisits           Maximum allowed visits
     * @param maxVisitsFallback   Optional fallback state key
     * @return A new {@link State} instance
     */
    @NonNull
    static <C extends StateMachineContext, S extends Enum<S> & StateKey> State<C, S> of(
            @NonNull Action<C> action,
            @NonNull Set<S> permittedTargets,
            @NonNull Transition<C, S> transition,
            int maxVisits,
            @Nullable S maxVisitsFallback
    ) {
        return new SimpleState<>(action, permittedTargets, transition, false, maxVisits, maxVisitsFallback);
    }

    /**
     * Creates an intermediate state pairing the given action with an unconditional next state.
     *
     * @param <C>       The context type
     * @param <S>       The state key type
     * @param action    The action business logic
     * @param nextState The deterministic next state key
     * @return A new {@link State} instance
     */
    @NonNull
    static <C extends StateMachineContext, S extends Enum<S> & StateKey> State<C, S> of(
            @NonNull Action<C> action,
            @NonNull S nextState
    ) {
        Objects.requireNonNull(nextState, "nextState must not be null");
        return new SimpleState<>(action, Set.of(nextState), Transition.to(nextState), false, -1, null);
    }

    /**
     * Creates a terminal end state that performs no action and signals the end of the state machine.
     *
     * @param <C> The context type
     * @param <S> The state key type
     * @return A terminal {@link State} instance
     */
    @NonNull
    @SuppressWarnings("unchecked")
    static <C extends StateMachineContext, S extends Enum<S> & StateKey> State<C, S> terminal() {
        return (State<C, S>) TerminalState.INSTANCE;
    }

    /**
     * Singleton terminal state implementation.
     */
    enum TerminalState implements State<StateMachineContext, StateKeyEnum> {
        INSTANCE;

        @NonNull
        @Override
        public Action<StateMachineContext> action() {
            return Action.identity();
        }

        @NonNull
        @Override
        public Transition<StateMachineContext, StateKeyEnum> transition() {
            return Transition.terminal();
        }

        @NonNull
        @Override
        public Set<StateKeyEnum> permittedTargets() {
            return Collections.emptySet();
        }

        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public int maxVisits() {
            return -1;
        }

        @Nullable
        @Override
        public StateKeyEnum maxVisitsFallback() {
            return null;
        }
    }

    /**
     * Private enum marker for raw TerminalState generic bound.
     */
    enum StateKeyEnum implements StateKey {}

    /**
     * Default immutable record implementation of {@link State}.
     */
    record SimpleState<C extends StateMachineContext, S extends Enum<S> & StateKey>(
            @NonNull Action<C> action,
            @NonNull Set<S> permittedTargets,
            @NonNull Transition<C, S> transition,
            boolean isTerminal,
            int maxVisits,
            @Nullable S maxVisitsFallback
    ) implements State<C, S> {
        public SimpleState {
            Objects.requireNonNull(action, "action must not be null");
            Objects.requireNonNull(transition, "transition must not be null");
            permittedTargets = Set.copyOf(Objects.requireNonNull(permittedTargets, "permittedTargets must not be null"));
        }
    }
}
