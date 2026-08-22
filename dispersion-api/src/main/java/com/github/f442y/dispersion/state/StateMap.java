package com.github.f442y.dispersion.state;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable topological directed graph representation of a Finite State Machine.
 * <p>
 * Enforces compile-time and build-time graph integrity checks:
 * <ul>
 *   <li>Initial state must be registered.</li>
 *   <li>All declared target states (permitted targets) must be valid registered states.</li>
 *   <li>Terminal end-states cannot have outgoing permitted target transitions.</li>
 *   <li>Fallback states for visit-limited states must be registered in the graph.</li>
 * </ul>
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 */
public final class StateMap<CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey> {

    private final Class<STATE_KEY> stateKeyClass;
    private final Map<STATE_KEY, State<CONTEXT, STATE_KEY>> stateMap;
    private final STATE_KEY initialState;
    private final Set<STATE_KEY> endStates;

    private StateMap(
            @NonNull Class<STATE_KEY> stateKeyClass,
            @NonNull Map<STATE_KEY, State<CONTEXT, STATE_KEY>> stateMap,
            @NonNull STATE_KEY initialState,
            @NonNull Set<STATE_KEY> endStates
    ) {
        this.stateKeyClass = Objects.requireNonNull(stateKeyClass, "stateKeyClass must not be null");
        this.stateMap = Collections.unmodifiableMap(new EnumMap<>(stateMap));
        this.initialState = Objects.requireNonNull(initialState, "initialState must not be null");
        this.endStates = Collections.unmodifiableSet(
                endStates.isEmpty() ? EnumSet.noneOf(stateKeyClass) : EnumSet.copyOf(endStates)
        );
        validateGraphTopology();
    }

    private void validateGraphTopology() {
        if (!stateMap.containsKey(initialState)) {
            throw new IllegalStateException("Initial state [" + initialState + "] is not registered in the state machine");
        }

        for (STATE_KEY endState : endStates) {
            State<CONTEXT, STATE_KEY> endStateNode = stateMap.get(endState);
            if (endStateNode != null && !endStateNode.permittedTargets().isEmpty()) {
                throw new IllegalStateException("Terminal end-state [" + endState + "] cannot declare outgoing permitted transitions");
            }
        }

        for (Map.Entry<STATE_KEY, State<CONTEXT, STATE_KEY>> entry : stateMap.entrySet()) {
            STATE_KEY sourceKey = entry.getKey();
            State<CONTEXT, STATE_KEY> node = entry.getValue();

            for (STATE_KEY targetKey : node.permittedTargets()) {
                if (!stateMap.containsKey(targetKey) && !endStates.contains(targetKey)) {
                    throw new IllegalStateException(
                            "State [" + sourceKey + "] declares transition target [" + targetKey +
                            "] which is not registered in the state machine"
                    );
                }
            }

            if (node.maxVisitsFallback() != null) {
                STATE_KEY fallback = node.maxVisitsFallback();
                if (!stateMap.containsKey(fallback) && !endStates.contains(fallback)) {
                    throw new IllegalStateException(
                            "State [" + sourceKey + "] defines maxVisitsFallback [" + fallback +
                            "] which is not registered in the state machine"
                    );
                }
            }
        }
    }

    /**
     * Retrieves the {@link State} node configuration associated with the given state key.
     *
     * @param stateKey The state identifier to lookup
     * @return The configured {@link State} node
     * @throws NoSuchElementException If the state key is not registered
     */
    @NonNull
    public State<CONTEXT, STATE_KEY> getState(@NonNull STATE_KEY stateKey) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        State<CONTEXT, STATE_KEY> state = stateMap.get(stateKey);
        if (state == null) {
            if (endStates.contains(stateKey)) {
                return State.terminal();
            }
            throw new NoSuchElementException("State [" + stateKey + "] is not registered in this StateMap");
        }
        return state;
    }

    /**
     * Checks whether the state key is registered in this state map.
     *
     * @param stateKey The state identifier
     * @return {@code true} if present; otherwise {@code false}
     */
    public boolean containsState(@NonNull STATE_KEY stateKey) {
        return stateMap.containsKey(stateKey) || endStates.contains(stateKey);
    }

    /**
     * Returns the initial starting state key of this state machine graph.
     *
     * @return The initial {@link StateKey}
     */
    @NonNull
    public STATE_KEY getInitialState() {
        return initialState;
    }

    /**
     * Returns the set of all terminal end states in this state machine graph.
     *
     * @return Immutable set of terminal {@link StateKey} identifiers
     */
    @NonNull
    public Set<STATE_KEY> getEndStates() {
        return endStates;
    }

    /**
     * Returns the enum class of state keys managed by this state map.
     *
     * @return The state key class
     */
    @NonNull
    public Class<STATE_KEY> getStateKeyClass() {
        return stateKeyClass;
    }

    /**
     * Returns an unmodifiable map of all registered states in this graph.
     *
     * @return Map of state keys to their state nodes
     */
    @NonNull
    public Map<STATE_KEY, State<CONTEXT, STATE_KEY>> getAllStates() {
        return stateMap;
    }

    /**
     * Generates a Mermaid stateDiagram-v2 string representing this Finite State Machine topology.
     *
     * @return Mermaid diagram string
     */
    @NonNull
    public String toMermaid() {
        StringBuilder sb = new StringBuilder();
        sb.append("stateDiagram-v2\n");
        sb.append("    [*] --> ").append(initialState.name()).append("\n");

        for (Map.Entry<STATE_KEY, State<CONTEXT, STATE_KEY>> entry : stateMap.entrySet()) {
            STATE_KEY source = entry.getKey();
            State<CONTEXT, STATE_KEY> node = entry.getValue();

            for (STATE_KEY target : node.permittedTargets()) {
                sb.append("    ").append(source.name()).append(" --> ").append(target.name()).append("\n");
            }

            if (node.maxVisitsFallback() != null) {
                sb.append("    ").append(source.name()).append(" --> ").append(node.maxVisitsFallback().name())
                  .append(" : maxVisitsFallback\n");
            }
        }

        for (STATE_KEY endState : endStates) {
            sb.append("    ").append(endState.name()).append(" --> [*]\n");
        }

        return sb.toString();
    }

    /**
     * Creates a new builder for constructing a {@link StateMap}.
     *
     * @param <C>           The context type
     * @param <S>           The state key enum type
     * @param stateKeyClass The state key enum class
     * @return A new {@link Builder} instance
     */
    @NonNull
    public static <C extends StateMachineContext, S extends Enum<S> & StateKey> Builder<C, S> builder(
            @NonNull Class<S> stateKeyClass
    ) {
        return new Builder<>(stateKeyClass);
    }

    /**
     * Builder for constructing validated immutable {@link StateMap} instances.
     */
    public static final class Builder<CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey> {
        private final Class<STATE_KEY> stateKeyClass;
        private final Map<STATE_KEY, State<CONTEXT, STATE_KEY>> states;
        private STATE_KEY initialState;
        private final Set<STATE_KEY> endStates;

        private Builder(@NonNull Class<STATE_KEY> stateKeyClass) {
            this.stateKeyClass = Objects.requireNonNull(stateKeyClass, "stateKeyClass must not be null");
            this.states = new EnumMap<>(stateKeyClass);
            this.endStates = EnumSet.noneOf(stateKeyClass);
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> addState(
                @NonNull STATE_KEY stateKey,
                @NonNull State<CONTEXT, STATE_KEY> state
        ) {
            Objects.requireNonNull(stateKey, "stateKey must not be null");
            Objects.requireNonNull(state, "state must not be null");
            this.states.put(stateKey, state);
            return this;
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> initialState(@NonNull STATE_KEY initialState) {
            this.initialState = Objects.requireNonNull(initialState, "initialState must not be null");
            return this;
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> endState(@NonNull STATE_KEY endState) {
            Objects.requireNonNull(endState, "endState must not be null");
            this.endStates.add(endState);
            return this;
        }

        @NonNull
        @SafeVarargs
        public final Builder<CONTEXT, STATE_KEY> endStates(@NonNull STATE_KEY... endStates) {
            for (STATE_KEY s : endStates) {
                endState(s);
            }
            return this;
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> endStates(@NonNull Set<STATE_KEY> endStates) {
            Objects.requireNonNull(endStates, "endStates must not be null");
            this.endStates.addAll(endStates);
            return this;
        }

        @NonNull
        public StateMap<CONTEXT, STATE_KEY> build() {
            if (initialState == null) {
                throw new IllegalStateException("Initial state must be configured on StateMap");
            }
            // If end states were registered without an explicit state node, register terminal nodes for them
            for (STATE_KEY endState : endStates) {
                states.putIfAbsent(endState, State.terminal());
            }
            return new StateMap<>(stateKeyClass, states, initialState, endStates);
        }
    }
}
