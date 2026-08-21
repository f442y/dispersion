package com.github.f442y.dispersion.state;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable lookup table and directed graph representing all states and their permitted transitions
 * within a Finite State Machine.
 * <p>
 * Validates graph integrity at construction time, ensuring all edges and fallback destinations
 * target existing states and that terminal states have no outgoing transitions.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 */
public final class StateMap<CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey> {

    @NonNull
    private final Map<STATE_KEY, State<CONTEXT, STATE_KEY>> stateEnumMap;

    @NonNull
    private final STATE_KEY initialState;

    /**
     * Constructs an immutable {@link StateMap} from the provided state definitions and initial state key.
     *
     * @param states       The map containing state keys mapped to state definitions
     * @param initialState The entry-point state key for the state machine
     */
    public StateMap(
            @NonNull Map<STATE_KEY, State<CONTEXT, STATE_KEY>> states,
            @NonNull STATE_KEY initialState
    ) {
        Objects.requireNonNull(states, "states map must not be null");
        this.initialState = Objects.requireNonNull(initialState, "initialState must not be null");
        this.stateEnumMap = new EnumMap<>(states);
    }

    /**
     * Returns the initial entry-point {@link State} definition for this state machine.
     *
     * @return The initial {@link State} instance
     */
    @NonNull
    public State<CONTEXT, STATE_KEY> getInitialState() {
        State<CONTEXT, STATE_KEY> state = this.stateEnumMap.get(initialState);
        if (state == null) {
            throw new IllegalStateException("Initial state '" + initialState + "' is not registered in the state map");
        }
        return state;
    }

    /**
     * Returns the designated starting state key.
     *
     * @return The initial state key
     */
    @NonNull
    public STATE_KEY getInitialStateKey() {
        return initialState;
    }

    /**
     * Retrieves the {@link State} definition registered for the given state key.
     *
     * @param stateKey The state identifier to look up
     * @return The registered {@link State} instance, or {@code null} if not found
     */
    @Nullable
    public State<CONTEXT, STATE_KEY> getState(@NonNull STATE_KEY stateKey) {
        return this.stateEnumMap.get(stateKey);
    }

    /**
     * Returns an unmodifiable view of all registered states.
     *
     * @return Unmodifiable map of state keys to their state definitions
     */
    @NonNull
    public Map<STATE_KEY, State<CONTEXT, STATE_KEY>> stateEnumMap() {
        return Collections.unmodifiableMap(stateEnumMap);
    }

    /**
     * Returns the directed adjacency map containing each state's permitted outgoing target states.
     *
     * @return Map of state keys to their set of permitted target state keys
     */
    @NonNull
    public Map<STATE_KEY, Set<STATE_KEY>> adjacencyList() {
        if (stateEnumMap.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<STATE_KEY, Set<STATE_KEY>> adj = new EnumMap<>(initialState.getDeclaringClass());
        for (Map.Entry<STATE_KEY, State<CONTEXT, STATE_KEY>> entry : stateEnumMap.entrySet()) {
            adj.put(entry.getKey(), entry.getValue().permittedTargets());
        }
        return Collections.unmodifiableMap(adj);
    }

    /**
     * Generates a Mermaid diagram representing the complete directed state machine graph topology.
     *
     * @return Mermaid markdown string (e.g. `stateDiagram-v2 ...`)
     */
    @NonNull
    public String toMermaid() {
        StringBuilder sb = new StringBuilder();
        sb.append("stateDiagram-v2\n");
        sb.append("    [*] --> ").append(initialState.name()).append("\n");

        for (Map.Entry<STATE_KEY, State<CONTEXT, STATE_KEY>> entry : stateEnumMap.entrySet()) {
            STATE_KEY source = entry.getKey();
            State<CONTEXT, STATE_KEY> state = entry.getValue();

            if (state.isTerminal()) {
                sb.append("    ").append(source.name()).append(" --> [*]\n");
            } else {
                for (STATE_KEY target : state.permittedTargets()) {
                    sb.append("    ").append(source.name()).append(" --> ").append(target.name()).append("\n");
                }
                if (state.maxVisitsFallback() != null) {
                    sb.append("    ").append(source.name()).append(" --> ")
                            .append(state.maxVisitsFallback().name())
                            .append(" : max visits (").append(state.maxVisits()).append(") exceeded\n");
                }
            }
        }
        return sb.toString();
    }

    /**
     * Creates a new fluent {@link Builder} for assembling and validating an immutable {@link StateMap}.
     *
     * @param <CONTEXT_TYPE>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY_TYPE> The enum type representing state identifiers
     * @param stateKeyClass    The Class token for the state key enum
     * @return A new {@link Builder} instance
     */
    @NonNull
    public static <CONTEXT_TYPE extends StateMachineContext, STATE_KEY_TYPE extends Enum<STATE_KEY_TYPE> & StateKey>
    Builder<CONTEXT_TYPE, STATE_KEY_TYPE> builder(@NonNull Class<STATE_KEY_TYPE> stateKeyClass) {
        return new Builder<>(stateKeyClass);
    }

    /**
     * Builder for assembling and verifying immutable {@link StateMap} instances.
     *
     * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY> The enum type representing state identifiers
     */
    public static final class Builder<CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey> {
        private final Map<STATE_KEY, State<CONTEXT, STATE_KEY>> states;
        private STATE_KEY initialState;

        public Builder(@NonNull Class<STATE_KEY> stateKeyClass) {
            this.states = new EnumMap<>(Objects.requireNonNull(stateKeyClass, "stateKeyClass must not be null"));
        }

        /**
         * Registers a state definition for the specified state key.
         *
         * @param stateKey The state key identifier
         * @param state    The state definition
         * @return This builder instance for chaining
         */
        @NonNull
        public Builder<CONTEXT, STATE_KEY> addState(@NonNull STATE_KEY stateKey, @NonNull State<CONTEXT, STATE_KEY> state) {
            this.states.put(Objects.requireNonNull(stateKey, "stateKey must not be null"),
                    Objects.requireNonNull(state, "state must not be null"));
            return this;
        }

        /**
         * Designates the starting state key for this state machine.
         *
         * @param initialState The initial state key
         * @return This builder instance for chaining
         */
        @NonNull
        public Builder<CONTEXT, STATE_KEY> initialState(@NonNull STATE_KEY initialState) {
            this.initialState = Objects.requireNonNull(initialState, "initialState must not be null");
            return this;
        }

        /**
         * Registers a terminal end state for the given state key.
         *
         * @param endStateKey The state key representing a completion endpoint
         * @return This builder instance for chaining
         */
        @NonNull
        public Builder<CONTEXT, STATE_KEY> endState(@NonNull STATE_KEY endStateKey) {
            this.states.put(Objects.requireNonNull(endStateKey, "endStateKey must not be null"), State.terminal());
            return this;
        }

        /**
         * Builds and verifies the integrity of the state graph.
         *
         * @return An immutable, validated {@link StateMap} instance
         * @throws IllegalStateException If initial state is missing or any state targets an unregistered state
         */
        @NonNull
        public StateMap<CONTEXT, STATE_KEY> build() {
            if (initialState == null) {
                throw new IllegalStateException("Initial state must be specified before building the state map");
            }
            if (!states.containsKey(initialState)) {
                throw new IllegalStateException("Initial state '" + initialState + "' is not registered in the state map");
            }

            // Verify graph topology: all permitted target states must be registered in the state map
            for (Map.Entry<STATE_KEY, State<CONTEXT, STATE_KEY>> entry : states.entrySet()) {
                STATE_KEY sourceKey = entry.getKey();
                State<CONTEXT, STATE_KEY> state = entry.getValue();

                if (state.isTerminal() && !state.permittedTargets().isEmpty()) {
                    throw new IllegalStateException(String.format(
                            "Graph integrity violation: Terminal end-state '%s' must not declare outgoing transitions %s",
                            sourceKey, state.permittedTargets()
                    ));
                }

                for (STATE_KEY targetKey : state.permittedTargets()) {
                    if (!states.containsKey(targetKey)) {
                        throw new IllegalStateException(String.format(
                                "Graph integrity violation: State '%s' declares transition to unregistered state '%s'",
                                sourceKey, targetKey
                        ));
                    }
                }

                if (state.maxVisitsFallback() != null && !states.containsKey(state.maxVisitsFallback())) {
                    throw new IllegalStateException(String.format(
                            "Graph integrity violation: State '%s' declares max visits fallback to unregistered state '%s'",
                            sourceKey, state.maxVisitsFallback()
                    ));
                }
            }

            return new StateMap<>(states, initialState);
        }
    }
}
