package com.github.f442y.dispersion.fsm.state;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable topological directed graph representation of a Finite State Machine with
 * precomputed $O(1)$ array lookup tables for ultra-high-throughput execution.
 *
 * <h2>Graph Integrity Guarantees &amp; Build-Time Validation</h2>
 * During construction via {@link StateMap.Builder#build()}, the complete graph topology is statically validated:
 * <ul>
 *   <li>The configured {@code initialState} must be registered as a state node or declared terminal state.</li>
 *   <li>All declared target states (in {@link State#permittedTargets()}) must be valid registered states.</li>
 *   <li>Terminal end-states cannot have outgoing transitions or declared permitted targets.</li>
 *   <li>Configured fallback recovery states for visit-limited states must exist in the graph.</li>
 * </ul>
 *
 * <h2>Precomputed $O(1)$ Flat-Array Architecture</h2>
 * To eliminate hashing, boxing, and map traversal overhead on hot virtual thread execution paths:
 * <ul>
 *   <li><b>State Array ({@code statesByOrdinal}):</b> States are mapped into a dense array indexed by {@link Enum#ordinal()}.\n *       Retrievals via {@link #getStateFast(int)} execute as a single direct memory dereference.</li>
 *   <li><b>Terminal State Bitmask ({@code isEndState}):</b> Terminal states are pre-compiled into a flat boolean array.\n *       Checking whether a state is terminal via {@link #isEndStateFast(int)} requires exactly 1 CPU instruction.</li>
 *   <li><b>Feature Flags ({@code hasVisitLimits}, {@code hasAdjacencyConstraints}):</b> Graph structural characteristics\n *       are pre-calculated at build time so execution engines can conditionally bypass visit arrays and adjacency\n *       loops entirely when not configured.</li>
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

    // High-performance precomputed lookup tables (indexed by stateKey.ordinal())
    private final State<CONTEXT, STATE_KEY>[] statesByOrdinal;
    private final boolean[] isEndState;
    private final boolean hasVisitLimits;
    private final boolean hasAdjacencyConstraints;

    @SuppressWarnings("unchecked")
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

        STATE_KEY[] enumConstants = stateKeyClass.getEnumConstants();
        int enumLength = (enumConstants != null) ? enumConstants.length : 0;

        this.statesByOrdinal = (State<CONTEXT, STATE_KEY>[]) Array.newInstance(State.class, enumLength);
        this.isEndState = new boolean[enumLength];

        boolean foundVisitLimits = false;
        boolean foundAdjacency = false;

        for (int i = 0; i < enumLength; i++) {
            STATE_KEY key = enumConstants[i];
            State<CONTEXT, STATE_KEY> node = stateMap.get(key);

            if (this.endStates.contains(key)) {
                this.isEndState[i] = true;
                if (node == null) {
                    node = State.terminal();
                }
            }
            this.statesByOrdinal[i] = node;

            if (node != null) {
                if (node.maxVisits() > 0) {
                    foundVisitLimits = true;
                }
                if (!node.permittedTargets().isEmpty()) {
                    foundAdjacency = true;
                }
            }
        }

        this.hasVisitLimits = foundVisitLimits;
        this.hasAdjacencyConstraints = foundAdjacency;

        validateGraphTopology();
    }

    private void validateGraphTopology() {
        if (!containsState(initialState)) {
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
     * Fast-path $O(1)$ state retrieval by enum ordinal.
     */
    @NonNull
    public State<CONTEXT, STATE_KEY> getStateFast(int ordinal) {
        if (ordinal >= 0 && ordinal < statesByOrdinal.length) {
            State<CONTEXT, STATE_KEY> state = statesByOrdinal[ordinal];
            if (state != null) {
                return state;
            }
        }
        throw new NoSuchElementException("State with ordinal [" + ordinal + "] is not registered in this StateMap");
    }

    /**
     * Fast-path $O(1)$ terminal end-state check by enum ordinal.
     */
    public boolean isEndStateFast(int ordinal) {
        return ordinal >= 0 && ordinal < isEndState.length && isEndState[ordinal];
    }

    /**
     * Fast-path $O(1)$ state existence check by enum ordinal.
     */
    public boolean containsStateFast(int ordinal) {
        return ordinal >= 0 && ordinal < statesByOrdinal.length && statesByOrdinal[ordinal] != null;
    }

    /**
     * Returns the total count of states in the state key enum domain.
     *
     * @return The state count
     */
    public int stateCount() {
        return statesByOrdinal.length;
    }

    /**
     * Returns true if any state in this graph has visit limits configured.
     */
    public boolean hasVisitLimits() {
        return hasVisitLimits;
    }

    /**
     * Returns true if any state in this graph has explicit permitted targets configured.
     */
    public boolean hasAdjacencyConstraints() {
        return hasAdjacencyConstraints;
    }

    /**
     * Retrieves the {@link State} node configuration associated with the given state key.
     */
    @NonNull
    public State<CONTEXT, STATE_KEY> getState(@NonNull STATE_KEY stateKey) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        return getStateFast(stateKey.ordinal());
    }

    /**
     * Checks whether the state key is registered in this state map.
     */
    public boolean containsState(@NonNull STATE_KEY stateKey) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        return containsStateFast(stateKey.ordinal());
    }

    @NonNull
    public STATE_KEY getInitialState() {
        return initialState;
    }

    @NonNull
    public Set<STATE_KEY> getEndStates() {
        return endStates;
    }

    @NonNull
    public Class<STATE_KEY> getStateKeyClass() {
        return stateKeyClass;
    }

    @NonNull
    public Map<STATE_KEY, State<CONTEXT, STATE_KEY>> getAllStates() {
        return stateMap;
    }

    /**
     * Generates a Mermaid stateDiagram-v2 string representing this Finite State Machine topology.
     */
    @NonNull
    public String toMermaid() {
        StringBuilder sb = new StringBuilder();
        sb.append("stateDiagram-v2\n");
        sb.append("    [*] --> ").append(initialState.name()).append("\n");

        for (Map.Entry<STATE_KEY, State<CONTEXT, STATE_KEY>> entry : stateMap.entrySet()) {
            STATE_KEY source = entry.getKey();
            State<CONTEXT, STATE_KEY> stateNode = entry.getValue();

            for (STATE_KEY target : stateNode.permittedTargets()) {
                if (endStates.contains(target)) {
                    sb.append("    ").append(source.name()).append(" --> [*] : ").append(target.name()).append("\n");
                } else {
                    sb.append("    ").append(source.name()).append(" --> ").append(target.name()).append("\n");
                }
            }

            STATE_KEY fallback = stateNode.maxVisitsFallback();
            if (fallback != null) {
                sb.append("    ").append(source.name()).append(" --> ").append(fallback.name())
                        .append(" : fallback (maxVisits exceeded)\n");
            }
        }

        for (STATE_KEY endState : endStates) {
            if (!stateMap.containsKey(endState)) {
                sb.append("    ").append(endState.name()).append(" --> [*]\n");
            }
        }

        return sb.toString();
    }

    @NonNull
    public static <C extends StateMachineContext, S extends Enum<S> & StateKey> Builder<C, S> builder(@NonNull Class<S> stateKeyClass) {
        return new Builder<>(stateKeyClass);
    }

    public static final class Builder<CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey> {
        private final Class<STATE_KEY> stateKeyClass;
        private final Map<STATE_KEY, State<CONTEXT, STATE_KEY>> stateMap;
        private STATE_KEY initialState;
        private final Set<STATE_KEY> endStates;

        Builder(@NonNull Class<STATE_KEY> stateKeyClass) {
            this.stateKeyClass = Objects.requireNonNull(stateKeyClass, "stateKeyClass must not be null");
            this.stateMap = new EnumMap<>(stateKeyClass);
            this.endStates = EnumSet.noneOf(stateKeyClass);
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> initialState(@NonNull STATE_KEY initialState) {
            this.initialState = Objects.requireNonNull(initialState, "initialState must not be null");
            return this;
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> addEndState(@NonNull STATE_KEY endState) {
            Objects.requireNonNull(endState, "endState must not be null");
            this.endStates.add(endState);
            return this;
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> endState(@NonNull STATE_KEY endState) {
            return addEndState(endState);
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> endStates(@NonNull Set<STATE_KEY> endStates) {
            Objects.requireNonNull(endStates, "endStates must not be null");
            this.endStates.addAll(endStates);
            return this;
        }

        @NonNull
        public Builder<CONTEXT, STATE_KEY> addState(
                @NonNull STATE_KEY stateKey,
                @NonNull State<CONTEXT, STATE_KEY> state
        ) {
            Objects.requireNonNull(stateKey, "stateKey must not be null");
            Objects.requireNonNull(state, "state must not be null");
            this.stateMap.put(stateKey, state);
            return this;
        }

        @NonNull
        public StateMap<CONTEXT, STATE_KEY> build() {
            if (initialState == null) {
                throw new IllegalStateException("Initial state must be configured in StateMap");
            }
            return new StateMap<>(stateKeyClass, stateMap, initialState, endStates);
        }
    }
}
