package com.github.f442y.dispersion.atomic;

import com.github.f442y.dispersion.builder.AbstractStateMachineBuilder;
import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.context.StateMachineContextFactory;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.executor.AdmissionController;
import com.github.f442y.dispersion.state.Action;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import com.github.f442y.dispersion.state.SubStateMachineAction;
import com.github.f442y.dispersion.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Fluent builder domain-specific language (DSL) for declaring, assembling,
 * and configuring Atomic (micro / thread-bound) Finite State Machines.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by the state machine
 * @param <OUTPUT>    The type of output result produced upon state machine completion
 */
public final class AtomicStateMachineBuilder<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        extends AbstractStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT, AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT>> {

    private final Map<STATE_KEY, StateHolder<CONTEXT, STATE_KEY>> stateHolders = new LinkedHashMap<>();

    private record StateHolder<C extends StateMachineContext, S extends Enum<S> & StateKey>(
            Action<C> action,
            Set<S> explicitTargets,
            Transition<C, S> transition,
            int maxVisits,
            S maxVisitsFallback,
            State<C, S> prebuiltState
    ) {}

    private AtomicStateMachineBuilder(@NonNull Class<STATE_KEY> stateKeyClass) {
        super(stateKeyClass);
    }

    /**
     * Initializes a new fluent builder for defining an atomic state machine using the specified state key enum.
     *
     * @param <CONTEXT_TYPE>   The concrete type of {@link StateMachineContext}
     * @param <STATE_KEY_TYPE> The enum type representing state identifiers
     * @param <INPUT_TYPE>     The type of input payload
     * @param <OUTPUT_TYPE>    The type of output result
     * @param stateKeyClass    The class of the state key enum
     * @return A new {@link AtomicStateMachineBuilder} instance
     */
    @NonNull
    public static <CONTEXT_TYPE extends StateMachineContext,
            STATE_KEY_TYPE extends Enum<STATE_KEY_TYPE> & StateKey,
            INPUT_TYPE,
            OUTPUT_TYPE>
    AtomicStateMachineBuilder<CONTEXT_TYPE, STATE_KEY_TYPE, INPUT_TYPE, OUTPUT_TYPE> create(
            @NonNull Class<STATE_KEY_TYPE> stateKeyClass
    ) {
        return new AtomicStateMachineBuilder<>(stateKeyClass);
    }

    /**
     * Begins defining an atomic state step using the chained sub-builder syntax.
     *
     * @param stateKey The state key identifier being defined
     * @return An {@link AtomicStateDefinitionBuilder} for configuring this state's action and transition
     */
    @NonNull
    public AtomicStateDefinitionBuilder state(@NonNull STATE_KEY stateKey) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        return new AtomicStateDefinitionBuilder(stateKey);
    }

    /**
     * Registers a state step with an action, explicit permitted target states, and dynamic transition in a single call.
     *
     * @param stateKey         The state key identifier
     * @param action           The business action to execute
     * @param permittedTargets The explicit set of valid next states
     * @param transition       The routing logic for determining the next state
     * @return This builder instance for chaining
     */
    @NonNull
    public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> state(
            @NonNull STATE_KEY stateKey,
            @NonNull Action<CONTEXT> action,
            @NonNull Set<STATE_KEY> permittedTargets,
            @NonNull Transition<CONTEXT, STATE_KEY> transition
    ) {
        stateHolders.put(stateKey, new StateHolder<>(action, permittedTargets, transition, -1, null, null));
        return this;
    }

    /**
     * Registers a state step with an action and dynamic transition in a single call.
     * Permitted targets default to all registered states in the machine.
     *
     * @param stateKey   The state key identifier
     * @param action     The business action to execute
     * @param transition The routing logic for determining the next state
     * @return This builder instance for chaining
     */
    @NonNull
    public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> state(
            @NonNull STATE_KEY stateKey,
            @NonNull Action<CONTEXT> action,
            @NonNull Transition<CONTEXT, STATE_KEY> transition
    ) {
        stateHolders.put(stateKey, new StateHolder<>(action, null, transition, -1, null, null));
        return this;
    }

    /**
     * Registers a state step with an action and unconditional next state in a single call.
     *
     * @param stateKey  The state key identifier
     * @param action    The business action to execute
     * @param nextState The deterministic next state key
     * @return This builder instance for chaining
     */
    @NonNull
    public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> state(
            @NonNull STATE_KEY stateKey,
            @NonNull Action<CONTEXT> action,
            @NonNull STATE_KEY nextState
    ) {
        stateHolders.put(stateKey, new StateHolder<>(action, Set.of(nextState), Transition.to(nextState), -1, null, null));
        return this;
    }

    /**
     * Directly adds a pre-constructed {@link State} instance to the state map.
     *
     * @param stateKey The state key identifier
     * @param state    The pre-constructed state definition
     * @return This builder instance for chaining
     */
    @NonNull
    public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> addState(
            @NonNull STATE_KEY stateKey,
            @NonNull State<CONTEXT, STATE_KEY> state
    ) {
        stateHolders.put(stateKey, new StateHolder<>(null, null, null, -1, null, state));
        return this;
    }

    /**
     * Validates graph integrity and constructs the immutable {@link StateMachineConfiguration}.
     *
     * @return An executable, verified {@link StateMachineConfiguration} instance
     * @throws IllegalStateException If the initial state is missing or any state targets an unregistered state
     */
    @NonNull
    public StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> build() {
        if (initialStateKey == null) {
            throw new IllegalStateException("Initial state must be specified before building atomic state machine");
        }
        if (contextFactory == null) {
            throw new IllegalStateException("Context factory must be specified before building atomic state machine");
        }

        Set<STATE_KEY> allRegisteredKeys = new HashSet<>(stateHolders.keySet());
        allRegisteredKeys.addAll(endStateKeys);

        StateMap.Builder<CONTEXT, STATE_KEY> builder = StateMap.<CONTEXT, STATE_KEY>builder(stateKeyClass)
                .initialState(initialStateKey);

        for (Map.Entry<STATE_KEY, StateHolder<CONTEXT, STATE_KEY>> entry : stateHolders.entrySet()) {
            STATE_KEY key = entry.getKey();
            StateHolder<CONTEXT, STATE_KEY> holder = entry.getValue();

            if (holder.prebuiltState != null) {
                builder.addState(key, holder.prebuiltState);
            } else {
                Set<STATE_KEY> targets = (holder.explicitTargets != null)
                        ? holder.explicitTargets
                        : allRegisteredKeys;
                builder.addState(key, State.of(
                        holder.action,
                        targets,
                        holder.transition,
                        holder.maxVisits,
                        holder.maxVisitsFallback
                ));
            }
        }

        for (STATE_KEY endKey : endStateKeys) {
            builder.endState(endKey);
        }

        StateMap<CONTEXT, STATE_KEY> builtStateMap = builder.build();

        final StateMachineContextFactory<CONTEXT> finalContextFactory = this.contextFactory;
        final InputFunction<CONTEXT, INPUT> finalInputFunction = this.inputFunction;
        final OutputFunction<CONTEXT, OUTPUT> finalOutputFunction = this.outputFunction;
        final int finalMaxTransitions = this.maxTransitions;

        return new StateMachineConfiguration<>(builtStateMap, finalMaxTransitions) {
            @NonNull
            @Override
            public StateMachineContextFactory<CONTEXT> stateMachineContextFactory() {
                return finalContextFactory;
            }

            @Nullable
            @Override
            public InputFunction<CONTEXT, INPUT> inputFunction() throws StateMachineException {
                return finalInputFunction;
            }

            @Nullable
            @Override
            public OutputFunction<CONTEXT, OUTPUT> outputFunction() throws StateMachineException {
                return finalOutputFunction;
            }
        };
    }

    /**
     * Builds and returns an active {@link AtomicStateMachineExecutor} ready for dispatch.
     *
     * @param machineName The name identifier of the executor
     * @return An {@link AtomicStateMachineExecutor} instance
     */
    @NonNull
    public AtomicStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> buildExecutor(@NonNull String machineName) {
        return new AtomicStateMachineExecutor<>(machineName, build());
    }

    /**
     * Builds and returns an active {@link AtomicStateMachineExecutor} with a custom buffer size.
     *
     * @param machineName The name identifier of the executor
     * @param bufferSize  Maximum concurrency limit
     * @return An {@link AtomicStateMachineExecutor} instance
     */
    @NonNull
    public AtomicStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> buildExecutor(
            @NonNull String machineName,
            int bufferSize
    ) {
        return new AtomicStateMachineExecutor<>(machineName, build(), bufferSize);
    }

    /**
     * Builds and returns an active {@link AtomicStateMachineExecutor} with a custom admission controller.
     *
     * @param machineName         The name identifier of the executor
     * @param admissionController The admission controller
     * @return An {@link AtomicStateMachineExecutor} instance
     */
    @NonNull
    public AtomicStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> buildExecutor(
            @NonNull String machineName,
            @NonNull AdmissionController admissionController
    ) {
        return new AtomicStateMachineExecutor<>(machineName, build(), admissionController);
    }

    /**
     * Sub-builder for incrementally configuring a specific atomic state's action, nested child machine,
     * permitted targets, visit limits, and transition routing.
     */
    public class AtomicStateDefinitionBuilder {
        private final STATE_KEY stateKey;
        private Action<CONTEXT> action;
        private int maxVisits = -1;
        private STATE_KEY maxVisitsFallback;

        private AtomicStateDefinitionBuilder(@NonNull STATE_KEY stateKey) {
            this.stateKey = stateKey;
        }

        /**
         * Sets the business logic action to execute for this state.
         *
         * @param action The action business logic
         * @return This builder instance for chaining
         */
        @NonNull
        public AtomicStateDefinitionBuilder action(@NonNull Action<CONTEXT> action) {
            this.action = Objects.requireNonNull(action, "action must not be null");
            return this;
        }

        /**
         * Sets a visit limit on this state before an exception is thrown.
         *
         * @param maxVisits Maximum number of times this state can be visited
         * @return This builder instance for chaining
         */
        @NonNull
        public AtomicStateDefinitionBuilder maxVisits(int maxVisits) {
            this.maxVisits = maxVisits;
            return this;
        }

        /**
         * Sets a visit limit on this state and automatically diverts to the fallback state when exceeded.
         *
         * @param maxVisits Maximum visits allowed before diverting
         * @param fallback  The fallback state to transition to
         * @return This builder instance for chaining
         */
        @NonNull
        public AtomicStateDefinitionBuilder maxVisits(int maxVisits, @NonNull STATE_KEY fallback) {
            this.maxVisits = maxVisits;
            this.maxVisitsFallback = Objects.requireNonNull(fallback, "fallback state must not be null");
            return this;
        }

        /**
         * Configures this state to execute a nested child state machine synchronously.
         *
         * @param <CHILD_CONTEXT>   The child state machine's context type
         * @param <CHILD_STATE_KEY> The child state key enum
         * @param <CHILD_INPUT>     The child input payload type
         * @param <CHILD_OUTPUT>    The child output result type
         * @param childStateMachine The child state machine configuration
         * @param inputMapper       Mapper transforming parent context to child input
         * @param outputMerger      Merger combining child output back into parent context
         * @return This builder instance for chaining
         */
        @NonNull
        public <CHILD_CONTEXT extends StateMachineContext,
                CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
                CHILD_INPUT,
                CHILD_OUTPUT>
        AtomicStateDefinitionBuilder subStateMachine(
                @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childStateMachine,
                @NonNull Function<CONTEXT, CHILD_INPUT> inputMapper,
                @NonNull BiFunction<CONTEXT, CHILD_OUTPUT, CONTEXT> outputMerger
        ) {
            this.action = new SubStateMachineAction<>(childStateMachine, inputMapper, outputMerger);
            return this;
        }

        /**
         * Configures this state to execute a nested child state machine with initial context mapping.
         *
         * @param <CHILD_CONTEXT>      The child state machine's context type
         * @param <CHILD_STATE_KEY>    The child state key enum
         * @param <CHILD_INPUT>        The child input payload type
         * @param <CHILD_OUTPUT>       The child output result type
         * @param childStateMachine    The child state machine configuration
         * @param inputMapper          Mapper transforming parent context to child input
         * @param initialContextMapper Mapper initializing child context from parent context
         * @param outputMerger         Merger combining child output back into parent context
         * @return This builder instance for chaining
         */
        @NonNull
        public <CHILD_CONTEXT extends StateMachineContext,
                CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
                CHILD_INPUT,
                CHILD_OUTPUT>
        AtomicStateDefinitionBuilder subStateMachine(
                @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childStateMachine,
                @NonNull Function<CONTEXT, CHILD_INPUT> inputMapper,
                @Nullable Function<CONTEXT, CHILD_CONTEXT> initialContextMapper,
                @NonNull BiFunction<CONTEXT, CHILD_OUTPUT, CONTEXT> outputMerger
        ) {
            this.action = new SubStateMachineAction<>(childStateMachine, inputMapper, initialContextMapper, outputMerger);
            return this;
        }

        /**
         * Configures dynamic transition logic with an explicit set of permitted target states.
         *
         * @param permittedTargets The explicit set of valid target state keys
         * @param transition       The dynamic routing function
         * @return The parent {@link AtomicStateMachineBuilder} instance
         */
        @NonNull
        public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<STATE_KEY> permittedTargets,
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            Objects.requireNonNull(permittedTargets, "permittedTargets must not be null");
            Objects.requireNonNull(transition, "transition must not be null");
            Action<CONTEXT> effectiveAction = (this.action != null) ? this.action : Action.identity();
            stateHolders.put(stateKey, new StateHolder<>(
                    effectiveAction,
                    permittedTargets,
                    transition,
                    this.maxVisits,
                    this.maxVisitsFallback,
                    null
            ));
            return AtomicStateMachineBuilder.this;
        }

        /**
         * Configures dynamic transition logic where permitted targets default to all registered states in the machine.
         *
         * @param transition The dynamic routing function taking context and returning next state key
         * @return The parent {@link AtomicStateMachineBuilder} instance
         */
        @NonNull
        public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            Objects.requireNonNull(transition, "transition must not be null");
            Action<CONTEXT> effectiveAction = (this.action != null) ? this.action : Action.identity();
            stateHolders.put(stateKey, new StateHolder<>(
                    effectiveAction,
                    null,
                    transition,
                    this.maxVisits,
                    this.maxVisitsFallback,
                    null
            ));
            return AtomicStateMachineBuilder.this;
        }

        /**
         * Finalizes this state definition with an unconditional next state.
         *
         * @param nextState The deterministic next state key
         * @return The parent {@link AtomicStateMachineBuilder} instance
         */
        @NonNull
        public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull STATE_KEY nextState
        ) {
            return transitionsTo(Set.of(nextState), Transition.to(nextState));
        }
    }
}
