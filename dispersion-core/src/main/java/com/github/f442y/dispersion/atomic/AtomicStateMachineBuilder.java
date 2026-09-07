package com.github.f442y.dispersion.atomic;

import com.github.f442y.dispersion.builder.AbstractStateMachineBuilder;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.Action;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import com.github.f442y.dispersion.state.SubStateMachineAction;
import com.github.f442y.dispersion.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Fluent builder for constructing high-throughput Atomic State Machines.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
 * @param <STATE_KEY> The state identifier enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output return type
 */
public class AtomicStateMachineBuilder<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> extends AbstractStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT,
        AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT>> {

    protected AtomicStateMachineBuilder(@NonNull Class<STATE_KEY> stateKeyClass) {
        super(stateKeyClass);
    }

    protected AtomicStateMachineBuilder(@NonNull String machineName, @NonNull Class<STATE_KEY> stateKeyClass) {
        super(stateKeyClass);
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
    }

    @NonNull
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> create(@NonNull Class<STATE_KEY> stateKeyClass) {
        return new AtomicStateMachineBuilder<>(stateKeyClass);
    }

    @NonNull
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> create(
            @NonNull String machineName,
            @NonNull Class<STATE_KEY> stateKeyClass
    ) {
        return new AtomicStateMachineBuilder<>(machineName, stateKeyClass);
    }

    @NonNull
    public StateStepBuilder state(@NonNull STATE_KEY stateKey) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        return new StateStepBuilder(stateKey);
    }

    public final class StateStepBuilder {
        private final STATE_KEY stateKey;
        private Action<CONTEXT> action = Action.identity();
        private int maxVisits = -1;
        private STATE_KEY maxVisitsFallback;

        private StateStepBuilder(@NonNull STATE_KEY stateKey) {
            this.stateKey = stateKey;
        }

        @NonNull
        public StateStepBuilder action(@NonNull Action<CONTEXT> action) {
            this.action = Objects.requireNonNull(action, "action must not be null");
            return this;
        }

        @NonNull
        public <
                CHILD_CONTEXT extends StateMachineContext,
                CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
                CHILD_INPUT,
                CHILD_OUTPUT>
        StateStepBuilder subStateMachine(
                @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childConfiguration,
                @NonNull Function<CONTEXT, CHILD_INPUT> inputMapper,
                @NonNull BiFunction<CONTEXT, CHILD_OUTPUT, CONTEXT> outputMerger
        ) {
            this.action = new SubStateMachineAction<>(childConfiguration, inputMapper, outputMerger);
            return this;
        }

        @NonNull
        public StateStepBuilder maxVisits(int maxVisits) {
            this.maxVisits = maxVisits;
            return this;
        }

        @NonNull
        public StateStepBuilder maxVisits(int maxVisits, @Nullable STATE_KEY fallbackState) {
            this.maxVisits = maxVisits;
            this.maxVisitsFallback = fallbackState;
            return this;
        }

        @NonNull
        public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(@NonNull STATE_KEY nextState) {
            Objects.requireNonNull(nextState, "nextState must not be null");
            State<CONTEXT, STATE_KEY> node = State.of(
                    action,
                    Set.of(nextState),
                    Transition.to(nextState),
                    maxVisits,
                    maxVisitsFallback
            );
            states.put(stateKey, node);
            return AtomicStateMachineBuilder.this;
        }

        @NonNull
        public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            Objects.requireNonNull(transition, "transition must not be null");
            // If explicit permitted targets were not defined, permitted targets is empty (or end states)
            State<CONTEXT, STATE_KEY> node = State.of(
                    action,
                    Collections.emptySet(),
                    transition,
                    maxVisits,
                    maxVisitsFallback
            );
            states.put(stateKey, node);
            return AtomicStateMachineBuilder.this;
        }

        @NonNull
        public AtomicStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<STATE_KEY> permittedTargets,
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            Objects.requireNonNull(permittedTargets, "permittedTargets must not be null");
            Objects.requireNonNull(transition, "transition must not be null");
            State<CONTEXT, STATE_KEY> node = State.of(
                    action,
                    permittedTargets,
                    transition,
                    maxVisits,
                    maxVisitsFallback
            );
            states.put(stateKey, node);
            return AtomicStateMachineBuilder.this;
        }
    }

    @NonNull
    public StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> build() {
        if (initialState == null) {
            throw new IllegalStateException("Initial state must be configured");
        }

        StateMap.Builder<CONTEXT, STATE_KEY> stateMapBuilder = StateMap.<CONTEXT, STATE_KEY>builder(stateKeyClass)
                .initialState(initialState)
                .endStates(endStates);

        for (Map.Entry<STATE_KEY, State<CONTEXT, STATE_KEY>> entry : states.entrySet()) {
            stateMapBuilder.addState(entry.getKey(), entry.getValue());
        }

        StateMap<CONTEXT, STATE_KEY> stateMap = stateMapBuilder.build();

        return new StateMachineConfiguration<>(
                machineName,
                stateMap,
                maxTransitions,
                contextFactory,
                inputFunction,
                outputFunction,
                exceptionTrigger,
                finishTrigger,
                eventListener
        );
    }

    @NonNull
    public AtomicStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> buildExecutor(@NonNull String name) {
        return new AtomicStateMachineExecutor<>(name, build());
    }

    @NonNull
    public AtomicStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> buildExecutor(
            @NonNull String name,
            int maxConcurrent
    ) {
        return new AtomicStateMachineExecutor<>(name, build(), maxConcurrent);
    }

    @NonNull
    public AtomicStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> buildExecutor() {
        String name = (machineName != null) ? machineName : "atomic-" + stateKeyClass.getSimpleName();
        return buildExecutor(name);
    }
}
