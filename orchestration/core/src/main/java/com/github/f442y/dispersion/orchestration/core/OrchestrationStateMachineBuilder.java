package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.fsm.config.InputFunction;
import com.github.f442y.dispersion.fsm.config.OutputFunction;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.context.StateMachineContextFactory;
import com.github.f442y.dispersion.fsm.state.Action;
import com.github.f442y.dispersion.fsm.state.State;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import com.github.f442y.dispersion.fsm.state.Transition;
import com.github.f442y.dispersion.orchestration.CheckpointStore;
import com.github.f442y.dispersion.orchestration.CompensationAction;
import com.github.f442y.dispersion.orchestration.ContextRecoverer;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.OrchestrationState;
import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineConfiguration;
import com.github.f442y.dispersion.orchestration.ParallelBranch;
import com.github.f442y.dispersion.orchestration.RetryPolicy;
import com.github.f442y.dispersion.orchestration.SignalHandler;
import com.github.f442y.dispersion.orchestration.command.SagaCommand;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.messaging.SignalPublisher;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Fluent builder for creating durable, turn-based Macro Orchestration State Machines.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 * @param <INPUT>     The input type
 * @param <OUTPUT>    The output type
 */
public class OrchestrationStateMachineBuilder<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> {

    protected final Class<STATE_KEY> stateKeyClass;
    protected final Map<STATE_KEY, State<CONTEXT, STATE_KEY>> states;
    protected String machineName;
    protected STATE_KEY initialState;
    protected final Set<STATE_KEY> endStates;
    protected int maxTransitions = -1;
    protected StateMachineContextFactory<CONTEXT> contextFactory;
    protected InputFunction<CONTEXT, INPUT> inputFunction;
    protected OutputFunction<CONTEXT, OUTPUT> outputFunction;
    protected BiConsumer<CONTEXT, Throwable> exceptionTrigger;
    protected Consumer<CONTEXT> finishTrigger;
    protected ExecutionEventListener eventListener;

    private Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener;
    private Function<CONTEXT, String> correlationKeyExtractor;
    private CheckpointStore<CONTEXT, STATE_KEY> checkpointStore;

    private OrchestrationStateMachineBuilder(@NonNull String machineName, @NonNull Class<STATE_KEY> stateKeyClass) {
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
        this.stateKeyClass = Objects.requireNonNull(stateKeyClass, "stateKeyClass must not be null");
        this.states = new EnumMap<>(stateKeyClass);
        this.endStates = EnumSet.noneOf(stateKeyClass);
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> name(@NonNull String name) {
        this.machineName = Objects.requireNonNull(name, "name must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> eventListener(@NonNull ExecutionEventListener eventListener) {
        this.eventListener = Objects.requireNonNull(eventListener, "eventListener must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> context(@NonNull StateMachineContextFactory<CONTEXT> factory) {
        this.contextFactory = Objects.requireNonNull(factory, "factory must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> initialState(@NonNull STATE_KEY initialState) {
        this.initialState = Objects.requireNonNull(initialState, "initialState must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> endState(@NonNull STATE_KEY endState) {
        Objects.requireNonNull(endState, "endState must not be null");
        this.endStates.add(endState);
        return this;
    }

    @NonNull
    @SafeVarargs
    public final OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> endStates(@NonNull STATE_KEY... endStates) {
        for (STATE_KEY s : endStates) {
            endState(s);
        }
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> endStates(@NonNull Set<STATE_KEY> endStates) {
        Objects.requireNonNull(endStates, "endStates must not be null");
        this.endStates.addAll(endStates);
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> maxTransitions(int maxTransitions) {
        this.maxTransitions = maxTransitions;
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> input(@NonNull InputFunction<CONTEXT, INPUT> inputFunction) {
        this.inputFunction = Objects.requireNonNull(inputFunction, "inputFunction must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> output(@NonNull OutputFunction<CONTEXT, OUTPUT> outputFunction) {
        this.outputFunction = Objects.requireNonNull(outputFunction, "outputFunction must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> onException(@NonNull BiConsumer<CONTEXT, Throwable> exceptionTrigger) {
        this.exceptionTrigger = Objects.requireNonNull(exceptionTrigger, "exceptionTrigger must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> onFinish(@NonNull Consumer<CONTEXT> finishTrigger) {
        this.finishTrigger = Objects.requireNonNull(finishTrigger, "finishTrigger must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> addState(@NonNull STATE_KEY stateKey, @NonNull State<CONTEXT, STATE_KEY> state) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        Objects.requireNonNull(state, "state must not be null");
        this.states.put(stateKey, state);
        return this;
    }

    @NonNull
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> create(
            @NonNull String machineName,
            @NonNull Class<STATE_KEY> stateKeyClass
    ) {
        return new OrchestrationStateMachineBuilder<>(machineName, stateKeyClass);
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> onCheckpoint(
            @NonNull Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> listener
    ) {
        this.checkpointListener = Objects.requireNonNull(listener, "listener must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> correlationKey(
            @NonNull Function<CONTEXT, String> extractor
    ) {
        this.correlationKeyExtractor = Objects.requireNonNull(extractor, "extractor must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> checkpointStore(
            @NonNull CheckpointStore<CONTEXT, STATE_KEY> store
    ) {
        this.checkpointStore = Objects.requireNonNull(store, "store must not be null");
        return this;
    }

    @NonNull
    public OrchestrationStateStepBuilder state(@NonNull STATE_KEY stateKey) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        return new OrchestrationStateStepBuilder(stateKey);
    }

    public final class OrchestrationStateStepBuilder {
        private final STATE_KEY stateKey;
        private Action<CONTEXT> action = Action.identity();
        private CompensationAction<CONTEXT> compensationAction;
        private StateMachineConfiguration<?, ?, ?, ?> childStateMachine;
        private Function<CONTEXT, ?> childInputExtractor;
        private BiFunction<CONTEXT, Object, CONTEXT> childOutputMerger;
        private ContextRecoverer<CONTEXT, ?> contextRecoverer;
        private RetryPolicy retryPolicy = RetryPolicy.noRetries();
        private List<ParallelBranch<CONTEXT>> parallelBranches;
        private Function<CONTEXT, CONTEXT> parallelContextCloner;
        private BinaryOperator<CONTEXT> parallelContextReducer;
        private String expectedSignal;
        private SignalHandler<CONTEXT, ?> signalHandler;
        private SignalPublisher signalPublisher;
        private String publishDestination;
        private Function<CONTEXT, ?> publishPayloadExtractor;

        private OrchestrationStateStepBuilder(@NonNull STATE_KEY stateKey) {
            this.stateKey = stateKey;
        }

        @NonNull
        public OrchestrationStateStepBuilder action(@NonNull Action<CONTEXT> action) {
            this.action = Objects.requireNonNull(action, "action must not be null");
            return this;
        }

        @NonNull
        public OrchestrationStateStepBuilder command(@NonNull SagaCommand<CONTEXT> sagaCommand) {
            Objects.requireNonNull(sagaCommand, "sagaCommand must not be null");
            this.action = sagaCommand::execute;
            this.compensationAction = sagaCommand::compensate;
            return this;
        }

        @NonNull
        public OrchestrationStateStepBuilder compensate(@NonNull CompensationAction<CONTEXT> compensation) {
            this.compensationAction = Objects.requireNonNull(compensation, "compensation must not be null");
            return this;
        }

        @NonNull
        public <CHILD_CONTEXT extends StateMachineContext, CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey, CHILD_INPUT, CHILD_OUTPUT>
        ChildMachineStepBuilder<CHILD_INPUT, CHILD_OUTPUT> atomicMachine(@NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> machine) {
            this.childStateMachine = Objects.requireNonNull(machine, "machine must not be null");
            return new ChildMachineStepBuilder<>(this);
        }

        @NonNull
        public <CHILD_CONTEXT extends StateMachineContext, CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey, CHILD_INPUT, CHILD_OUTPUT>
        ChildMachineStepBuilder<CHILD_INPUT, CHILD_OUTPUT> childMachine(@NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> machine) {
            this.childStateMachine = Objects.requireNonNull(machine, "machine must not be null");
            return new ChildMachineStepBuilder<>(this);
        }

        @NonNull
        public ParallelStepBuilder parallel() {
            return new ParallelStepBuilder(this);
        }

        @NonNull
        public OrchestrationStateStepBuilder waitForSignal(
                @NonNull String signalName,
                @NonNull SignalHandler<CONTEXT, ?> handler
        ) {
            this.expectedSignal = Objects.requireNonNull(signalName, "signalName must not be null");
            this.signalHandler = Objects.requireNonNull(handler, "handler must not be null");
            return this;
        }

        @NonNull
        @SuppressWarnings("unchecked")
        public <SIGNAL_PAYLOAD> OrchestrationStateStepBuilder waitForSignal(
                @NonNull String signalName,
                @NonNull Class<SIGNAL_PAYLOAD> payloadClass,
                @NonNull SignalHandler<CONTEXT, SIGNAL_PAYLOAD> handler
        ) {
            Objects.requireNonNull(signalName, "signalName must not be null");
            Objects.requireNonNull(payloadClass, "payloadClass must not be null");
            Objects.requireNonNull(handler, "handler must not be null");
            this.expectedSignal = signalName;
            this.signalHandler = (ctx, payload) -> handler.handleSignal(ctx, (SIGNAL_PAYLOAD) payload);
            return this;
        }

        @NonNull
        @SuppressWarnings("unchecked")
        public <COMMAND_TYPE extends SignalCommand> OrchestrationStateStepBuilder waitForCommand(
                @NonNull Class<COMMAND_TYPE> commandClass,
                @NonNull SignalHandler<CONTEXT, COMMAND_TYPE> handler
        ) {
            Objects.requireNonNull(commandClass, "commandClass must not be null");
            Objects.requireNonNull(handler, "handler must not be null");
            this.expectedSignal = commandClass.getSimpleName();
            this.signalHandler = (ctx, payload) -> handler.handleSignal(ctx, (COMMAND_TYPE) payload);
            return this;
        }

        @NonNull
        public OrchestrationStateStepBuilder publish(
                @NonNull SignalPublisher publisher,
                @NonNull String destination,
                @NonNull Function<CONTEXT, ?> payloadExtractor
        ) {
            this.signalPublisher = Objects.requireNonNull(publisher, "publisher must not be null");
            this.publishDestination = Objects.requireNonNull(destination, "destination must not be null");
            this.publishPayloadExtractor = Objects.requireNonNull(payloadExtractor, "payloadExtractor must not be null");
            return this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull STATE_KEY nextState
        ) {
            Objects.requireNonNull(nextState, "nextState must not be null");
            return transition(Set.of(nextState), Transition.to(nextState));
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            Objects.requireNonNull(transition, "transition must not be null");
            return transition(Collections.emptySet(), transition);
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<STATE_KEY> permittedTargets,
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            return transition(permittedTargets, transition);
        }

        private OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                Set<STATE_KEY> permittedTargets,
                Transition<CONTEXT, STATE_KEY> transition
        ) {
            OrchestrationState<CONTEXT, STATE_KEY> node = new OrchestrationState<>(
                    action,
                    transition,
                    permittedTargets,
                    false,
                    -1,
                    null,
                    compensationAction,
                    childStateMachine,
                    childInputExtractor,
                    childOutputMerger,
                    contextRecoverer,
                    retryPolicy,
                    parallelBranches,
                    parallelContextCloner,
                    parallelContextReducer,
                    expectedSignal,
                    signalHandler,
                    signalPublisher,
                    publishDestination,
                    publishPayloadExtractor
            );
            states.put(stateKey, node);
            return OrchestrationStateMachineBuilder.this;
        }
    }

    public final class ChildMachineStepBuilder<CHILD_INPUT, CHILD_OUTPUT> {
        private final OrchestrationStateStepBuilder stepBuilder;

        private ChildMachineStepBuilder(@NonNull OrchestrationStateStepBuilder stepBuilder) {
            this.stepBuilder = stepBuilder;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_INPUT, CHILD_OUTPUT> input(@NonNull Function<CONTEXT, CHILD_INPUT> inputExtractor) {
            stepBuilder.childInputExtractor = Objects.requireNonNull(inputExtractor, "inputExtractor must not be null");
            return this;
        }

        @NonNull
        @SuppressWarnings("unchecked")
        public ChildMachineStepBuilder<CHILD_INPUT, CHILD_OUTPUT> output(@NonNull BiFunction<CONTEXT, CHILD_OUTPUT, CONTEXT> outputMerger) {
            Objects.requireNonNull(outputMerger, "outputMerger must not be null");
            stepBuilder.childOutputMerger = (ctx, out) -> outputMerger.apply(ctx, (CHILD_OUTPUT) out);
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_INPUT, CHILD_OUTPUT> recoverer(@NonNull ContextRecoverer<CONTEXT, CHILD_INPUT> recoverer) {
            stepBuilder.contextRecoverer = Objects.requireNonNull(recoverer, "recoverer must not be null");
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_INPUT, CHILD_OUTPUT> retry(@NonNull RetryPolicy policy) {
            stepBuilder.retryPolicy = Objects.requireNonNull(policy, "policy must not be null");
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_INPUT, CHILD_OUTPUT> compensate(@NonNull CompensationAction<CONTEXT> compensation) {
            stepBuilder.compensationAction = Objects.requireNonNull(compensation, "compensation must not be null");
            return this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull STATE_KEY nextState
        ) {
            return stepBuilder.transition(nextState);
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            return stepBuilder.transition(transition);
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<STATE_KEY> permittedTargets,
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            return stepBuilder.transitionsTo(permittedTargets, transition);
        }
    }

    public final class ParallelStepBuilder {
        private final OrchestrationStateStepBuilder stepBuilder;
        private final List<ParallelBranch<CONTEXT>> branches = new ArrayList<>();
        private Function<CONTEXT, CONTEXT> cloner;
        private BinaryOperator<CONTEXT> reducer;

        private ParallelStepBuilder(@NonNull OrchestrationStateStepBuilder stepBuilder) {
            this.stepBuilder = stepBuilder;
        }

        /**
         * Configures an isolator/cloner function creating independent context copies for each parallel branch.
         * <p>
         * Use this when context implementations contain unsynchronized collections or fields, ensuring
         * each parallel branch executes without memory contention or data races.
         *
         * @param cloner Function creating an isolated context copy
         * @return This builder
         */
        @NonNull
        public ParallelStepBuilder cloner(@NonNull Function<CONTEXT, CONTEXT> cloner) {
            this.cloner = Objects.requireNonNull(cloner, "cloner must not be null");
            return this;
        }

        /**
         * Configures a reducer function folding parallel branch outputs back into the primary context.
         * <p>
         * After all parallel branches complete successfully, this reducer is called sequentially in
         * branch registration order to merge each branch's returned context into the root context.
         *
         * @param reducer Reducer combining (currentRootContext, branchResultContext) -&gt; mergedContext
         * @return This builder
         */
        @NonNull
        public ParallelStepBuilder reducer(@NonNull BinaryOperator<CONTEXT> reducer) {
            this.reducer = Objects.requireNonNull(reducer, "reducer must not be null");
            return this;
        }

        @NonNull
        public ParallelStepBuilder branch(@NonNull String name, @NonNull Action<CONTEXT> branchAction) {
            branches.add(ParallelBranch.of(name, branchAction));
            return this;
        }

        @NonNull
        public ParallelStepBuilder branch(
                @NonNull String name,
                @NonNull Action<CONTEXT> branchAction,
                @Nullable CompensationAction<CONTEXT> branchCompensation
        ) {
            branches.add(ParallelBranch.of(name, branchAction, branchCompensation));
            return this;
        }

        @NonNull
        public ParallelStepBuilder compensate(@NonNull CompensationAction<CONTEXT> parallelCompensation) {
            stepBuilder.compensationAction = parallelCompensation;
            return this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull STATE_KEY nextState
        ) {
            stepBuilder.parallelBranches = branches;
            stepBuilder.parallelContextCloner = cloner;
            stepBuilder.parallelContextReducer = reducer;
            return stepBuilder.transition(nextState);
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            stepBuilder.parallelBranches = branches;
            stepBuilder.parallelContextCloner = cloner;
            stepBuilder.parallelContextReducer = reducer;
            return stepBuilder.transition(transition);
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<STATE_KEY> permittedTargets,
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            stepBuilder.parallelBranches = branches;
            stepBuilder.parallelContextCloner = cloner;
            stepBuilder.parallelContextReducer = reducer;
            return stepBuilder.transitionsTo(permittedTargets, transition);
        }
    }

    @NonNull
    public OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> build() {
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

        return new OrchestrationStateMachineConfiguration<>(
                machineName,
                stateMap,
                maxTransitions,
                contextFactory,
                inputFunction,
                outputFunction,
                exceptionTrigger,
                finishTrigger,
                checkpointListener,
                correlationKeyExtractor,
                checkpointStore,
                eventListener
        );
    }

    @NonNull
    public OrchestrationStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> buildExecutor() {
        return new OrchestrationStateMachineExecutor<>(build());
    }
}
