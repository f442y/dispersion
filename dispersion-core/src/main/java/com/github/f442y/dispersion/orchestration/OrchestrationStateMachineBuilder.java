package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.builder.AbstractStateMachineBuilder;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.command.SagaCommand;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.messaging.SignalPublisher;
import com.github.f442y.dispersion.state.Action;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import com.github.f442y.dispersion.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
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
        OUTPUT> extends AbstractStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT,
        OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT>> {

    private final String machineName;
    private Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener;
    private Function<CONTEXT, String> correlationKeyExtractor;
    private CheckpointStore<CONTEXT, STATE_KEY> checkpointStore;

    private OrchestrationStateMachineBuilder(@NonNull String machineName, @NonNull Class<STATE_KEY> stateKeyClass) {
        super(stateKeyClass);
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
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
        public <C extends StateMachineContext, S extends Enum<S> & StateKey, I, O>
        ChildMachineStepBuilder<I, O> atomicMachine(@NonNull StateMachineConfiguration<C, S, I, O> machine) {
            this.childStateMachine = Objects.requireNonNull(machine, "machine must not be null");
            return new ChildMachineStepBuilder<>(this);
        }

        @NonNull
        public <C extends StateMachineContext, S extends Enum<S> & StateKey, I, O>
        ChildMachineStepBuilder<I, O> childMachine(@NonNull StateMachineConfiguration<C, S, I, O> machine) {
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
        public <PAYLOAD> OrchestrationStateStepBuilder waitForSignal(
                @NonNull String signalName,
                @NonNull Class<PAYLOAD> payloadClass,
                @NonNull SignalHandler<CONTEXT, PAYLOAD> handler
        ) {
            Objects.requireNonNull(signalName, "signalName must not be null");
            Objects.requireNonNull(payloadClass, "payloadClass must not be null");
            Objects.requireNonNull(handler, "handler must not be null");
            this.expectedSignal = signalName;
            this.signalHandler = (ctx, payload) -> handler.handleSignal(ctx, (PAYLOAD) payload);
            return this;
        }

        @NonNull
        @SuppressWarnings("unchecked")
        public <CMD extends SignalCommand> OrchestrationStateStepBuilder waitForCommand(
                @NonNull Class<CMD> commandClass,
                @NonNull SignalHandler<CONTEXT, CMD> handler
        ) {
            Objects.requireNonNull(commandClass, "commandClass must not be null");
            Objects.requireNonNull(handler, "handler must not be null");
            this.expectedSignal = commandClass.getSimpleName();
            this.signalHandler = (ctx, payload) -> handler.handleSignal(ctx, (CMD) payload);
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

        private ParallelStepBuilder(@NonNull OrchestrationStateStepBuilder stepBuilder) {
            this.stepBuilder = stepBuilder;
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
            return stepBuilder.transition(nextState);
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            stepBuilder.parallelBranches = branches;
            return stepBuilder.transition(transition);
        }

        @NonNull
        public OrchestrationStateMachineBuilder<CONTEXT, STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<STATE_KEY> permittedTargets,
                @NonNull Transition<CONTEXT, STATE_KEY> transition
        ) {
            stepBuilder.parallelBranches = branches;
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
                checkpointStore
        );
    }

    @NonNull
    public OrchestrationStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> buildExecutor() {
        return new OrchestrationStateMachineExecutor<>(build());
    }
}
