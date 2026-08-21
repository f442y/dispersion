package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.builder.AbstractStateMachineBuilder;
import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.context.StateMachineContextFactory;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.executor.AdmissionController;
import com.github.f442y.dispersion.state.Action;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import com.github.f442y.dispersion.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Fluent builder DSL for assembling Orchestration State Machines with directed graph routing,
 * atomic child state machine executions, nested orchestrations, parallel fork-join execution,
 * context recovery, and Saga compensations.
 *
 * @param <ORCHESTRATION_CONTEXT>   The orchestration context type
 * @param <ORCHESTRATION_STATE_KEY> The orchestration state key enum type
 * @param <INPUT>                   The input payload type
 * @param <OUTPUT>                  The output result type
 */
public final class OrchestrationStateMachineBuilder<
        ORCHESTRATION_CONTEXT extends StateMachineContext,
        ORCHESTRATION_STATE_KEY extends Enum<ORCHESTRATION_STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        extends AbstractStateMachineBuilder<
        ORCHESTRATION_CONTEXT,
        ORCHESTRATION_STATE_KEY,
        INPUT,
        OUTPUT,
        OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT>> {

    @NonNull
    private final String machineName;

    @Nullable
    private Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> checkpointListener;

    private final Map<ORCHESTRATION_STATE_KEY, OrchestrationStateHolder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> stateHolders = new LinkedHashMap<>();

    private record OrchestrationStateHolder<C extends StateMachineContext, S extends Enum<S> & StateKey>(
            Action<C> action,
            Set<S> explicitTargets,
            Transition<C, S> transition,
            int maxVisits,
            S maxVisitsFallback,
            StateMachineConfiguration<?, ?, ?, ?> childMachine,
            List<ParallelBranch<C>> parallelBranches,
            ContextRecoverer<C, ?> recoverer,
            BiFunction<C, ?, C> outputMerger,
            RetryPolicy retryPolicy,
            CompensationAction<C> compensationAction
    ) {}

    private OrchestrationStateMachineBuilder(
            @NonNull String machineName,
            @NonNull Class<ORCHESTRATION_STATE_KEY> stateKeyClass
    ) {
        super(stateKeyClass);
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
    }

    /**
     * Initializes a new fluent builder for defining an orchestration state machine.
     *
     * @param <ORCH_CTX>    The orchestration context type
     * @param <ORCH_KEY>    The orchestration state key enum type
     * @param <IN>          The input payload type
     * @param <OUT>         The output result type
     * @param machineName   The name of this orchestration state machine
     * @param stateKeyClass The state key enum class token
     * @return A new {@link OrchestrationStateMachineBuilder} instance
     */
    @NonNull
    public static <ORCH_CTX extends StateMachineContext,
            ORCH_KEY extends Enum<ORCH_KEY> & StateKey,
            IN,
            OUT>
    OrchestrationStateMachineBuilder<ORCH_CTX, ORCH_KEY, IN, OUT> create(
            @NonNull String machineName,
            @NonNull Class<ORCH_KEY> stateKeyClass
    ) {
        return new OrchestrationStateMachineBuilder<>(machineName, stateKeyClass);
    }

    /**
     * Attaches a listener callback that receives immutable checkpoint snapshots upon state transitions and failures.
     *
     * @param checkpointListener The checkpoint consumer callback
     * @return This builder instance for chaining
     */
    @NonNull
    public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> onCheckpoint(
            @Nullable Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> checkpointListener
    ) {
        this.checkpointListener = checkpointListener;
        return this;
    }

    /**
     * Begins defining a state step in the orchestration graph.
     *
     * @param stateKey The orchestration state key identifier
     * @return An {@link OrchestrationStateDefinitionBuilder} for configuring this step
     */
    @NonNull
    public OrchestrationStateDefinitionBuilder state(@NonNull ORCHESTRATION_STATE_KEY stateKey) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        return new OrchestrationStateDefinitionBuilder(stateKey);
    }

    /**
     * Builds and validates the complete directed orchestration graph and returns an immutable configuration model.
     *
     * @return An {@link OrchestrationStateMachineConfiguration} instance
     */
    @NonNull
    public OrchestrationStateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> build() {
        if (initialStateKey == null) {
            throw new IllegalStateException("Initial state must be specified before building orchestration state machine");
        }
        if (contextFactory == null) {
            throw new IllegalStateException("Context factory must be specified before building orchestration state machine");
        }

        Set<ORCHESTRATION_STATE_KEY> allRegisteredKeys = new HashSet<>(stateHolders.keySet());
        allRegisteredKeys.addAll(endStateKeys);

        StateMap.Builder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> stateMapBuilder =
                StateMap.<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>builder(stateKeyClass)
                        .initialState(initialStateKey);

        for (Map.Entry<ORCHESTRATION_STATE_KEY, OrchestrationStateHolder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> entry : stateHolders.entrySet()) {
            ORCHESTRATION_STATE_KEY key = entry.getKey();
            OrchestrationStateHolder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> holder = entry.getValue();

            Set<ORCHESTRATION_STATE_KEY> targets = (holder.explicitTargets != null)
                    ? holder.explicitTargets
                    : allRegisteredKeys;

            OrchestrationState<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> orchState =
                    OrchestrationState.of(
                            holder.action,
                            targets,
                            holder.transition,
                            false,
                            holder.maxVisits,
                            holder.maxVisitsFallback,
                            holder.childMachine,
                            holder.parallelBranches,
                            holder.recoverer,
                            holder.outputMerger,
                            holder.retryPolicy,
                            holder.compensationAction
                    );

            stateMapBuilder.addState(key, orchState);
        }

        for (ORCHESTRATION_STATE_KEY endKey : endStateKeys) {
            stateMapBuilder.endState(endKey);
        }

        StateMap<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> builtStateMap = stateMapBuilder.build();

        final StateMachineContextFactory<ORCHESTRATION_CONTEXT> finalContextFactory = this.contextFactory;
        final InputFunction<ORCHESTRATION_CONTEXT, INPUT> finalInputFunction = this.inputFunction;
        final OutputFunction<ORCHESTRATION_CONTEXT, OUTPUT> finalOutputFunction = this.outputFunction;
        final int finalMaxTransitions = this.maxTransitions;
        final String finalMachineName = this.machineName;
        final Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> finalCheckpointListener = this.checkpointListener;

        return new OrchestrationStateMachineConfiguration<>(finalMachineName, builtStateMap, finalMaxTransitions, finalCheckpointListener) {
            @NonNull
            @Override
            public StateMachineContextFactory<ORCHESTRATION_CONTEXT> stateMachineContextFactory() {
                return finalContextFactory;
            }

            @Nullable
            @Override
            public InputFunction<ORCHESTRATION_CONTEXT, INPUT> inputFunction() throws StateMachineException {
                return finalInputFunction;
            }

            @Nullable
            @Override
            public OutputFunction<ORCHESTRATION_CONTEXT, OUTPUT> outputFunction() throws StateMachineException {
                return finalOutputFunction;
            }
        };
    }

    /**
     * Builds and returns an active {@link OrchestrationStateMachineExecutor} ready for dispatch.
     *
     * @return An {@link OrchestrationStateMachineExecutor} instance
     */
    @NonNull
    public OrchestrationStateMachineExecutor<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> buildExecutor() {
        return new OrchestrationStateMachineExecutor<>(build());
    }

    /**
     * Builds and returns an active {@link OrchestrationStateMachineExecutor} with a custom admission controller.
     *
     * @param admissionController The admission controller
     * @return An {@link OrchestrationStateMachineExecutor} instance
     */
    @NonNull
    public OrchestrationStateMachineExecutor<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> buildExecutor(
            @NonNull AdmissionController admissionController
    ) {
        return new OrchestrationStateMachineExecutor<>(build(), admissionController);
    }

    public class OrchestrationStateDefinitionBuilder {
        private final ORCHESTRATION_STATE_KEY stateKey;
        private Action<ORCHESTRATION_CONTEXT> action;
        private int maxVisits = -1;
        private ORCHESTRATION_STATE_KEY maxVisitsFallback;
        private CompensationAction<ORCHESTRATION_CONTEXT> compensationAction = CompensationAction.noop();

        private OrchestrationStateDefinitionBuilder(@NonNull ORCHESTRATION_STATE_KEY stateKey) {
            this.stateKey = stateKey;
        }

        @NonNull
        public OrchestrationStateDefinitionBuilder action(@NonNull Action<ORCHESTRATION_CONTEXT> action) {
            this.action = Objects.requireNonNull(action, "action must not be null");
            return this;
        }

        /**
         * Configures this state to execute a child state machine (Atomic or Nested Orchestration).
         *
         * @param <CHILD_CTX>       Child context type
         * @param <CHILD_STATE_KEY> Child state key type
         * @param <CHILD_IN>        Child input type
         * @param <CHILD_OUT>       Child output type
         * @param childMachine      Child machine configuration
         * @return Sub-builder for child machine configuration
         */
        @NonNull
        public <CHILD_CTX extends StateMachineContext,
                CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
                CHILD_IN,
                CHILD_OUT>
        ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> childMachine(
                @NonNull StateMachineConfiguration<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> childMachine
        ) {
            return new ChildMachineStepBuilder<>(stateKey, childMachine, action, maxVisits, maxVisitsFallback, compensationAction);
        }

        /**
         * Alias for {@link #childMachine(StateMachineConfiguration)}.
         *
         * @param <CHILD_CTX>       Child context type
         * @param <CHILD_STATE_KEY> Child state key type
         * @param <CHILD_IN>        Child input type
         * @param <CHILD_OUT>       Child output type
         * @param atomicMachine     Child machine configuration
         * @return Sub-builder for child machine configuration
         */
        @NonNull
        public <CHILD_CTX extends StateMachineContext,
                CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
                CHILD_IN,
                CHILD_OUT>
        ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> atomicMachine(
                @NonNull StateMachineConfiguration<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> atomicMachine
        ) {
            return childMachine(atomicMachine);
        }

        /**
         * Begins configuring multiple independent branches to execute in parallel on virtual threads.
         *
         * @return Parallel step configuration builder
         */
        @NonNull
        public ParallelStepBuilder parallel() {
            return new ParallelStepBuilder(stateKey, maxVisits, maxVisitsFallback, compensationAction);
        }

        @NonNull
        public OrchestrationStateDefinitionBuilder compensate(
                @NonNull CompensationAction<ORCHESTRATION_CONTEXT> compensationAction
        ) {
            this.compensationAction = Objects.requireNonNull(compensationAction, "compensationAction must not be null");
            return this;
        }

        @NonNull
        public OrchestrationStateDefinitionBuilder maxVisits(int maxVisits) {
            this.maxVisits = maxVisits;
            return this;
        }

        @NonNull
        public OrchestrationStateDefinitionBuilder maxVisits(int maxVisits, @NonNull ORCHESTRATION_STATE_KEY fallback) {
            this.maxVisits = maxVisits;
            this.maxVisitsFallback = Objects.requireNonNull(fallback, "fallback state must not be null");
            return this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<ORCHESTRATION_STATE_KEY> permittedTargets,
                @NonNull Transition<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> transition
        ) {
            Objects.requireNonNull(permittedTargets, "permittedTargets must not be null");
            Objects.requireNonNull(transition, "transition must not be null");

            Action<ORCHESTRATION_CONTEXT> effAction = (this.action != null) ? this.action : Action.identity();

            stateHolders.put(stateKey, new OrchestrationStateHolder<>(
                    effAction,
                    permittedTargets,
                    transition,
                    this.maxVisits,
                    this.maxVisitsFallback,
                    null,
                    Collections.emptyList(),
                    null,
                    null,
                    RetryPolicy.noRetries(),
                    this.compensationAction
            ));
            return OrchestrationStateMachineBuilder.this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> transition
        ) {
            Objects.requireNonNull(transition, "transition must not be null");
            Action<ORCHESTRATION_CONTEXT> effAction = (this.action != null) ? this.action : Action.identity();

            stateHolders.put(stateKey, new OrchestrationStateHolder<>(
                    effAction,
                    null,
                    transition,
                    this.maxVisits,
                    this.maxVisitsFallback,
                    null,
                    Collections.emptyList(),
                    null,
                    null,
                    RetryPolicy.noRetries(),
                    this.compensationAction
            ));
            return OrchestrationStateMachineBuilder.this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull ORCHESTRATION_STATE_KEY nextState
        ) {
            return transitionsTo(Set.of(nextState), Transition.to(nextState));
        }
    }

    public class ChildMachineStepBuilder<
            CHILD_CTX extends StateMachineContext,
            CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
            CHILD_IN,
            CHILD_OUT> {

        private final ORCHESTRATION_STATE_KEY stateKey;
        private final StateMachineConfiguration<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> childMachine;
        private final Action<ORCHESTRATION_CONTEXT> action;
        private int maxVisits;
        private ORCHESTRATION_STATE_KEY maxVisitsFallback;
        private CompensationAction<ORCHESTRATION_CONTEXT> compensationAction;
        private ContextRecoverer<ORCHESTRATION_CONTEXT, CHILD_IN> recoverer;
        private BiFunction<ORCHESTRATION_CONTEXT, CHILD_OUT, ORCHESTRATION_CONTEXT> outputMerger;
        private RetryPolicy retryPolicy = RetryPolicy.noRetries();

        private ChildMachineStepBuilder(
                @NonNull ORCHESTRATION_STATE_KEY stateKey,
                @NonNull StateMachineConfiguration<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> childMachine,
                @Nullable Action<ORCHESTRATION_CONTEXT> action,
                int maxVisits,
                @Nullable ORCHESTRATION_STATE_KEY maxVisitsFallback,
                @NonNull CompensationAction<ORCHESTRATION_CONTEXT> compensationAction
        ) {
            this.stateKey = stateKey;
            this.childMachine = childMachine;
            this.action = action;
            this.maxVisits = maxVisits;
            this.maxVisitsFallback = maxVisitsFallback;
            this.compensationAction = compensationAction;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> input(
                @NonNull Function<ORCHESTRATION_CONTEXT, CHILD_IN> inputMapper
        ) {
            Objects.requireNonNull(inputMapper, "inputMapper must not be null");
            this.recoverer = (ctx, err, attempt) -> inputMapper.apply(ctx);
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> recoverer(
                @NonNull ContextRecoverer<ORCHESTRATION_CONTEXT, CHILD_IN> recoverer
        ) {
            this.recoverer = Objects.requireNonNull(recoverer, "recoverer must not be null");
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> output(
                @NonNull BiFunction<ORCHESTRATION_CONTEXT, CHILD_OUT, ORCHESTRATION_CONTEXT> outputMerger
        ) {
            this.outputMerger = Objects.requireNonNull(outputMerger, "outputMerger must not be null");
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> retry(@NonNull RetryPolicy retryPolicy) {
            this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> retry(int maxAttempts, @NonNull Duration delay) {
            this.retryPolicy = RetryPolicy.fixed(maxAttempts, delay);
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> compensate(
                @NonNull CompensationAction<ORCHESTRATION_CONTEXT> compensationAction
        ) {
            this.compensationAction = Objects.requireNonNull(compensationAction, "compensationAction must not be null");
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> maxVisits(int maxVisits) {
            this.maxVisits = maxVisits;
            return this;
        }

        @NonNull
        public ChildMachineStepBuilder<CHILD_CTX, CHILD_STATE_KEY, CHILD_IN, CHILD_OUT> maxVisits(int maxVisits, @NonNull ORCHESTRATION_STATE_KEY fallback) {
            this.maxVisits = maxVisits;
            this.maxVisitsFallback = Objects.requireNonNull(fallback, "fallback must not be null");
            return this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<ORCHESTRATION_STATE_KEY> permittedTargets,
                @NonNull Transition<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> transition
        ) {
            Objects.requireNonNull(permittedTargets, "permittedTargets must not be null");
            Objects.requireNonNull(transition, "transition must not be null");

            Action<ORCHESTRATION_CONTEXT> effAction = (this.action != null) ? this.action : Action.identity();

            stateHolders.put(stateKey, new OrchestrationStateHolder<>(
                    effAction,
                    permittedTargets,
                    transition,
                    this.maxVisits,
                    this.maxVisitsFallback,
                    this.childMachine,
                    Collections.emptyList(),
                    this.recoverer,
                    this.outputMerger,
                    this.retryPolicy,
                    this.compensationAction
            ));
            return OrchestrationStateMachineBuilder.this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> transition
        ) {
            Objects.requireNonNull(transition, "transition must not be null");
            Action<ORCHESTRATION_CONTEXT> effAction = (this.action != null) ? this.action : Action.identity();

            stateHolders.put(stateKey, new OrchestrationStateHolder<>(
                    effAction,
                    null,
                    transition,
                    this.maxVisits,
                    this.maxVisitsFallback,
                    this.childMachine,
                    Collections.emptyList(),
                    this.recoverer,
                    this.outputMerger,
                    this.retryPolicy,
                    this.compensationAction
            ));
            return OrchestrationStateMachineBuilder.this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull ORCHESTRATION_STATE_KEY nextState
        ) {
            return transitionsTo(Set.of(nextState), Transition.to(nextState));
        }
    }

    public class ParallelStepBuilder {
        private final ORCHESTRATION_STATE_KEY stateKey;
        private final List<ParallelBranch<ORCHESTRATION_CONTEXT>> branches = new ArrayList<>();
        private int maxVisits = -1;
        private ORCHESTRATION_STATE_KEY maxVisitsFallback;
        private CompensationAction<ORCHESTRATION_CONTEXT> compensationAction;

        private ParallelStepBuilder(
                @NonNull ORCHESTRATION_STATE_KEY stateKey,
                int maxVisits,
                @Nullable ORCHESTRATION_STATE_KEY maxVisitsFallback,
                @NonNull CompensationAction<ORCHESTRATION_CONTEXT> compensationAction
        ) {
            this.stateKey = stateKey;
            this.maxVisits = maxVisits;
            this.maxVisitsFallback = maxVisitsFallback;
            this.compensationAction = compensationAction;
        }

        @NonNull
        public ParallelStepBuilder branch(
                @NonNull String name,
                @NonNull Action<ORCHESTRATION_CONTEXT> action
        ) {
            this.branches.add(ParallelBranch.of(name, action, null));
            return this;
        }

        @NonNull
        public ParallelStepBuilder branch(
                @NonNull String name,
                @NonNull Action<ORCHESTRATION_CONTEXT> action,
                @Nullable CompensationAction<ORCHESTRATION_CONTEXT> compensation
        ) {
            this.branches.add(ParallelBranch.of(name, action, compensation));
            return this;
        }

        @NonNull
        public ParallelStepBuilder branch(@NonNull ParallelBranch<ORCHESTRATION_CONTEXT> branch) {
            this.branches.add(Objects.requireNonNull(branch, "branch must not be null"));
            return this;
        }

        @NonNull
        public ParallelStepBuilder compensate(@NonNull CompensationAction<ORCHESTRATION_CONTEXT> compensationAction) {
            this.compensationAction = Objects.requireNonNull(compensationAction, "compensationAction must not be null");
            return this;
        }

        @NonNull
        public ParallelStepBuilder maxVisits(int maxVisits) {
            this.maxVisits = maxVisits;
            return this;
        }

        @NonNull
        public ParallelStepBuilder maxVisits(int maxVisits, @NonNull ORCHESTRATION_STATE_KEY fallback) {
            this.maxVisits = maxVisits;
            this.maxVisitsFallback = Objects.requireNonNull(fallback, "fallback must not be null");
            return this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transitionsTo(
                @NonNull Set<ORCHESTRATION_STATE_KEY> permittedTargets,
                @NonNull Transition<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> transition
        ) {
            Objects.requireNonNull(permittedTargets, "permittedTargets must not be null");
            Objects.requireNonNull(transition, "transition must not be null");

            stateHolders.put(stateKey, new OrchestrationStateHolder<>(
                    Action.identity(),
                    permittedTargets,
                    transition,
                    this.maxVisits,
                    this.maxVisitsFallback,
                    null,
                    List.copyOf(this.branches),
                    null,
                    null,
                    RetryPolicy.noRetries(),
                    this.compensationAction
            ));
            return OrchestrationStateMachineBuilder.this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull Transition<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> transition
        ) {
            Objects.requireNonNull(transition, "transition must not be null");
            stateHolders.put(stateKey, new OrchestrationStateHolder<>(
                    Action.identity(),
                    null,
                    transition,
                    this.maxVisits,
                    this.maxVisitsFallback,
                    null,
                    List.copyOf(this.branches),
                    null,
                    null,
                    RetryPolicy.noRetries(),
                    this.compensationAction
            ));
            return OrchestrationStateMachineBuilder.this;
        }

        @NonNull
        public OrchestrationStateMachineBuilder<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> transition(
                @NonNull ORCHESTRATION_STATE_KEY nextState
        ) {
            return transitionsTo(Set.of(nextState), Transition.to(nextState));
        }
    }
}
