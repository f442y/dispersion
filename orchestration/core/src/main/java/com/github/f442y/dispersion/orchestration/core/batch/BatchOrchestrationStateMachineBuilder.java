package com.github.f442y.dispersion.orchestration.core.batch;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.Action;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.Transition;
import com.github.f442y.dispersion.orchestration.CompensationAction;
import com.github.f442y.dispersion.orchestration.SignalHandler;
import com.github.f442y.dispersion.orchestration.batch.BarrierPolicy;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.batch.BatchOrchestrationStepDriver.BatchConfiguration;
import com.github.f442y.dispersion.orchestration.core.batch.BatchOrchestrationStepDriver.ItemStateDefinition;
import org.jspecify.annotations.NonNull;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Fluent builder for creating Set/Batch Orchestration state machines with independent streaming and barrier states.
 *
 * @param <BATCH_CONTEXT> The batch-level context type
 * @param <ITEM_CONTEXT>  The item-level context type
 * @param <STATE_KEY>     The state key enum type
 * @param <OUTPUT>        The output type
 */
public class BatchOrchestrationStateMachineBuilder<
        BATCH_CONTEXT extends StateMachineContext,
        ITEM_CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        OUTPUT> {

    private final BatchConfiguration<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> configuration = new BatchConfiguration<>();
    private Supplier<BATCH_CONTEXT> batchContextSupplier;

    private BatchOrchestrationStateMachineBuilder(@NonNull String batchName, @NonNull Class<STATE_KEY> stateKeyClass) {
        this.configuration.batchName = Objects.requireNonNull(batchName, "batchName must not be null");
        Objects.requireNonNull(stateKeyClass, "stateKeyClass must not be null");
    }

    public static <
            BATCH_CONTEXT extends StateMachineContext,
            ITEM_CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            OUTPUT>
    BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> create(
            @NonNull String batchName,
            @NonNull Class<STATE_KEY> stateKeyClass
    ) {
        return new BatchOrchestrationStateMachineBuilder<>(batchName, stateKeyClass);
    }

    @NonNull
    public BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> batchContext(
            @NonNull Supplier<BATCH_CONTEXT> batchContextSupplier
    ) {
        this.batchContextSupplier = Objects.requireNonNull(batchContextSupplier, "batchContextSupplier must not be null");
        return this;
    }

    @NonNull
    public BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> batchKey(
            @NonNull Function<BATCH_CONTEXT, String> batchKeyExtractor
    ) {
        this.configuration.batchKeyExtractor = Objects.requireNonNull(batchKeyExtractor, "batchKeyExtractor must not be null");
        return this;
    }

    @NonNull
    public BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> itemKey(
            @NonNull Function<ITEM_CONTEXT, String> itemKeyExtractor
    ) {
        this.configuration.itemKeyExtractor = Objects.requireNonNull(itemKeyExtractor, "itemKeyExtractor must not be null");
        return this;
    }

    @NonNull
    public BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> initialState(
            @NonNull STATE_KEY initialStateKey
    ) {
        this.configuration.initialStateKey = Objects.requireNonNull(initialStateKey, "initialStateKey must not be null");
        return this;
    }

    @NonNull
    @SafeVarargs
    public final BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> endStates(
            @NonNull STATE_KEY... endStates
    ) {
        this.configuration.endStates.addAll(Arrays.asList(endStates));
        return this;
    }

    @NonNull
    public BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> output(
            @NonNull Function<BATCH_CONTEXT, OUTPUT> outputFunction
    ) {
        this.configuration.outputFunction = Objects.requireNonNull(outputFunction, "outputFunction must not be null");
        return this;
    }

    @NonNull
    public ItemStateBuilder itemState(@NonNull STATE_KEY stateKey) {
        Objects.requireNonNull(stateKey, "stateKey must not be null");
        return new ItemStateBuilder(stateKey);
    }

    public class ItemStateBuilder {
        private final STATE_KEY stateKey;
        private final ItemStateDefinition<ITEM_CONTEXT, STATE_KEY> def = new ItemStateDefinition<>();

        private ItemStateBuilder(@NonNull STATE_KEY stateKey) {
            this.stateKey = stateKey;
        }

        @NonNull
        public ItemStateBuilder action(@NonNull Action<ITEM_CONTEXT> action) {
            this.def.action = Objects.requireNonNull(action, "action must not be null");
            return this;
        }

        @NonNull
        public ItemStateBuilder compensate(@NonNull CompensationAction<ITEM_CONTEXT> compensation) {
            this.def.compensationAction = Objects.requireNonNull(compensation, "compensation must not be null");
            return this;
        }

        @NonNull
        public ItemStateBuilder barrier(@NonNull BarrierPolicy policy) {
            this.def.isBarrier = true;
            this.def.barrierPolicy = Objects.requireNonNull(policy, "policy must not be null");
            return this;
        }

        @NonNull
        public ItemStateBuilder waitForSignal(
                @NonNull String signalName,
                @NonNull SignalHandler<ITEM_CONTEXT, ?> handler
        ) {
            this.def.expectedSignal = Objects.requireNonNull(signalName, "signalName must not be null");
            this.def.signalHandler = Objects.requireNonNull(handler, "handler must not be null");
            return this;
        }

        @NonNull
        @SuppressWarnings("unchecked")
        public <CMD extends SignalCommand> ItemStateBuilder waitForCommand(
                @NonNull Class<CMD> commandClass,
                @NonNull SignalHandler<ITEM_CONTEXT, CMD> handler
        ) {
            Objects.requireNonNull(commandClass, "commandClass must not be null");
            Objects.requireNonNull(handler, "handler must not be null");
            this.def.expectedSignal = commandClass.getSimpleName();
            this.def.signalHandler = (ctx, payload) -> handler.handleSignal(ctx, (CMD) payload);
            return this;
        }

        @NonNull
        public BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> transition(
                @NonNull STATE_KEY nextState
        ) {
            this.def.transition = Transition.to(nextState);
            configuration.stateDefinitions.put(stateKey, def);
            return BatchOrchestrationStateMachineBuilder.this;
        }

        @NonNull
        public BatchOrchestrationStateMachineBuilder<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> transition(
                @NonNull Transition<ITEM_CONTEXT, STATE_KEY> transition
        ) {
            this.def.transition = Objects.requireNonNull(transition, "transition must not be null");
            configuration.stateDefinitions.put(stateKey, def);
            return BatchOrchestrationStateMachineBuilder.this;
        }
    }

    @NonNull
    public BatchOrchestrationExecutor<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> buildExecutor() {
        if (batchContextSupplier == null) {
            throw new IllegalStateException("batchContext supplier must be specified");
        }
        if (configuration.itemKeyExtractor == null) {
            throw new IllegalStateException("itemKey extractor must be specified");
        }
        return new BatchOrchestrationExecutor<>(configuration, batchContextSupplier);
    }
}
