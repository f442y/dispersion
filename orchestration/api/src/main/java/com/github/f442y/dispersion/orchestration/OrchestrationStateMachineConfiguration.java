package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.fsm.config.InputFunction;
import com.github.f442y.dispersion.fsm.config.OutputFunction;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.context.StateMachineContextFactory;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Immutable configuration for an Orchestration State Machine.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 * @param <INPUT>     The input type
 * @param <OUTPUT>    The output type
 */
public class OrchestrationStateMachineConfiguration<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> extends StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

    private final Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener;
    private final Function<CONTEXT, String> correlationKeyExtractor;
    private final CheckpointStore<CONTEXT, STATE_KEY> checkpointStore;

    public OrchestrationStateMachineConfiguration(
            @NonNull String machineName,
            @NonNull StateMap<CONTEXT, STATE_KEY> stateMap,
            int maxTransitions,
            @Nullable StateMachineContextFactory<CONTEXT> contextFactory,
            @Nullable InputFunction<CONTEXT, INPUT> inputFunction,
            @Nullable OutputFunction<CONTEXT, OUTPUT> outputFunction,
            @Nullable BiConsumer<CONTEXT, Throwable> exceptionTrigger,
            @Nullable Consumer<CONTEXT> finishTrigger,
            @Nullable Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener,
            @Nullable Function<CONTEXT, String> correlationKeyExtractor,
            @Nullable CheckpointStore<CONTEXT, STATE_KEY> checkpointStore
    ) {
        this(machineName, stateMap, maxTransitions, contextFactory, inputFunction, outputFunction,
                exceptionTrigger, finishTrigger, checkpointListener, correlationKeyExtractor, checkpointStore, null);
    }

    public OrchestrationStateMachineConfiguration(
            @NonNull String machineName,
            @NonNull StateMap<CONTEXT, STATE_KEY> stateMap,
            int maxTransitions,
            @Nullable StateMachineContextFactory<CONTEXT> contextFactory,
            @Nullable InputFunction<CONTEXT, INPUT> inputFunction,
            @Nullable OutputFunction<CONTEXT, OUTPUT> outputFunction,
            @Nullable BiConsumer<CONTEXT, Throwable> exceptionTrigger,
            @Nullable Consumer<CONTEXT> finishTrigger,
            @Nullable Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener,
            @Nullable Function<CONTEXT, String> correlationKeyExtractor,
            @Nullable CheckpointStore<CONTEXT, STATE_KEY> checkpointStore,
            @Nullable ExecutionEventListener eventListener
    ) {
        super(Objects.requireNonNull(machineName, "machineName must not be null"),
                stateMap, maxTransitions, contextFactory, inputFunction, outputFunction,
                exceptionTrigger, finishTrigger, eventListener);
        this.checkpointListener = checkpointListener;
        this.correlationKeyExtractor = correlationKeyExtractor;
        this.checkpointStore = checkpointStore;
    }

    @Nullable
    public Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> getCheckpointListener() {
        return checkpointListener;
    }

    @Nullable
    public Function<CONTEXT, String> getCorrelationKeyExtractor() {
        return correlationKeyExtractor;
    }

    @Nullable
    public CheckpointStore<CONTEXT, STATE_KEY> getCheckpointStore() {
        return checkpointStore;
    }
}
