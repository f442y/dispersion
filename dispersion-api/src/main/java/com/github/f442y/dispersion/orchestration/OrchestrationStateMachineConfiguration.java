package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.context.StateMachineContextFactory;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
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

    private final String machineName;
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
        super(stateMap, maxTransitions, contextFactory, inputFunction, outputFunction, exceptionTrigger, finishTrigger);
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
        this.checkpointListener = checkpointListener;
        this.correlationKeyExtractor = correlationKeyExtractor;
        this.checkpointStore = checkpointStore;
    }

    @NonNull
    public String getMachineName() {
        return machineName;
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
