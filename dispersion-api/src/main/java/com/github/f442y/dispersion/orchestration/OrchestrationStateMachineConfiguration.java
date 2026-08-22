package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Specialized configuration model for Orchestration State Machines, holding the machine name,
 * directed graph state map, checkpoint snapshot listeners, correlation key extractor, and checkpoint store.
 *
 * @param <CONTEXT>   The orchestration context type
 * @param <STATE_KEY> The orchestration state key enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output result type
 */
public abstract class OrchestrationStateMachineConfiguration<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        extends StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

    @NonNull
    private final String machineName;

    @Nullable
    private final Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener;

    @Nullable
    private final Function<CONTEXT, String> correlationKeyExtractor;

    @Nullable
    private final CheckpointStore<CONTEXT, STATE_KEY> checkpointStore;

    public OrchestrationStateMachineConfiguration(
            @NonNull String machineName,
            @NonNull StateMap<CONTEXT, STATE_KEY> stateMap,
            int maxTransitions,
            @Nullable Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener
    ) {
        this(machineName, stateMap, maxTransitions, checkpointListener, null, null);
    }

    public OrchestrationStateMachineConfiguration(
            @NonNull String machineName,
            @NonNull StateMap<CONTEXT, STATE_KEY> stateMap,
            int maxTransitions,
            @Nullable Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener,
            @Nullable Function<CONTEXT, String> correlationKeyExtractor,
            @Nullable CheckpointStore<CONTEXT, STATE_KEY> checkpointStore
    ) {
        super(stateMap, maxTransitions);
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
        this.checkpointListener = checkpointListener;
        this.correlationKeyExtractor = correlationKeyExtractor;
        this.checkpointStore = checkpointStore;
    }

    /**
     * Returns the name identifier of this orchestration state machine.
     *
     * @return The machine name
     */
    @NonNull
    public String machineName() {
        return machineName;
    }

    /**
     * Returns the checkpoint listener attached to this orchestration flow.
     *
     * @return The checkpoint consumer, or {@code null}
     */
    @Nullable
    public Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener() {
        return checkpointListener;
    }

    /**
     * Returns the function used to extract the business correlation key from the context.
     *
     * @return The correlation key extractor, or {@code null}
     */
    @Nullable
    public Function<CONTEXT, String> correlationKeyExtractor() {
        return correlationKeyExtractor;
    }

    /**
     * Returns the configured checkpoint store for persistence and rehydration.
     *
     * @return The checkpoint store, or {@code null}
     */
    @Nullable
    public CheckpointStore<CONTEXT, STATE_KEY> checkpointStore() {
        return checkpointStore;
    }
}
