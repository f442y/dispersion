package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Specialized configuration model for Orchestration State Machines, holding the machine name,
 * directed graph state map, and checkpoint snapshot listeners.
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

    public OrchestrationStateMachineConfiguration(
            @NonNull String machineName,
            @NonNull StateMap<CONTEXT, STATE_KEY> stateMap,
            int maxTransitions,
            @Nullable Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> checkpointListener
    ) {
        super(stateMap, maxTransitions);
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
        this.checkpointListener = checkpointListener;
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
}
