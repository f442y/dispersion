package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;

import java.util.Optional;
import java.util.UUID;

/**
 * Pluggable storage SPI for persisting and rehydrating orchestration checkpoints across turn boundaries.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 */
public interface CheckpointStore<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey> {

    /**
     * Persists or updates the given orchestration checkpoint.
     *
     * @param checkpoint The checkpoint snapshot to save
     */
    void save(@NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint);

    /**
     * Finds a persisted checkpoint by the unique machine ID.
     *
     * @param machineId The state machine UUID
     * @return Optional containing the checkpoint if found
     */
    @NonNull
    Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> findById(@NonNull UUID machineId);

    /**
     * Finds a persisted checkpoint by a domain correlation key.
     *
     * @param correlationKey The domain correlation key
     * @return Optional containing the checkpoint if found
     */
    @NonNull
    Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> findByCorrelationKey(@NonNull String correlationKey);

    /**
     * Deletes the checkpoint corresponding to the given machine ID.
     *
     * @param machineId The state machine UUID to delete
     */
    void delete(@NonNull UUID machineId);
}
