package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;

import java.util.Optional;
import java.util.UUID;

/**
 * Service Provider Interface (SPI) for persisting and retrieving {@link OrchestrationCheckpoint} snapshots
 * across dehydration, suspension, and rehydration lifecycles.
 *
 * @param <CONTEXT>   The concrete context type
 * @param <STATE_KEY> The state key enum type
 */
public interface CheckpointStore<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey> {

    /**
     * Persists or updates the specified orchestration checkpoint snapshot.
     *
     * @param checkpoint The checkpoint snapshot to save
     */
    void save(@NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint);

    /**
     * Retrieves an orchestration checkpoint snapshot by its unique machine UUID.
     *
     * @param machineId The state machine UUID
     * @return Optional containing the checkpoint if found; otherwise empty
     */
    @NonNull
    Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> findById(@NonNull UUID machineId);

    /**
     * Retrieves an orchestration checkpoint snapshot by its business correlation key.
     *
     * @param correlationKey The correlation identifier (e.g. order ID, payment ID)
     * @return Optional containing the checkpoint if found; otherwise empty
     */
    @NonNull
    Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> findByCorrelationKey(@NonNull String correlationKey);

    /**
     * Deletes a checkpoint snapshot from storage upon terminal workflow completion.
     *
     * @param machineId The unique machine UUID to remove
     */
    void delete(@NonNull UUID machineId);
}
