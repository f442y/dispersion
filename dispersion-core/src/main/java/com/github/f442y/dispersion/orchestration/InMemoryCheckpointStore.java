package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory implementation of {@link CheckpointStore} using {@link ConcurrentHashMap}
 * with secondary indexing for correlation keys.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 */
public class InMemoryCheckpointStore<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey>
        implements CheckpointStore<CONTEXT, STATE_KEY> {

    private final Map<UUID, OrchestrationCheckpoint<CONTEXT, STATE_KEY>> store = new ConcurrentHashMap<>();
    private final Map<String, UUID> correlationIndex = new ConcurrentHashMap<>();

    @Override
    public void save(@NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        store.put(checkpoint.machineId(), checkpoint);
        if (checkpoint.correlationKey() != null) {
            correlationIndex.put(checkpoint.correlationKey(), checkpoint.machineId());
        }
    }

    @NonNull
    @Override
    public Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> findById(@NonNull UUID machineId) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        return Optional.ofNullable(store.get(machineId));
    }

    @NonNull
    @Override
    public Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> findByCorrelationKey(@NonNull String correlationKey) {
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        UUID id = correlationIndex.get(correlationKey);
        if (id == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(store.get(id));
    }

    @Override
    public void delete(@NonNull UUID machineId) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        OrchestrationCheckpoint<CONTEXT, STATE_KEY> removed = store.remove(machineId);
        if (removed != null && removed.correlationKey() != null) {
            correlationIndex.remove(removed.correlationKey());
        }
    }

    /**
     * Clears all entries from the store.
     */
    public void clear() {
        store.clear();
        correlationIndex.clear();
    }

    /**
     * Returns the current number of checkpoints in the store.
     *
     * @return The count of stored checkpoints
     */
    public int size() {
        return store.size();
    }
}
