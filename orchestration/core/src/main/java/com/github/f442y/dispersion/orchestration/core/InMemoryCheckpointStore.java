package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.orchestration.*;
import com.github.f442y.dispersion.orchestration.batch.*;
import com.github.f442y.dispersion.orchestration.command.*;
import com.github.f442y.dispersion.orchestration.messaging.*;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import org.jspecify.annotations.NonNull;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory implementation of {@link CheckpointStore} with secondary correlation index.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 */
public class InMemoryCheckpointStore<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey> implements CheckpointStore<CONTEXT, STATE_KEY> {

    private final Map<UUID, OrchestrationCheckpoint<CONTEXT, STATE_KEY>> store = new ConcurrentHashMap<>();
    private final Map<String, UUID> correlationIndex = new ConcurrentHashMap<>();

    @Override
    public void save(@NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        store.put(checkpoint.machineId(), checkpoint);
        if (checkpoint.correlationKey() != null && !checkpoint.correlationKey().isBlank()) {
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
        UUID machineId = correlationIndex.get(correlationKey);
        if (machineId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(store.get(machineId));
    }

    @Override
    public void delete(@NonNull UUID machineId) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        OrchestrationCheckpoint<CONTEXT, STATE_KEY> removed = store.remove(machineId);
        if (removed != null && removed.correlationKey() != null) {
            correlationIndex.remove(removed.correlationKey());
        }
    }

    public void clear() {
        store.clear();
        correlationIndex.clear();
    }

    public int size() {
        return store.size();
    }
}
