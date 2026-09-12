package com.github.f442y.dispersion.orchestration.test;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.CheckpointStore;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import org.jspecify.annotations.NonNull;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe, in-memory {@link CheckpointStore} test double that tracks save invocations and checkpoints.
 */
public class RecordingCheckpointStore<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey>
        implements CheckpointStore<CONTEXT, STATE_KEY> {

    private final Map<UUID, OrchestrationCheckpoint<CONTEXT, STATE_KEY>> store = new ConcurrentHashMap<>();
    private final Map<String, UUID> correlationIndex = new ConcurrentHashMap<>();
    private final AtomicInteger saveCount = new AtomicInteger(0);

    @Override
    public void save(@NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        store.put(checkpoint.machineId(), checkpoint);
        if (checkpoint.correlationKey() != null) {
            correlationIndex.put(checkpoint.correlationKey(), checkpoint.machineId());
        }
        saveCount.incrementAndGet();
    }

    @Override
    @NonNull
    public Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> findById(@NonNull UUID machineId) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        return Optional.ofNullable(store.get(machineId));
    }

    @Override
    @NonNull
    public Optional<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> findByCorrelationKey(@NonNull String correlationKey) {
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        UUID id = correlationIndex.get(correlationKey);
        if (id == null) {
            return Optional.empty();
        }
        return findById(id);
    }

    @Override
    public void delete(@NonNull UUID machineId) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        OrchestrationCheckpoint<CONTEXT, STATE_KEY> removed = store.remove(machineId);
        if (removed != null && removed.correlationKey() != null) {
            correlationIndex.remove(removed.correlationKey());
        }
    }

    public int saveCount() {
        return saveCount.get();
    }

    public int size() {
        return store.size();
    }

    public void clear() {
        store.clear();
        correlationIndex.clear();
        saveCount.set(0);
    }
}
