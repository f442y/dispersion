package com.github.f442y.dispersion.orchestration.batch;

import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted in batch orchestrations when an item reaches a synchronization barrier.
 */
public record BatchBarrierReachedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String itemKey,
        @NonNull String stateName,
        @NonNull Instant timestamp
) implements ExecutionEvent {
    public BatchBarrierReachedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(itemKey, "itemKey must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
