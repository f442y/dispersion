package com.github.f442y.dispersion.orchestration.batch;

import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted in batch orchestrations when a barrier unlocks and all items proceed.
 */
public record BatchBarrierUnlockedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        int itemCount,
        @NonNull Instant timestamp
) implements ExecutionEvent {
    public BatchBarrierUnlockedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
