package com.github.f442y.dispersion.event.parallel;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when all parallel branches join and reduction completes.
 */
public record ParallelJoinCompletedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        int totalBranches,
        @NonNull Duration duration,
        @NonNull Instant timestamp
) implements ParallelExecutionEvent {
    public ParallelJoinCompletedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
