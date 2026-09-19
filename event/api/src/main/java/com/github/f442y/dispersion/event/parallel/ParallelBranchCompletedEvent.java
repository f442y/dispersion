package com.github.f442y.dispersion.event.parallel;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an individual parallel branch finishes its action.
 */
public record ParallelBranchCompletedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull String branchName,
        @NonNull Duration duration,
        @NonNull Instant timestamp
) implements ParallelExecutionEvent {
    public ParallelBranchCompletedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(branchName, "branchName must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
