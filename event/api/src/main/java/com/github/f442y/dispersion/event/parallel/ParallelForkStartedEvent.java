package com.github.f442y.dispersion.event.parallel;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a parallel state forks concurrent execution branches on virtual threads.
 */
public record ParallelForkStartedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull List<String> branchNames,
        @NonNull Instant timestamp
) implements ParallelExecutionEvent {
    public ParallelForkStartedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(branchNames, "branchNames must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        branchNames = List.copyOf(branchNames);
    }
}
