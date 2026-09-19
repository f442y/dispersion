package com.github.f442y.dispersion.event.guard;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a state's loop visit count exceeds maxVisits.
 */
public record StateVisitLimitExceededEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        int visitLimit,
        @Nullable String fallbackState,
        @NonNull Instant timestamp
) implements ExecutionGuardEvent {
    public StateVisitLimitExceededEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
