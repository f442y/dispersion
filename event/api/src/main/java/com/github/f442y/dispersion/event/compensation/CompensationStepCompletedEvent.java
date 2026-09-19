package com.github.f442y.dispersion.event.compensation;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted immediately after a single state's compensation rollback action completes successfully.
 */
public record CompensationStepCompletedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull Duration duration,
        @NonNull Instant timestamp
) implements CompensationEvent {
    public CompensationStepCompletedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
