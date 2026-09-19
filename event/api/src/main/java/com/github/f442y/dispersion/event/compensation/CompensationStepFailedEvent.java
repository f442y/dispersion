package com.github.f442y.dispersion.event.compensation;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an individual compensation step fails.
 */
public record CompensationStepFailedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull Throwable cause,
        @NonNull Instant timestamp
) implements CompensationEvent {
    public CompensationStepFailedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(cause, "cause must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
