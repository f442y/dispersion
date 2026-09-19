package com.github.f442y.dispersion.event.control;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a paused execution is resumed.
 */
public record ExecutionResumedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull Instant timestamp
) implements ControlPlaneEvent {
    public ExecutionResumedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
