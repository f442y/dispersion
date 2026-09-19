package com.github.f442y.dispersion.event.control;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an execution is paused by an operator or control plane.
 */
public record ExecutionPausedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull String reason,
        @NonNull Instant timestamp
) implements ControlPlaneEvent {
    public ExecutionPausedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
