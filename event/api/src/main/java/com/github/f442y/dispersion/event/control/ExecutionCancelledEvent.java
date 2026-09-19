package com.github.f442y.dispersion.event.control;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an operator or control plane cancels an active execution.
 */
public record ExecutionCancelledEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @Nullable String stateName,
        @NonNull String operatorId,
        @NonNull String reason,
        @NonNull Instant timestamp
) implements ControlPlaneEvent {
    public ExecutionCancelledEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(operatorId, "operatorId must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
