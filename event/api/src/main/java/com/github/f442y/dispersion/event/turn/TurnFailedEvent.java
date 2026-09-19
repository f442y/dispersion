package com.github.f442y.dispersion.event.turn;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a turn fails due to an unhandled exception without Saga compensation.
 */
public record TurnFailedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String failedStateName,
        @NonNull Throwable cause,
        @Nullable String correlationKey,
        @NonNull Duration duration,
        @NonNull Instant timestamp
) implements TurnLifecycleEvent {
    public TurnFailedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(failedStateName, "failedStateName must not be null");
        Objects.requireNonNull(cause, "cause must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
