package com.github.f442y.dispersion.event.turn;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an execution reaches a terminal end state and completes successfully.
 */
public record TurnCompletedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @Nullable String finalStateName,
        @Nullable String correlationKey,
        @NonNull Duration duration,
        @NonNull Instant timestamp
) implements TurnLifecycleEvent {
    public TurnCompletedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
