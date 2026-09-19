package com.github.f442y.dispersion.event.turn;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a turn suspends execution (e.g. waiting for an inbound signal or human input).
 */
public record TurnSuspendedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @Nullable String expectedSignal,
        @Nullable String correlationKey,
        @NonNull Duration duration,
        @NonNull Instant timestamp
) implements TurnLifecycleEvent {
    public TurnSuspendedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
