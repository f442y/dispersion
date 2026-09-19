package com.github.f442y.dispersion.event.turn;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an orchestration or state machine turn begins execution.
 */
public record TurnStartedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @Nullable String correlationKey,
        @NonNull Instant timestamp
) implements TurnLifecycleEvent {
    public TurnStartedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
