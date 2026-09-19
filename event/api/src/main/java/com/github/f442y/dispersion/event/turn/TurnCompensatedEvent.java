package com.github.f442y.dispersion.event.turn;

import com.github.f442y.dispersion.event.compensation.CompensationEvent;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a turn fails and automated LIFO Saga compensations are executed.
 */
public record TurnCompensatedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String failedStateName,
        @NonNull List<String> compensatedStates,
        @Nullable Throwable cause,
        @Nullable String correlationKey,
        @NonNull Duration duration,
        @NonNull Instant timestamp
) implements TurnLifecycleEvent, CompensationEvent {
    public TurnCompensatedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(failedStateName, "failedStateName must not be null");
        Objects.requireNonNull(compensatedStates, "compensatedStates must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        compensatedStates = List.copyOf(compensatedStates);
    }
}
