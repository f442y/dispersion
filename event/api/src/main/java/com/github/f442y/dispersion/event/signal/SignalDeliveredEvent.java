package com.github.f442y.dispersion.event.signal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an incoming signal arrives and is dispatched to the waiting state machine.
 */
public record SignalDeliveredEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull String signalName,
        @Nullable String correlationKey,
        @NonNull Instant timestamp
) implements SignalEvent {
    public SignalDeliveredEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
