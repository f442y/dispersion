package com.github.f442y.dispersion.event.signal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an incoming signal arrives but cannot be delivered.
 */
public record SignalDiscardedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String signalName,
        @Nullable String correlationKey,
        @NonNull String reason,
        @NonNull Instant timestamp
) implements SignalEvent {
    public SignalDiscardedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
