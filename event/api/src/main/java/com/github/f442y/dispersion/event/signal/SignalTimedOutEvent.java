package com.github.f442y.dispersion.event.signal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a workflow halts waiting for an external signal and the deadline expires.
 */
public record SignalTimedOutEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull String expectedSignal,
        @Nullable String correlationKey,
        @NonNull Duration timeout,
        @NonNull Instant timestamp
) implements SignalEvent {
    public SignalTimedOutEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(expectedSignal, "expectedSignal must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
