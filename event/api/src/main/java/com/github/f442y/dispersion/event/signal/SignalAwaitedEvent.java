package com.github.f442y.dispersion.event.signal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a workflow halts at a signal wait state expecting an external event.
 */
public record SignalAwaitedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull String expectedSignal,
        @Nullable String correlationKey,
        @NonNull Instant timestamp
) implements SignalEvent {
    public SignalAwaitedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(expectedSignal, "expectedSignal must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
