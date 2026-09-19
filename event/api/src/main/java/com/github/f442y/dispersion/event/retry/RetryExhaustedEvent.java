package com.github.f442y.dispersion.event.retry;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when all configured retry attempts have failed.
 */
public record RetryExhaustedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        int attempts,
        @NonNull Throwable finalCause,
        @NonNull Instant timestamp
) implements RetryLifecycleEvent {
    public RetryExhaustedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(finalCause, "finalCause must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
