package com.github.f442y.dispersion.event.retry;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an action fails and is scheduled for retry after a backoff delay.
 */
public record RetryAttemptedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        int attempt,
        int maxAttempts,
        @NonNull Duration delay,
        @NonNull Throwable lastCause,
        @NonNull Instant timestamp
) implements RetryLifecycleEvent {
    public RetryAttemptedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(delay, "delay must not be null");
        Objects.requireNonNull(lastCause, "lastCause must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
