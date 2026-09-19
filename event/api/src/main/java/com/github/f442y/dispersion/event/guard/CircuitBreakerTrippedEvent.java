package com.github.f442y.dispersion.event.guard;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when total transitions breach the safety circuit breaker threshold.
 */
public record CircuitBreakerTrippedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        int maxTransitions,
        @NonNull Instant timestamp
) implements ExecutionGuardEvent {
    public CircuitBreakerTrippedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
