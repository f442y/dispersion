package com.github.f442y.dispersion.event.compensation;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted immediately before executing a single state's compensation rollback action.
 */
public record CompensationStepStartedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        boolean isRouted,
        @NonNull Instant timestamp
) implements CompensationEvent {
    public CompensationStepStartedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
