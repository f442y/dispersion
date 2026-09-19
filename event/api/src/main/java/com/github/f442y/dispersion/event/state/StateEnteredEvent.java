package com.github.f442y.dispersion.event.state;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted immediately before executing a state's business logic action.
 */
public record StateEnteredEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String stateName,
        @NonNull Instant timestamp
) implements StateLifecycleEvent {
    public StateEnteredEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(stateName, "stateName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
