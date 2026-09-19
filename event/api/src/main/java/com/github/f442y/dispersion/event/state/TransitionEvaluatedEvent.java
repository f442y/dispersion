package com.github.f442y.dispersion.event.state;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a transition routing function evaluates and resolves the next target state.
 */
public record TransitionEvaluatedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull String sourceState,
        @NonNull String targetState,
        @NonNull Instant timestamp
) implements StateLifecycleEvent {
    public TransitionEvaluatedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(sourceState, "sourceState must not be null");
        Objects.requireNonNull(targetState, "targetState must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
