package com.github.f442y.dispersion.event.child;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when a child sub-workflow finishes execution.
 */
public record ChildMachineCompletedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull UUID childMachineId,
        @NonNull String childMachineName,
        @NonNull Instant timestamp
) implements ChildMachineEvent {
    public ChildMachineCompletedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(childMachineId, "childMachineId must not be null");
        Objects.requireNonNull(childMachineName, "childMachineName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
