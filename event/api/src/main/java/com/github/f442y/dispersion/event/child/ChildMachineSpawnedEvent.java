package com.github.f442y.dispersion.event.child;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an orchestration state invokes a sub-workflow, establishing parent-child lineage.
 */
public record ChildMachineSpawnedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull UUID childMachineId,
        @NonNull String childMachineName,
        @NonNull String parentStateName,
        @NonNull Instant timestamp
) implements ChildMachineEvent {
    public ChildMachineSpawnedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(childMachineId, "childMachineId must not be null");
        Objects.requireNonNull(childMachineName, "childMachineName must not be null");
        Objects.requireNonNull(parentStateName, "parentStateName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
