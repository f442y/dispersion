package com.github.f442y.dispersion.orchestration.command;

import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Emitted when an incoming command envelope is recognized as a duplicate and ignored.
 */
public record CommandDeduplicatedEvent(
        @NonNull UUID machineId,
        @NonNull String machineName,
        @NonNull UUID commandId,
        @Nullable String correlationKey,
        @NonNull Instant timestamp
) implements ExecutionEvent {
    public CommandDeduplicatedEvent {
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(commandId, "commandId must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
    }
}
