package com.github.f442y.dispersion.orchestration.command;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Distributed delivery envelope wrapping a {@link SignalCommand} with a unique {@code commandId}
 * for idempotency, deduplication, and timestamps.
 *
 * @param <C> The concrete {@link SignalCommand} type
 */
public record CommandEnvelope<C extends SignalCommand>(
        @NonNull UUID commandId,
        @NonNull Instant timestamp,
        @NonNull C command
) {

    public CommandEnvelope {
        Objects.requireNonNull(commandId, "commandId must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        Objects.requireNonNull(command, "command must not be null");
    }

    /**
     * Creates a new {@link CommandEnvelope} with an auto-generated random commandId and current timestamp.
     *
     * @param <CMD>   The command type
     * @param command The signal command to wrap
     * @return A new {@link CommandEnvelope} instance
     */
    @NonNull
    public static <CMD extends SignalCommand> CommandEnvelope<CMD> of(@NonNull CMD command) {
        return new CommandEnvelope<>(UUID.randomUUID(), Instant.now(), command);
    }

    /**
     * Creates a new {@link CommandEnvelope} with an explicit commandId and current timestamp.
     *
     * @param <CMD>     The command type
     * @param commandId The explicit command identifier
     * @param command   The signal command to wrap
     * @return A new {@link CommandEnvelope} instance
     */
    @NonNull
    public static <CMD extends SignalCommand> CommandEnvelope<CMD> of(@NonNull UUID commandId, @NonNull CMD command) {
        return new CommandEnvelope<>(commandId, Instant.now(), command);
    }
}
