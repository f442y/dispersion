package com.github.f442y.dispersion.orchestration.command;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Idempotency envelope wrapping a {@link SignalCommand} with a unique message/command ID
 * to guarantee exactly-once processing semantics across network retries and webhook redeliveries.
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

    @NonNull
    public static <CMD extends SignalCommand> CommandEnvelope<CMD> of(
            @NonNull UUID commandId,
            @NonNull CMD command
    ) {
        return new CommandEnvelope<>(commandId, Instant.now(), command);
    }

    @NonNull
    public static <CMD extends SignalCommand> CommandEnvelope<CMD> of(@NonNull CMD command) {
        return new CommandEnvelope<>(UUID.randomUUID(), Instant.now(), command);
    }
}
