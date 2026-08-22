package com.github.f442y.dispersion.orchestration.messaging;

import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Universal broker-agnostic message container for transporting signals.
 */
public record SignalMessage(
        @NonNull String destination,
        @NonNull String signalName,
        @Nullable String correlationKey,
        @NonNull UUID messageId,
        @NonNull Instant timestamp,
        @NonNull Map<String, String> headers,
        @NonNull Object payload
) {

    public SignalMessage {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        headers = (headers != null) ? Map.copyOf(headers) : Collections.emptyMap();
        Objects.requireNonNull(payload, "payload must not be null");
    }

    public static SignalMessage of(@NonNull String destination, @NonNull SignalCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        return new SignalMessage(
                destination,
                command.signalName(),
                command.correlationKey(),
                UUID.randomUUID(),
                Instant.now(),
                Collections.emptyMap(),
                command
        );
    }

    public static SignalMessage of(@NonNull String destination, @NonNull CommandEnvelope<? extends SignalCommand> envelope) {
        Objects.requireNonNull(envelope, "envelope must not be null");
        return new SignalMessage(
                destination,
                envelope.command().signalName(),
                envelope.command().correlationKey(),
                envelope.commandId(),
                envelope.timestamp(),
                Collections.emptyMap(),
                envelope.command()
        );
    }

    @NonNull
    @SuppressWarnings("unchecked")
    public <C extends SignalCommand> CommandEnvelope<C> toEnvelope() {
        if (payload instanceof SignalCommand cmd) {
            return (CommandEnvelope<C>) new CommandEnvelope<>(messageId, timestamp, cmd);
        }
        throw new IllegalStateException("Cannot convert to CommandEnvelope: payload is not a SignalCommand (" + payload.getClass().getName() + ")");
    }
}
