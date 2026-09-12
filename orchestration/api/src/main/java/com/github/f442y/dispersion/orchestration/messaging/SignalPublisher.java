package com.github.f442y.dispersion.orchestration.messaging;

import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Outbound message publishing SPI for dispatching signals to message brokers (e.g. Kafka, RabbitMQ, SQS).
 */
public interface SignalPublisher {

    /**
     * Publishes a signal message asynchronously to the destination topic/queue.
     *
     * @param message The signal message to publish
     * @return CompletableFuture completing when message is acknowledged by the broker
     */
    @NonNull
    CompletableFuture<Void> publish(@NonNull SignalMessage message);

    /**
     * Publishes a {@link SignalCommand} asynchronously.
     *
     * @param destination The target broker topic/queue
     * @param command     The signal command to publish
     * @return CompletableFuture completing when message is published
     */
    @NonNull
    default CompletableFuture<Void> publish(@NonNull String destination, @NonNull SignalCommand command) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(command, "command must not be null");
        SignalMessage msg = new SignalMessage(
                destination,
                command.signalName(),
                command.correlationKey(),
                UUID.randomUUID(),
                Instant.now(),
                Collections.emptyMap(),
                command
        );
        return publish(msg);
    }

    /**
     * Publishes an idempotent {@link CommandEnvelope} asynchronously.
     *
     * @param destination The target broker topic/queue
     * @param envelope    The command envelope
     * @return CompletableFuture completing when message is published
     */
    @NonNull
    default CompletableFuture<Void> publish(@NonNull String destination, @NonNull CommandEnvelope<?> envelope) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(envelope, "envelope must not be null");
        SignalMessage msg = new SignalMessage(
                destination,
                envelope.command().signalName(),
                envelope.command().correlationKey(),
                envelope.commandId(),
                envelope.timestamp(),
                Collections.emptyMap(),
                envelope
        );
        return publish(msg);
    }
}
