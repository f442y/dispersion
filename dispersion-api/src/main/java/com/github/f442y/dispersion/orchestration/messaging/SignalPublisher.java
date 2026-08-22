package com.github.f442y.dispersion.orchestration.messaging;

import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import org.jspecify.annotations.NonNull;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * SPI for publishing outbound signals to message brokers (Kafka, RabbitMQ, SQS, etc.).
 */
@FunctionalInterface
public interface SignalPublisher {

    @NonNull
    CompletableFuture<Void> publish(@NonNull SignalMessage message);

    @NonNull
    default CompletableFuture<Void> publish(@NonNull String destination, @NonNull SignalCommand command) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(command, "command must not be null");
        return publish(SignalMessage.of(destination, command));
    }

    @NonNull
    default CompletableFuture<Void> publish(@NonNull String destination, @NonNull CommandEnvelope<? extends SignalCommand> envelope) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(envelope, "envelope must not be null");
        return publish(SignalMessage.of(destination, envelope));
    }
}
