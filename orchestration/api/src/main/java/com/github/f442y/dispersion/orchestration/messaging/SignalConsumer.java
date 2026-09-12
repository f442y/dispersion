package com.github.f442y.dispersion.orchestration.messaging;

import org.jspecify.annotations.NonNull;

import java.util.concurrent.CompletableFuture;

/**
 * Inbound message consumer SPI for receiving signals from message brokers and routing them into state machines.
 */
@FunctionalInterface
public interface SignalConsumer {

    /**
     * Processes an incoming signal message delivered from a message broker.
     *
     * @param message The received signal message
     * @return CompletableFuture completing when the message processing turn resolves
     */
    @NonNull
    CompletableFuture<?> onMessage(@NonNull SignalMessage message);
}
