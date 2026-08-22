package com.github.f442y.dispersion.orchestration.messaging;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * In-memory implementation of {@link SignalPublisher} using Virtual Threads.
 */
public class InMemorySignalBroker implements SignalPublisher, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InMemorySignalBroker.class);

    private final Map<String, List<SignalConsumer>> topicSubscribers = new ConcurrentHashMap<>();
    private final List<SignalConsumer> globalSubscribers = new CopyOnWriteArrayList<>();
    private final ExecutorService virtualThreadExecutor;

    public InMemorySignalBroker() {
        this(Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("in-memory-broker-", 0).factory()
        ));
    }

    public InMemorySignalBroker(@NonNull ExecutorService virtualThreadExecutor) {
        this.virtualThreadExecutor = Objects.requireNonNull(virtualThreadExecutor, "virtualThreadExecutor must not be null");
    }

    @NonNull
    public InMemorySignalBroker subscribe(@NonNull String destination, @NonNull SignalConsumer consumer) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        topicSubscribers.computeIfAbsent(destination, k -> new CopyOnWriteArrayList<>()).add(consumer);
        return this;
    }

    @NonNull
    public InMemorySignalBroker subscribeGlobal(@NonNull SignalConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer must not be null");
        globalSubscribers.add(consumer);
        return this;
    }

    @Override
    @NonNull
    public CompletableFuture<Void> publish(@NonNull SignalMessage message) {
        Objects.requireNonNull(message, "message must not be null");

        log.debug("Publishing message [{}] (signal: {}, corr: {}) to destination [{}]",
                message.messageId(), message.signalName(), message.correlationKey(), message.destination());

        CompletableFuture<Void> publishFuture = new CompletableFuture<>();

        virtualThreadExecutor.submit(() -> {
            try {
                List<SignalConsumer> topicListeners = topicSubscribers.getOrDefault(message.destination(), List.of());
                for (SignalConsumer listener : topicListeners) {
                    listener.onMessage(message);
                }
                for (SignalConsumer globalListener : globalSubscribers) {
                    globalListener.onMessage(message);
                }
                publishFuture.complete(null);
            } catch (Throwable t) {
                log.error("Error dispatching message to in-memory broker subscribers", t);
                publishFuture.completeExceptionally(t);
            }
        });

        return publishFuture;
    }

    @Override
    public void close() {
        virtualThreadExecutor.close();
    }
}
