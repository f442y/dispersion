package com.github.f442y.dispersion.orchestration.core.messaging;

import com.github.f442y.dispersion.orchestration.messaging.SignalConsumer;
import com.github.f442y.dispersion.orchestration.messaging.SignalMessage;
import com.github.f442y.dispersion.orchestration.messaging.SignalPublisher;

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
 * In-memory implementation of {@link SignalPublisher} using dedicated Virtual Threads.
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
        topicSubscribers.computeIfAbsent(destination, _ -> new CopyOnWriteArrayList<>()).add(consumer);
        return this;
    }

    @NonNull
    public InMemorySignalBroker subscribeTopic(@NonNull String destination, @NonNull SignalConsumer consumer) {
        return subscribe(destination, consumer);
    }

    @NonNull
    public InMemorySignalBroker subscribeGlobal(@NonNull SignalConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer must not be null");
        globalSubscribers.add(consumer);
        return this;
    }

    /**
     * Registers a topic consumer and returns an {@link AutoCloseable} subscription handle for easy cleanup.
     */
    @NonNull
    public AutoCloseable register(@NonNull String destination, @NonNull SignalConsumer consumer) {
        subscribe(destination, consumer);
        return () -> unsubscribe(destination, consumer);
    }

    /**
     * Registers a global consumer and returns an {@link AutoCloseable} subscription handle for easy cleanup.
     */
    @NonNull
    public AutoCloseable registerGlobal(@NonNull SignalConsumer consumer) {
        subscribeGlobal(consumer);
        return () -> unsubscribeGlobal(consumer);
    }

    /**
     * Unsubscribes a consumer from a specific destination topic.
     *
     * @return true if the consumer was removed, false otherwise
     */
    public boolean unsubscribe(@NonNull String destination, @NonNull SignalConsumer consumer) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        List<SignalConsumer> listeners = topicSubscribers.get(destination);
        return listeners != null && listeners.remove(consumer);
    }

    /**
     * Unsubscribes a global consumer.
     *
     * @return true if the consumer was removed, false otherwise
     */
    public boolean unsubscribeGlobal(@NonNull SignalConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer must not be null");
        return globalSubscribers.remove(consumer);
    }

    @Override
    @NonNull
    public CompletableFuture<Void> publish(@NonNull SignalMessage message) {
        Objects.requireNonNull(message, "message must not be null");

        log.debug("Publishing message [{}] (signal: {}, corr: {}) to destination [{}]",
                message.messageId(), message.signalName(), message.correlationKey(), message.destination());

        CompletableFuture<Void> publishFuture = new CompletableFuture<>();

        virtualThreadExecutor.submit(() -> {
            java.util.List<Throwable> errors = new java.util.ArrayList<>();

            List<SignalConsumer> topicListeners = topicSubscribers.getOrDefault(message.destination(), List.of());
            for (SignalConsumer listener : topicListeners) {
                try {
                    listener.onMessage(message);
                } catch (Throwable t) {
                    log.error("Subscriber [{}] failed to process message [{}] on topic [{}]",
                            listener, message.messageId(), message.destination(), t);
                    errors.add(t);
                }
            }

            for (SignalConsumer globalListener : globalSubscribers) {
                try {
                    globalListener.onMessage(message);
                } catch (Throwable t) {
                    log.error("Global subscriber [{}] failed to process message [{}]",
                            globalListener, message.messageId(), t);
                    errors.add(t);
                }
            }

            if (errors.isEmpty()) {
                publishFuture.complete(null);
            } else {
                publishFuture.completeExceptionally(errors.getFirst());
            }
        });

        return publishFuture;
    }

    @Override
    public void close() {
        topicSubscribers.clear();
        globalSubscribers.clear();
        virtualThreadExecutor.close();
    }
}
