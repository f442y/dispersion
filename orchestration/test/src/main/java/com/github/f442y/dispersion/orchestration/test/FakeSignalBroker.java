package com.github.f442y.dispersion.orchestration.test;

import com.github.f442y.dispersion.orchestration.messaging.SignalConsumer;
import com.github.f442y.dispersion.orchestration.messaging.SignalMessage;
import com.github.f442y.dispersion.orchestration.messaging.SignalPublisher;
import org.jspecify.annotations.NonNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Synchronous, in-memory {@link SignalPublisher} test double for saga and messaging tests.
 *
 * <p>Records all published messages for assertions and delivers messages directly to
 * registered consumers without network brokers or thread scheduling delays.</p>
 */
public class FakeSignalBroker implements SignalPublisher, AutoCloseable {

    private final List<SignalMessage> publishedMessages = new CopyOnWriteArrayList<>();
    private final Map<String, List<SignalConsumer>> topicSubscribers = new ConcurrentHashMap<>();
    private final List<SignalConsumer> globalSubscribers = new CopyOnWriteArrayList<>();

    public FakeSignalBroker subscribe(@NonNull String destination, @NonNull SignalConsumer consumer) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(consumer, "consumer must not be null");
        topicSubscribers.computeIfAbsent(destination, _ -> new CopyOnWriteArrayList<>()).add(consumer);
        return this;
    }

    public FakeSignalBroker subscribeGlobal(@NonNull SignalConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer must not be null");
        globalSubscribers.add(consumer);
        return this;
    }

    public AutoCloseable register(@NonNull String destination, @NonNull SignalConsumer consumer) {
        subscribe(destination, consumer);
        return () -> unsubscribe(destination, consumer);
    }

    public AutoCloseable registerGlobal(@NonNull SignalConsumer consumer) {
        subscribeGlobal(consumer);
        return () -> unsubscribeGlobal(consumer);
    }

    public boolean unsubscribe(@NonNull String destination, @NonNull SignalConsumer consumer) {
        List<SignalConsumer> listeners = topicSubscribers.get(destination);
        return listeners != null && listeners.remove(consumer);
    }

    public boolean unsubscribeGlobal(@NonNull SignalConsumer consumer) {
        return globalSubscribers.remove(consumer);
    }

    @Override
    @NonNull
    public CompletableFuture<Void> publish(@NonNull SignalMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        publishedMessages.add(message);

        List<Throwable> errors = new CopyOnWriteArrayList<>();

        List<SignalConsumer> topicListeners = topicSubscribers.getOrDefault(message.destination(), List.of());
        for (SignalConsumer listener : topicListeners) {
            try {
                listener.onMessage(message);
            } catch (Throwable t) {
                errors.add(t);
            }
        }

        for (SignalConsumer globalListener : globalSubscribers) {
            try {
                globalListener.onMessage(message);
            } catch (Throwable t) {
                errors.add(t);
            }
        }

        if (errors.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        } else {
            return CompletableFuture.failedFuture(errors.getFirst());
        }
    }

    @NonNull
    public List<SignalMessage> publishedMessages() {
        return Collections.unmodifiableList(publishedMessages);
    }

    public boolean hasPublished(@NonNull Predicate<SignalMessage> filter) {
        Objects.requireNonNull(filter, "filter must not be null");
        return publishedMessages.stream().anyMatch(filter);
    }

    public void assertPublished(@NonNull String destination) {
        if (!hasPublished(m -> destination.equals(m.destination()))) {
            throw new AssertionError("Expected message to destination [" + destination + "], but none was found. Published: " + publishedMessages);
        }
    }

    public void assertPublished(@NonNull String destination, @NonNull String correlationKey) {
        if (!hasPublished(m -> destination.equals(m.destination()) && correlationKey.equals(m.correlationKey()))) {
            throw new AssertionError("Expected message to destination [" + destination + "] with correlationKey [" + correlationKey + "], but none was found.");
        }
    }

    public void clear() {
        publishedMessages.clear();
    }

    @Override
    public void close() {
        topicSubscribers.clear();
        globalSubscribers.clear();
        publishedMessages.clear();
    }
}
