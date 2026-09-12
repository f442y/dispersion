package com.github.f442y.dispersion.event.test;

import com.github.f442y.dispersion.event.EventBus;
import com.github.f442y.dispersion.event.EventBusMetrics;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.Subscription;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Synchronous, in-memory {@link EventBus} implementation designed for unit tests.
 *
 * <p>Dispatches events immediately on the calling thread to prevent asynchronous
 * race conditions in unit tests, while recording every event for assertions.</p>
 */
public class RecordingEventBus implements EventBus {

    private final List<ExecutionEvent> recordedEvents = new CopyOnWriteArrayList<>();
    private final List<SubscriptionEntry> subscriptions = new CopyOnWriteArrayList<>();
    private final List<RecordingStream> activeStreams = new CopyOnWriteArrayList<>();

    private final AtomicLong publishedCount = new AtomicLong(0);
    private final AtomicLong deliveredCount = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private record SubscriptionEntry(
            Predicate<ExecutionEvent> filter,
            ExecutionEventListener listener
    ) {}

    @Override
    public void onEvent(@NonNull ExecutionEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        if (closed.get()) {
            return;
        }

        publishedCount.incrementAndGet();
        recordedEvents.add(event);

        for (SubscriptionEntry entry : subscriptions) {
            if (entry.filter.test(event)) {
                try {
                    entry.listener.onEvent(event);
                    deliveredCount.incrementAndGet();
                } catch (Throwable ignored) {
                }
            }
        }

        for (RecordingStream stream : activeStreams) {
            if (!stream.isClosed() && stream.filter.test(event)) {
                stream.offer(event);
                deliveredCount.incrementAndGet();
            }
        }
    }

    @Override
    @NonNull
    public Subscription subscribe(@NonNull ExecutionEventListener listener) {
        return subscribe(_ -> true, listener);
    }

    @Override
    @NonNull
    public Subscription subscribe(
            @NonNull Predicate<ExecutionEvent> filter,
            @NonNull ExecutionEventListener listener
    ) {
        Objects.requireNonNull(filter, "filter must not be null");
        Objects.requireNonNull(listener, "listener must not be null");

        SubscriptionEntry entry = new SubscriptionEntry(filter, listener);
        subscriptions.add(entry);
        return () -> subscriptions.remove(entry);
    }

    @Override
    @NonNull
    public <E extends ExecutionEvent> Subscription subscribe(
            @NonNull Class<E> eventType,
            @NonNull Consumer<E> listener
    ) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(listener, "listener must not be null");

        return subscribe(
                eventType::isInstance,
                event -> listener.accept(eventType.cast(event))
        );
    }

    @Override
    @NonNull
    public EventStream openStream() {
        return openStream(_ -> true);
    }

    @Override
    @NonNull
    public EventStream openStream(int queueCapacity) {
        return openStream(_ -> true);
    }

    @Override
    @NonNull
    public EventStream openStream(@NonNull Predicate<ExecutionEvent> filter) {
        Objects.requireNonNull(filter, "filter must not be null");
        RecordingStream stream = new RecordingStream(filter, this::removeStream);
        activeStreams.add(stream);
        return stream;
    }

    @Override
    @NonNull
    public <E extends ExecutionEvent> EventStream openStream(@NonNull Class<E> eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        return openStream(eventType::isInstance);
    }

    private void removeStream(RecordingStream stream) {
        activeStreams.remove(stream);
    }

    @Override
    @NonNull
    public List<ExecutionEvent> history(int limit) {
        int max = Math.max(0, limit);
        int size = recordedEvents.size();
        if (size <= max) {
            return Collections.unmodifiableList(recordedEvents);
        }
        return Collections.unmodifiableList(recordedEvents.subList(size - max, size));
    }

    @Override
    @NonNull
    public EventBusMetrics metrics() {
        return new EventBusMetrics(
                publishedCount.get(),
                deliveredCount.get(),
                0,
                0,
                activeStreams.size(),
                subscriptions.size()
        );
    }

    // =========================================================================
    // Test Inspection Helpers
    // =========================================================================

    @NonNull
    public List<ExecutionEvent> events() {
        return Collections.unmodifiableList(recordedEvents);
    }

    @NonNull
    public List<ExecutionEvent> recordedEvents() {
        return events();
    }

    @SuppressWarnings("unchecked")
    @NonNull
    public <E extends ExecutionEvent> List<E> eventsOfType(@NonNull Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        return recordedEvents.stream()
                .filter(type::isInstance)
                .map(e -> (E) e)
                .toList();
    }

    public boolean hasEmitted(@NonNull Predicate<ExecutionEvent> filter) {
        Objects.requireNonNull(filter, "filter must not be null");
        return recordedEvents.stream().anyMatch(filter);
    }

    public <E extends ExecutionEvent> boolean hasEmitted(@NonNull Class<E> type) {
        Objects.requireNonNull(type, "type must not be null");
        return recordedEvents.stream().anyMatch(type::isInstance);
    }

    public <E extends ExecutionEvent> void assertEmitted(@NonNull Class<E> type) {
        if (!hasEmitted(type)) {
            throw new AssertionError("Expected event of type [" + type.getSimpleName() + "] to be emitted, but was not. Recorded events: " + recordedEvents);
        }
    }

    public <E extends ExecutionEvent> void assertEmitted(@NonNull Class<E> type, @NonNull Predicate<E> filter) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(filter, "filter must not be null");
        boolean matched = recordedEvents.stream()
                .filter(type::isInstance)
                .map(type::cast)
                .anyMatch(filter);
        if (!matched) {
            throw new AssertionError("Expected matching event of type [" + type.getSimpleName() + "] to be emitted, but no matching event was found. Recorded events: " + recordedEvents);
        }
    }

    public void clear() {
        recordedEvents.clear();
        publishedCount.set(0);
        deliveredCount.set(0);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (RecordingStream stream : activeStreams) {
                stream.close();
            }
            activeStreams.clear();
            subscriptions.clear();
        }
    }

    // =========================================================================
    // In-Memory Stream Test Double
    // =========================================================================

    private static class RecordingStream implements EventStream {
        private final Predicate<ExecutionEvent> filter;
        private final Consumer<RecordingStream> onClose;
        private final BlockingQueue<ExecutionEvent> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean isClosed = new AtomicBoolean(false);

        RecordingStream(Predicate<ExecutionEvent> filter, Consumer<RecordingStream> onClose) {
            this.filter = filter;
            this.onClose = onClose;
        }

        void offer(ExecutionEvent event) {
            if (!isClosed.get()) {
                queue.offer(event);
            }
        }

        @Override
        @Nullable
        public ExecutionEvent poll(@NonNull Duration timeout) throws InterruptedException {
            if (isClosed.get() && queue.isEmpty()) {
                return null;
            }
            return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        @NonNull
        public ExecutionEvent take() throws InterruptedException {
            if (isClosed.get() && queue.isEmpty()) {
                throw new IllegalStateException("Stream is closed");
            }
            ExecutionEvent event = queue.take();
            return event;
        }

        @Override
        public boolean isClosed() {
            return isClosed.get();
        }

        @Override
        public void close() {
            if (isClosed.compareAndSet(false, true)) {
                onClose.accept(this);
            }
        }

        @Override
        @NonNull
        public Iterator<ExecutionEvent> iterator() {
            return queue.iterator();
        }
    }
}
