package com.github.f442y.dispersion.event.dispatcher;

import com.github.f442y.dispersion.event.EventBus;
import com.github.f442y.dispersion.event.EventBusMetrics;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.OverflowPolicy;
import com.github.f442y.dispersion.event.Subscription;
import com.github.f442y.dispersion.event.bus.VirtualThreadEventBus;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * High-throughput Virtual Thread-native event dispatcher implementing {@link EventBus}.
 *
 * <p>Wraps {@link VirtualThreadEventBus} to provide non-blocking asynchronous event delivery
 * decoupling state machine execution threads from telemetry and logging consumers.</p>
 */
public class AsyncExecutionEventDispatcher implements EventBus {

    private final VirtualThreadEventBus delegate;
    private final Map<ExecutionEventListener, Subscription> legacySubscriptions = new ConcurrentHashMap<>();

    public AsyncExecutionEventDispatcher() {
        this(VirtualThreadEventBus.builder().build());
    }

    public AsyncExecutionEventDispatcher(int bufferCapacity, @NonNull OverflowPolicy overflowPolicy) {
        this(VirtualThreadEventBus.builder()
                .bufferCapacity(bufferCapacity)
                .overflowPolicy(Objects.requireNonNull(overflowPolicy, "overflowPolicy must not be null"))
                .build());
    }

    public AsyncExecutionEventDispatcher(@NonNull VirtualThreadEventBus delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    @NonNull
    public static AsyncExecutionEventDispatcher create() {
        return new AsyncExecutionEventDispatcher();
    }

    @NonNull
    public static Builder builder() {
        return new Builder();
    }

    @NonNull
    public AsyncExecutionEventDispatcher addListener(@NonNull ExecutionEventListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        Subscription sub = delegate.subscribe(listener);
        legacySubscriptions.put(listener, sub);
        return this;
    }

    @NonNull
    public AsyncExecutionEventDispatcher addListener(
            @NonNull Predicate<ExecutionEvent> filter,
            @NonNull ExecutionEventListener listener
    ) {
        Objects.requireNonNull(filter, "filter must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        Subscription sub = delegate.subscribe(filter, listener);
        legacySubscriptions.put(listener, sub);
        return this;
    }

    public boolean removeListener(@NonNull ExecutionEventListener listener) {
        Subscription sub = legacySubscriptions.remove(listener);
        if (sub != null) {
            sub.unsubscribe();
            return true;
        }
        return false;
    }

    @Override
    @NonNull
    public Subscription subscribe(@NonNull ExecutionEventListener listener) {
        return delegate.subscribe(listener);
    }

    @Override
    @NonNull
    public Subscription subscribe(
            @NonNull Predicate<ExecutionEvent> filter,
            @NonNull ExecutionEventListener listener
    ) {
        return delegate.subscribe(filter, listener);
    }

    @Override
    @NonNull
    public <EVENT_TYPE extends ExecutionEvent> Subscription subscribe(
            @NonNull Class<EVENT_TYPE> eventType,
            @NonNull Consumer<EVENT_TYPE> listener
    ) {
        return delegate.subscribe(eventType, listener);
    }

    @Override
    @NonNull
    public EventStream openStream() {
        return delegate.openStream();
    }

    @Override
    @NonNull
    public EventStream openStream(int queueCapacity) {
        return delegate.openStream(queueCapacity);
    }

    @Override
    @NonNull
    public EventStream openStream(@NonNull Predicate<ExecutionEvent> filter) {
        return delegate.openStream(filter);
    }

    @Override
    @NonNull
    public <EVENT_TYPE extends ExecutionEvent> EventStream openStream(@NonNull Class<EVENT_TYPE> eventType) {
        return delegate.openStream(eventType);
    }

    @Override
    @NonNull
    public List<ExecutionEvent> history(int limit) {
        return delegate.history(limit);
    }

    @Override
    @NonNull
    public EventBusMetrics metrics() {
        return delegate.metrics();
    }

    public long publishedCount() {
        return delegate.metrics().publishedCount();
    }

    public long droppedCount() {
        return delegate.metrics().droppedCount();
    }

    public int queuedCount() {
        return delegate.metrics().queuedCount();
    }

    @Override
    public void onEvent(@NonNull ExecutionEvent event) {
        delegate.onEvent(event);
    }

    @Override
    public void close() {
        delegate.close();
    }

    public static final class Builder {
        private int bufferCapacity = VirtualThreadEventBus.DEFAULT_BUFFER_CAPACITY;
        private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_OLDEST;
        private int historyCapacity = VirtualThreadEventBus.DEFAULT_HISTORY_CAPACITY;
        private int streamDefaultCapacity = VirtualThreadEventBus.DEFAULT_STREAM_CAPACITY;

        public Builder capacity(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public Builder overflowPolicy(@NonNull OverflowPolicy overflowPolicy) {
            this.overflowPolicy = Objects.requireNonNull(overflowPolicy, "overflowPolicy must not be null");
            return this;
        }

        public Builder historyCapacity(int historyCapacity) {
            this.historyCapacity = historyCapacity;
            return this;
        }

        public Builder streamDefaultCapacity(int streamDefaultCapacity) {
            this.streamDefaultCapacity = streamDefaultCapacity;
            return this;
        }

        @NonNull
        public AsyncExecutionEventDispatcher build() {
            VirtualThreadEventBus bus = VirtualThreadEventBus.builder()
                    .bufferCapacity(bufferCapacity)
                    .overflowPolicy(overflowPolicy)
                    .historyCapacity(historyCapacity)
                    .streamDefaultCapacity(streamDefaultCapacity)
                    .build();
            return new AsyncExecutionEventDispatcher(bus);
        }
    }
}
