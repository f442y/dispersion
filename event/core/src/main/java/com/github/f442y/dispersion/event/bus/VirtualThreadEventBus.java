package com.github.f442y.dispersion.event.bus;

import com.github.f442y.dispersion.event.EventBus;
import com.github.f442y.dispersion.event.EventBusMetrics;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.OverflowPolicy;
import com.github.f442y.dispersion.event.Subscription;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * High-throughput, non-reactive Virtual Thread-native Event Bus engineered for Java 25.
 *
 * <h2>Architectural Principles</h2>
 * <ul>
 *   <li><b>Zero Reactive Overhead:</b> No {@code Flow.Publisher}, no {@code Subscriber}, no complex
 *       demand-tracking state machines, and zero silent event drops.</li>
 *   <li><b>Virtual Thread Isolation:</b> State machine execution threads push events onto a fast
 *       bounded ring buffer and return instantly. A dedicated unpinned Virtual Thread drains and fans out.</li>
 *   <li><b>Strongly-Typed Subscriptions:</b> Native support for Java 25 sealed record types via
 *       {@link #subscribe(Class, Consumer)}, eliminating manual casting boilerplate.</li>
 *   <li><b>Pull-Based Virtual Thread Streaming:</b> SSE and WebSocket endpoints pull sequentially from
 *       {@link EventStream} using standard, lightweight blocking calls ({@link EventStream#take()}).</li>
 *   <li><b>Integrated Circular Replay Buffer:</b> In-memory recent event history for instant UI/timeline queries.</li>
 * </ul>
 */
public class VirtualThreadEventBus implements EventBus {

    private static final Logger log = LoggerFactory.getLogger(VirtualThreadEventBus.class);

    public static final int DEFAULT_BUFFER_CAPACITY = 16_384;
    public static final int DEFAULT_HISTORY_CAPACITY = 1_000;
    public static final int DEFAULT_STREAM_CAPACITY = 512;
    private static final int DRAIN_BATCH_SIZE = 256;

    private final BlockingQueue<ExecutionEvent> buffer;
    private final OverflowPolicy overflowPolicy;
    private final int historyCapacity;
    private final int streamDefaultCapacity;
    private final boolean direct;

    private final Deque<ExecutionEvent> historyBuffer;
    private final List<RegisteredListener> listeners = new CopyOnWriteArrayList<>();
    private final List<VirtualThreadEventStream> activeStreams = new CopyOnWriteArrayList<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong publishedCount = new AtomicLong();
    private final AtomicLong deliveredCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();
    private final Thread dispatchWorkerThread;

    public VirtualThreadEventBus() {
        this(new Builder());
    }

    protected VirtualThreadEventBus(@NonNull Builder builder) {
        Objects.requireNonNull(builder, "builder must not be null");
        this.overflowPolicy = builder.overflowPolicy;
        this.historyCapacity = builder.historyCapacity;
        this.streamDefaultCapacity = builder.streamDefaultCapacity;
        this.direct = builder.direct;

        this.historyBuffer = (historyCapacity > 0) ? new ArrayDeque<>(historyCapacity) : null;

        if (direct) {
            this.buffer = null;
            this.dispatchWorkerThread = null;
        } else {
            this.buffer = new ArrayBlockingQueue<>(builder.bufferCapacity);
            this.dispatchWorkerThread = Thread.ofVirtual()
                    .name("dispersion-event-bus-worker")
                    .start(this::drainLoop);
        }
    }

    @NonNull
    public static VirtualThreadEventBus create() {
        return new VirtualThreadEventBus();
    }

    @NonNull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a zero-thread, direct-delivery event bus that invokes listeners synchronously
     * on the emitting thread. Ideal for microsecond test harnesses and synchronous debugging.
     */
    @NonNull
    public static EventBus direct() {
        return new Builder().direct(true).build();
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

        RegisteredListener registered = new RegisteredListener(filter, listener);
        listeners.add(registered);
        return () -> listeners.remove(registered);
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
        return openStream(streamDefaultCapacity, _ -> true);
    }

    @Override
    @NonNull
    public EventStream openStream(int queueCapacity) {
        return openStream(queueCapacity, _ -> true);
    }

    @Override
    @NonNull
    public EventStream openStream(@NonNull Predicate<ExecutionEvent> filter) {
        return openStream(streamDefaultCapacity, filter);
    }

    @Override
    @NonNull
    public <E extends ExecutionEvent> EventStream openStream(@NonNull Class<E> eventType) {
        Objects.requireNonNull(eventType, "eventType must not be null");
        return openStream(streamDefaultCapacity, eventType::isInstance);
    }

    @NonNull
    public EventStream openStream(int queueCapacity, @NonNull Predicate<ExecutionEvent> filter) {
        Objects.requireNonNull(filter, "filter must not be null");
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive, but was: " + queueCapacity);
        }
        VirtualThreadEventStream stream = new VirtualThreadEventStream(queueCapacity, filter, this::removeStream);
        activeStreams.add(stream);
        return stream;
    }

    private void removeStream(@NonNull VirtualThreadEventStream stream) {
        activeStreams.remove(stream);
    }

    @Override
    @NonNull
    public List<ExecutionEvent> history(int limit) {
        if (historyBuffer == null || limit <= 0) {
            return Collections.emptyList();
        }
        synchronized (historyBuffer) {
            int size = historyBuffer.size();
            int start = Math.max(0, size - limit);
            List<ExecutionEvent> result = new ArrayList<>(size - start);
            int idx = 0;
            for (ExecutionEvent event : historyBuffer) {
                if (idx >= start) {
                    result.add(event);
                }
                idx++;
            }
            return Collections.unmodifiableList(result);
        }
    }

    @Override
    @NonNull
    public EventBusMetrics metrics() {
        int queued = (buffer != null) ? buffer.size() : 0;
        return new EventBusMetrics(
                publishedCount.get(),
                deliveredCount.get(),
                droppedCount.get(),
                queued,
                activeStreams.size(),
                listeners.size()
        );
    }

    @Override
    public void onEvent(@NonNull ExecutionEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        if (!running.get()) {
            droppedCount.incrementAndGet();
            return;
        }

        if (direct) {
            publishedCount.incrementAndGet();
            deliverSingleEvent(event);
            return;
        }

        switch (overflowPolicy) {
            case DROP_OLDEST -> {
                while (!buffer.offer(event)) {
                    buffer.poll(); // Discard oldest
                    droppedCount.incrementAndGet();
                }
                publishedCount.incrementAndGet();
            }
            case DROP_LATEST -> {
                if (buffer.offer(event)) {
                    publishedCount.incrementAndGet();
                } else {
                    droppedCount.incrementAndGet();
                }
            }
            case BLOCK -> {
                try {
                    buffer.put(event);
                    publishedCount.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    droppedCount.incrementAndGet();
                }
            }
        }
    }

    private void drainLoop() {
        List<ExecutionEvent> batch = new ArrayList<>(DRAIN_BATCH_SIZE);

        while (running.get() || !buffer.isEmpty()) {
            try {
                ExecutionEvent first = buffer.poll(50, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    buffer.drainTo(batch, DRAIN_BATCH_SIZE - 1);

                    for (ExecutionEvent event : batch) {
                        deliverSingleEvent(event);
                    }
                    batch.clear();
                }
            } catch (InterruptedException e) {
                if (!running.get()) {
                    break;
                }
            } catch (Throwable t) {
                log.error("Unexpected error in event bus worker loop", t);
            }
        }

        // Drain any remaining events before shutdown
        batch.clear();
        buffer.drainTo(batch);
        for (ExecutionEvent event : batch) {
            deliverSingleEvent(event);
        }

        // Close all active streams on shutdown
        for (VirtualThreadEventStream stream : activeStreams) {
            stream.close();
        }
        activeStreams.clear();
    }

    private void deliverSingleEvent(@NonNull ExecutionEvent event) {
        // 1. Record in circular replay history
        if (historyBuffer != null) {
            synchronized (historyBuffer) {
                if (historyBuffer.size() >= historyCapacity) {
                    historyBuffer.pollFirst();
                }
                historyBuffer.addLast(event);
            }
        }

        // 2. Deliver to active listeners
        for (RegisteredListener reg : listeners) {
            try {
                if (reg.filter.test(event)) {
                    reg.listener.onEvent(event);
                    deliveredCount.incrementAndGet();
                }
            } catch (Throwable t) {
                log.error("Error executing listener for event [{}]: {}", event.getClass().getSimpleName(), t.getMessage(), t);
            }
        }

        // 3. Deliver to active pull streams
        for (VirtualThreadEventStream stream : activeStreams) {
            try {
                if (stream.offer(event)) {
                    deliveredCount.incrementAndGet();
                } else {
                    droppedCount.incrementAndGet();
                }
            } catch (Throwable t) {
                log.error("Error delivering event to stream: {}", t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            if (dispatchWorkerThread != null) {
                dispatchWorkerThread.interrupt();
                try {
                    dispatchWorkerThread.join(Duration.ofSeconds(3).toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            // Close active streams
            for (VirtualThreadEventStream stream : activeStreams) {
                stream.close();
            }
            activeStreams.clear();
        }
    }

    private record RegisteredListener(
            Predicate<ExecutionEvent> filter,
            ExecutionEventListener listener
    ) {}

    /**
     * Concrete pull-based stream implementation backed by a bounded blocking queue.
     */
    private static final class VirtualThreadEventStream implements EventStream {

        private final BlockingQueue<ExecutionEvent> queue;
        private final Predicate<ExecutionEvent> filter;
        private final Consumer<VirtualThreadEventStream> onClose;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private VirtualThreadEventStream(
                int capacity,
                Predicate<ExecutionEvent> filter,
                Consumer<VirtualThreadEventStream> onClose
        ) {
            this.queue = new ArrayBlockingQueue<>(capacity);
            this.filter = filter;
            this.onClose = onClose;
        }

        boolean offer(@NonNull ExecutionEvent event) {
            if (closed.get()) {
                return false;
            }
            if (!filter.test(event)) {
                return true; // Not matched, considered handled without drop
            }
            // If the stream's buffer is full, drop oldest unread item to allow fresh events
            while (!queue.offer(event)) {
                queue.poll();
            }
            return true;
        }

        @Override
        @Nullable
        public ExecutionEvent poll(@NonNull Duration timeout) throws InterruptedException {
            Objects.requireNonNull(timeout, "timeout must not be null");
            if (closed.get() && queue.isEmpty()) {
                return null;
            }
            return queue.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        @NonNull
        public ExecutionEvent take() throws InterruptedException {
            while (!closed.get() || !queue.isEmpty()) {
                ExecutionEvent item = queue.poll(100, TimeUnit.MILLISECONDS);
                if (item != null) {
                    return item;
                }
            }
            throw new IllegalStateException("EventStream is closed");
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                onClose.accept(this);
            }
        }

        @Override
        @NonNull
        public Iterator<ExecutionEvent> iterator() {
            return new Iterator<>() {
                private ExecutionEvent nextItem = null;

                @Override
                public boolean hasNext() {
                    if (nextItem != null) {
                        return true;
                    }
                    if (closed.get() && queue.isEmpty()) {
                        return false;
                    }
                    try {
                        nextItem = poll(Duration.ofMillis(200));
                        return nextItem != null || (!closed.get() || !queue.isEmpty());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }

                @Override
                public ExecutionEvent next() {
                    if (nextItem != null) {
                        ExecutionEvent item = nextItem;
                        nextItem = null;
                        return item;
                    }
                    try {
                        return take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new NoSuchElementException("Interrupted waiting for next event");
                    }
                }
            };
        }
    }

    public static final class Builder {
        private int bufferCapacity = DEFAULT_BUFFER_CAPACITY;
        private int historyCapacity = DEFAULT_HISTORY_CAPACITY;
        private int streamDefaultCapacity = DEFAULT_STREAM_CAPACITY;
        private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_OLDEST;
        private boolean direct = false;

        public Builder bufferCapacity(int bufferCapacity) {
            if (bufferCapacity <= 0) {
                throw new IllegalArgumentException("bufferCapacity must be positive");
            }
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public Builder historyCapacity(int historyCapacity) {
            if (historyCapacity < 0) {
                throw new IllegalArgumentException("historyCapacity must be non-negative");
            }
            this.historyCapacity = historyCapacity;
            return this;
        }

        public Builder streamDefaultCapacity(int streamDefaultCapacity) {
            if (streamDefaultCapacity <= 0) {
                throw new IllegalArgumentException("streamDefaultCapacity must be positive");
            }
            this.streamDefaultCapacity = streamDefaultCapacity;
            return this;
        }

        public Builder overflowPolicy(@NonNull OverflowPolicy overflowPolicy) {
            this.overflowPolicy = Objects.requireNonNull(overflowPolicy, "overflowPolicy must not be null");
            return this;
        }

        public Builder direct(boolean direct) {
            this.direct = direct;
            return this;
        }

        @NonNull
        public VirtualThreadEventBus build() {
            return new VirtualThreadEventBus(this);
        }
    }
}
