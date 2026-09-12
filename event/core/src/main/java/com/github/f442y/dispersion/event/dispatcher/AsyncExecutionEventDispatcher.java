package com.github.f442y.dispersion.event.dispatcher;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-throughput, non-blocking event dispatcher that decouples state machine execution virtual threads
 * from telemetry, logging, and remote UI/SSE consumers.
 *
 * <p>Implements both {@link ExecutionEventListener} (for receiving events from state machine engines)
 * and {@link Flow.Publisher} (for downstream reactive consumers such as SSE streams and WebSockets).</p>
 */
public class AsyncExecutionEventDispatcher implements ExecutionEventListener, Flow.Publisher<ExecutionEvent>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AsyncExecutionEventDispatcher.class);

    private static final int DEFAULT_BUFFER_CAPACITY = 16_384;
    private static final int DRAIN_BATCH_SIZE = 256;

    /**
     * Policy dictating behavior when the event buffer reaches capacity.
     */
    public enum OverflowPolicy {
        /**
         * Discards the oldest event in the buffer to make room for the new event.
         * Ensures zero blocking on the execution hot path.
         */
        DROP_OLDEST,

        /**
         * Discards the incoming event if the buffer is full.
         * Ensures zero blocking on the execution hot path.
         */
        DROP_LATEST,

        /**
         * Blocks the executing thread until buffer space becomes available.
         */
        BLOCK
    }

    private final BlockingQueue<ExecutionEvent> buffer;
    private final OverflowPolicy overflowPolicy;
    private final List<ExecutionEventListener> listeners = new CopyOnWriteArrayList<>();
    private final List<FlowSubscriberSubscription> subscribers = new CopyOnWriteArrayList<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong publishedCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();
    private final Thread dispatchWorkerThread;

    public AsyncExecutionEventDispatcher() {
        this(DEFAULT_BUFFER_CAPACITY, OverflowPolicy.DROP_OLDEST);
    }

    public AsyncExecutionEventDispatcher(int bufferCapacity, @NonNull OverflowPolicy overflowPolicy) {
        if (bufferCapacity <= 0) {
            throw new IllegalArgumentException("bufferCapacity must be positive");
        }
        this.buffer = new ArrayBlockingQueue<>(bufferCapacity);
        this.overflowPolicy = Objects.requireNonNull(overflowPolicy, "overflowPolicy must not be null");

        this.dispatchWorkerThread = Thread.ofVirtual()
                .name("dispersion-event-dispatcher-worker")
                .start(this::drainLoop);
    }

    @NonNull
    public static AsyncExecutionEventDispatcher create() {
        return new AsyncExecutionEventDispatcher();
    }

    @NonNull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Registers a listener to receive dispatched events.
     *
     * @param listener The listener to register
     * @return this dispatcher for chaining
     */
    @NonNull
    public AsyncExecutionEventDispatcher addListener(@NonNull ExecutionEventListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        listeners.add(listener);
        return this;
    }

    /**
     * Removes a previously registered listener.
     *
     * @param listener The listener to remove
     * @return true if removed, false otherwise
     */
    public boolean removeListener(@NonNull ExecutionEventListener listener) {
        return listeners.remove(listener);
    }

    @Override
    public void subscribe(@NonNull Subscriber<? super ExecutionEvent> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber must not be null");
        FlowSubscriberSubscription subscription = new FlowSubscriberSubscription(subscriber);
        subscribers.add(subscription);
        subscriber.onSubscribe(subscription);
    }

    @Override
    public void onEvent(@NonNull ExecutionEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        if (!running.get()) {
            droppedCount.incrementAndGet();
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
                        deliverEvent(event);
                    }
                    batch.clear();
                }
            } catch (InterruptedException e) {
                if (!running.get()) {
                    break;
                }
            } catch (Throwable t) {
                log.error("Unexpected error in event dispatcher loop", t);
            }
        }

        // Deliver any remaining events
        batch.clear();
        buffer.drainTo(batch);
        for (ExecutionEvent event : batch) {
            deliverEvent(event);
        }

        // Complete reactive subscribers
        for (FlowSubscriberSubscription sub : subscribers) {
            sub.complete();
        }
        subscribers.clear();
    }

    private void deliverEvent(@NonNull ExecutionEvent event) {
        for (ExecutionEventListener listener : listeners) {
            try {
                listener.onEvent(event);
            } catch (Throwable t) {
                log.error("ExecutionEventListener error for event [{}]: {}", event.getClass().getSimpleName(), t.getMessage(), t);
            }
        }

        for (FlowSubscriberSubscription sub : subscribers) {
            sub.deliver(event);
        }
    }

    /**
     * Total number of events accepted into the buffer.
     */
    public long publishedCount() {
        return publishedCount.get();
    }

    /**
     * Total number of events dropped due to queue overflow or shutdown.
     */
    public long droppedCount() {
        return droppedCount.get();
    }

    /**
     * Current count of events pending in the buffer.
     */
    public int queuedCount() {
        return buffer.size();
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            dispatchWorkerThread.interrupt();
            try {
                dispatchWorkerThread.join(Duration.ofSeconds(3).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private final class FlowSubscriberSubscription implements Subscription {
        private final Subscriber<? super ExecutionEvent> subscriber;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicBoolean canceled = new AtomicBoolean(false);

        private FlowSubscriberSubscription(Subscriber<? super ExecutionEvent> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                subscriber.onError(new IllegalArgumentException("Flow.Subscription request count must be positive"));
                return;
            }
            demand.addAndGet(n);
        }

        @Override
        public void cancel() {
            if (canceled.compareAndSet(false, true)) {
                subscribers.remove(this);
            }
        }

        void deliver(@NonNull ExecutionEvent event) {
            if (canceled.get()) {
                return;
            }
            if (demand.get() > 0) {
                try {
                    subscriber.onNext(event);
                    demand.decrementAndGet();
                } catch (Throwable t) {
                    cancel();
                    subscriber.onError(t);
                }
            }
        }

        void complete() {
            if (!canceled.get()) {
                try {
                    subscriber.onComplete();
                } catch (Throwable t) {
                    log.error("Error during subscriber onComplete", t);
                }
            }
        }
    }

    public static final class Builder {
        private int bufferCapacity = DEFAULT_BUFFER_CAPACITY;
        private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_OLDEST;

        public Builder capacity(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public Builder overflowPolicy(@NonNull OverflowPolicy overflowPolicy) {
            this.overflowPolicy = Objects.requireNonNull(overflowPolicy, "overflowPolicy must not be null");
            return this;
        }

        @NonNull
        public AsyncExecutionEventDispatcher build() {
            return new AsyncExecutionEventDispatcher(bufferCapacity, overflowPolicy);
        }
    }
}
