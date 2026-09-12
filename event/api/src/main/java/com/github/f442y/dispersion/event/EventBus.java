package com.github.f442y.dispersion.event;

import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * High-throughput, non-reactive Virtual Thread-native Event Bus for state machine
 * lifecycle telemetry, monitoring, and live streaming.
 *
 * <p>The {@link EventBus} serves as the central observability hub for Dispersion workflows.
 * It decouples state machine execution threads from telemetry consumers via virtual-thread buffers,
 * provides strongly-typed subscriptions for sealed {@link ExecutionEvent} records, and supports
 * pull-based {@link EventStream} handles for Server-Sent Events (SSE) and WebSockets.</p>
 */
public interface EventBus extends ExecutionEventListener, AutoCloseable {

    /**
     * Subscribes a listener to receive all emitted execution events.
     *
     * @param listener The listener callback
     * @return A {@link Subscription} handle to unsubscribe
     */
    @NonNull
    Subscription subscribe(@NonNull ExecutionEventListener listener);

    /**
     * Subscribes a listener to receive only events matching the given predicate filter.
     *
     * @param filter   The predicate filter to evaluate for each event
     * @param listener The listener callback
     * @return A {@link Subscription} handle to unsubscribe
     */
    @NonNull
    Subscription subscribe(
            @NonNull Predicate<ExecutionEvent> filter,
            @NonNull ExecutionEventListener listener
    );

    /**
     * Subscribes a strongly-typed listener for a specific sealed {@link ExecutionEvent} record type.
     * Events of other types are filtered out automatically without requiring manual type-checks or casts.
     *
     * @param eventType The specific event class (e.g. {@code TurnFailedEvent.class})
     * @param listener  The consumer callback receiving instances of type {@code E}
     * @param <E>       The concrete event type
     * @return A {@link Subscription} handle to unsubscribe
     */
    @NonNull
    <E extends ExecutionEvent> Subscription subscribe(
            @NonNull Class<E> eventType,
            @NonNull Consumer<E> listener
    );

    /**
     * Opens an unbounded pull-based {@link EventStream} on a dedicated Virtual Thread queue.
     *
     * @return An open {@link EventStream}
     */
    @NonNull
    EventStream openStream();

    /**
     * Opens a bounded pull-based {@link EventStream} with the specified queue buffer capacity.
     *
     * @param queueCapacity The maximum number of unread events buffered for this stream
     * @return An open {@link EventStream}
     */
    @NonNull
    EventStream openStream(int queueCapacity);

    /**
     * Opens a filtered pull-based {@link EventStream} receiving only events matching the predicate.
     *
     * @param filter The predicate filter
     * @return An open {@link EventStream}
     */
    @NonNull
    EventStream openStream(@NonNull Predicate<ExecutionEvent> filter);

    /**
     * Opens a strongly-typed pull-based {@link EventStream} receiving only events of the specified type.
     *
     * @param eventType The event record class
     * @param <E>       The concrete event type
     * @return An open {@link EventStream}
     */
    @NonNull
    <E extends ExecutionEvent> EventStream openStream(@NonNull Class<E> eventType);

    /**
     * Retrieves up to {@code limit} recent events recorded in the in-memory circular replay buffer,
     * ordered chronologically from oldest to newest.
     *
     * @param limit Maximum number of recent events to retrieve
     * @return An unmodifiable list of historical events
     */
    @NonNull
    List<ExecutionEvent> history(int limit);

    /**
     * Returns real-time operational metrics for this event bus.
     *
     * @return The current {@link EventBusMetrics}
     */
    @NonNull
    EventBusMetrics metrics();

    /**
     * Dispatches an execution event onto the bus.
     *
     * @param event The execution event
     */
    @Override
    void onEvent(@NonNull ExecutionEvent event);

    /**
     * Shuts down the event bus, drains pending events, closes all active streams, and terminates workers.
     */
    @Override
    void close();
}
