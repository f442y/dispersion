package com.github.f442y.dispersion.event;

/**
 * Real-time operational metrics captured by an {@link EventBus}.
 *
 * @param publishedCount  Total events submitted to the bus
 * @param deliveredCount  Total event deliveries to listeners and active streams
 * @param droppedCount    Total events dropped due to buffer saturation or closed streams
 * @param queuedCount     Current number of events waiting in the intake buffer
 * @param activeStreams   Current count of open {@link EventStream} instances
 * @param activeListeners Current count of registered listeners
 */
public record EventBusMetrics(
        long publishedCount,
        long deliveredCount,
        long droppedCount,
        int queuedCount,
        int activeStreams,
        int activeListeners
) {}
