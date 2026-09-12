package com.github.f442y.dispersion.event;

/**
 * Handle representing an active subscription on an {@link EventBus}.
 * Calling {@link #unsubscribe()} or {@link #close()} removes the listener from the bus.
 */
@FunctionalInterface
public interface Subscription extends AutoCloseable {

    /**
     * Unsubscribes the associated listener from receiving further events.
     */
    void unsubscribe();

    @Override
    default void close() {
        unsubscribe();
    }
}
