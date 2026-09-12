package com.github.f442y.dispersion.event;

/**
 * Policy dictating behavior when an event buffer or stream queue reaches capacity.
 */
public enum OverflowPolicy {
    /**
     * Discards the oldest event in the buffer to make room for the incoming event.
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
