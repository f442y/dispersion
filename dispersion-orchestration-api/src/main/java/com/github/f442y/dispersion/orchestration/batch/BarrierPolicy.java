package com.github.f442y.dispersion.orchestration.batch;

/**
 * Synchronization policy for barrier states in a Set/Batch Orchestration.
 */
public enum BarrierPolicy {

    /**
     * Barrier is released only after ALL active items in the batch collection have reached the barrier state.
     */
    ALL_ITEMS_ARRIVED,

    /**
     * Barrier is held until an explicit batch-level external signal or command is received.
     */
    SIGNAL_TRIGGERED
}
