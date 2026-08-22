package com.github.f442y.dispersion.orchestration.batch;

/**
 * Strategy defining when a barrier synchronization state in a batch orchestration releases arrived items.
 */
public enum BarrierPolicy {

    /**
     * Barrier releases automatically when all items in the batch arrive at this state.
     */
    ALL_ITEMS_ARRIVED,

    /**
     * Barrier holds arrived items until an explicit batch-level signal is delivered.
     */
    SIGNAL_TRIGGERED
}
