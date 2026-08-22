package com.github.f442y.dispersion.orchestration.batch;

/**
 * Failure handling policy when an individual item encounters an unhandled failure in a batch.
 */
public enum BatchFailurePolicy {

    /**
     * Immediately terminates the entire batch orchestration and unwinds all completed items.
     */
    FAIL_FAST,

    /**
     * Marks the failed item as failed and continues processing remaining items in the batch.
     */
    ISOLATE_FAILED_ITEMS
}
