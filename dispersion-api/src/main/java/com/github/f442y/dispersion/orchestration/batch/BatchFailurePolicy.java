package com.github.f442y.dispersion.orchestration.batch;

/**
 * Failure handling strategy for batch orchestrations when one or more items encounter unhandled errors.
 */
public enum BatchFailurePolicy {

    /**
     * Any item failure aborts the entire batch and triggers automated Saga compensation rollbacks.
     */
    FAIL_FAST,

    /**
     * Failed items are isolated, allowing healthy items to cross barriers and finish.
     */
    ISOLATE_FAILED_ITEMS
}
