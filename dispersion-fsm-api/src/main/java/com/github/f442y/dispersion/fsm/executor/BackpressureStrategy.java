package com.github.f442y.dispersion.fsm.executor;

/**
 * Strategy defining how an executor's admission controller responds when capacity limits are saturated.
 */
public enum BackpressureStrategy {

    /**
     * Blocks the calling thread until an execution permit becomes available.
     */
    BLOCK,

    /**
     * Immediately rejects new task admissions and throws {@link com.github.f442y.dispersion.exception.BackpressureException}.
     */
    REJECT_IMMEDIATELY,

    /**
     * Waits up to a configured timeout duration for an execution permit before throwing
     * {@link com.github.f442y.dispersion.exception.BackpressureException}.
     */
    WAIT_WITH_TIMEOUT
}
