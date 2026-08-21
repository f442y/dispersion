package com.github.f442y.dispersion.executor;

/**
 * Strategy applied when the concurrency admission controller is saturated.
 */
public enum BackpressureStrategy {
    /**
     * Callers block indefinitely until an admission permit becomes available.
     */
    BLOCK,

    /**
     * Callers immediately receive a {@link com.github.f442y.dispersion.exception.BackpressureException}
     * if no permit is available.
     */
    REJECT_IMMEDIATELY,

    /**
     * Callers wait up to a specified timeout for an admission permit before throwing
     * {@link com.github.f442y.dispersion.exception.BackpressureException}.
     */
    WAIT_WITH_TIMEOUT
}
