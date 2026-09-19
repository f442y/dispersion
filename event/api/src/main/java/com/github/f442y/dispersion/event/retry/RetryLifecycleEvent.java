package com.github.f442y.dispersion.event.retry;

import com.github.f442y.dispersion.event.ExecutionEvent;

/**
 * Category interface for events emitted during fault-tolerant state retries and backoff.
 */
public sealed interface RetryLifecycleEvent extends ExecutionEvent
        permits RetryAttemptedEvent, RetryExhaustedEvent {
}
