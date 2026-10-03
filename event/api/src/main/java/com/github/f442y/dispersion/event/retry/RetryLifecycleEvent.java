package com.github.f442y.dispersion.event.retry;

import com.github.f442y.dispersion.event.EventTier;
import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

/**
 * Category interface for events emitted during fault-tolerant state retries and backoff.
 */
public sealed interface RetryLifecycleEvent extends ExecutionEvent
        permits RetryAttemptedEvent, RetryExhaustedEvent {

    @Override
    @NonNull
    default EventTier tier() {
        return EventTier.GRANULAR;
    }
}
