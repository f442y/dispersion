package com.github.f442y.dispersion.event.guard;

import com.github.f442y.dispersion.event.EventTier;
import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

/**
 * Category interface for execution safety guard events such as loop threshold and circuit breaker triggers.
 */
public sealed interface ExecutionGuardEvent extends ExecutionEvent
        permits StateVisitLimitExceededEvent, CircuitBreakerTrippedEvent {

    @Override
    @NonNull
    default EventTier tier() {
        return EventTier.GRANULAR;
    }
}
