package com.github.f442y.dispersion.event.guard;

import com.github.f442y.dispersion.event.ExecutionEvent;

/**
 * Category interface for execution safety guard events such as loop threshold and circuit breaker triggers.
 */
public sealed interface ExecutionGuardEvent extends ExecutionEvent
        permits StateVisitLimitExceededEvent, CircuitBreakerTrippedEvent {
}
