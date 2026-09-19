package com.github.f442y.dispersion.event.state;

import com.github.f442y.dispersion.event.ExecutionEvent;

/**
 * Category interface for events emitted during individual state execution and transition evaluation.
 */
public sealed interface StateLifecycleEvent extends ExecutionEvent
        permits StateEnteredEvent, StateExitedEvent, TransitionEvaluatedEvent, ActionExecutedEvent {
}
