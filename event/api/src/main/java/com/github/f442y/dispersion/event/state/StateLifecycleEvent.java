package com.github.f442y.dispersion.event.state;

import com.github.f442y.dispersion.event.EventTier;
import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

/**
 * Category interface for events emitted during individual state execution and transition evaluation.
 */
public sealed interface StateLifecycleEvent extends ExecutionEvent
        permits StateEnteredEvent, StateExitedEvent, TransitionEvaluatedEvent, ActionExecutedEvent {

    @Override
    @NonNull
    default EventTier tier() {
        return EventTier.GRANULAR;
    }
}
