package com.github.f442y.dispersion.event.control;

import com.github.f442y.dispersion.event.EventTier;
import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

/**
 * Category interface for control-plane management events (pause, resume, cancel).
 */
public sealed interface ControlPlaneEvent extends ExecutionEvent
        permits ExecutionCancelledEvent, ExecutionPausedEvent, ExecutionResumedEvent {

    @Override
    @NonNull
    default EventTier tier() {
        return EventTier.LIFECYCLE;
    }
}
