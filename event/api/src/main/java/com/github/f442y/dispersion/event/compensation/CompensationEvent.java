package com.github.f442y.dispersion.event.compensation;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.turn.TurnCompensatedEvent;

/**
 * Category interface for events emitted during automated Saga compensation rollbacks.
 */
public sealed interface CompensationEvent extends ExecutionEvent
        permits CompensationStepStartedEvent, CompensationStepCompletedEvent, CompensationStepFailedEvent, TurnCompensatedEvent {
}
