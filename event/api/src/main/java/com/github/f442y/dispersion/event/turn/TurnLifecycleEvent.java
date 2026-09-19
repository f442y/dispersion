package com.github.f442y.dispersion.event.turn;

import com.github.f442y.dispersion.event.ExecutionEvent;

/**
 * Category interface for events emitted during the lifecycle of an execution turn.
 */
public sealed interface TurnLifecycleEvent extends ExecutionEvent
        permits TurnStartedEvent, TurnSuspendedEvent, TurnCompletedEvent, TurnCompensatedEvent, TurnFailedEvent {
}
