package com.github.f442y.dispersion.event.signal;

import com.github.f442y.dispersion.event.ExecutionEvent;

/**
 * Category interface for events emitted during asynchronous external signal processing.
 */
public sealed interface SignalEvent extends ExecutionEvent
        permits SignalAwaitedEvent, SignalDeliveredEvent, SignalTimedOutEvent, SignalDiscardedEvent {
}
