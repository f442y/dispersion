package com.github.f442y.dispersion.event.signal;

import com.github.f442y.dispersion.event.EventTier;
import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

/**
 * Category interface for events emitted during asynchronous external signal processing.
 */
public sealed interface SignalEvent extends ExecutionEvent
        permits SignalAwaitedEvent, SignalDeliveredEvent, SignalTimedOutEvent, SignalDiscardedEvent {

    @Override
    @NonNull
    default EventTier tier() {
        return EventTier.GRANULAR;
    }
}
