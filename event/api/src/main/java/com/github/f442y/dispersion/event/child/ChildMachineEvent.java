package com.github.f442y.dispersion.event.child;

import com.github.f442y.dispersion.event.EventTier;
import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

/**
 * Category interface for events emitted during hierarchical sub-workflow (child machine) execution.
 */
public sealed interface ChildMachineEvent extends ExecutionEvent
        permits ChildMachineSpawnedEvent, ChildMachineCompletedEvent {

    @Override
    @NonNull
    default EventTier tier() {
        return EventTier.GRANULAR;
    }
}
