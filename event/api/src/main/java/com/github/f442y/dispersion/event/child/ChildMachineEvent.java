package com.github.f442y.dispersion.event.child;

import com.github.f442y.dispersion.event.ExecutionEvent;

/**
 * Category interface for events emitted during hierarchical sub-workflow (child machine) execution.
 */
public sealed interface ChildMachineEvent extends ExecutionEvent
        permits ChildMachineSpawnedEvent, ChildMachineCompletedEvent {
}
