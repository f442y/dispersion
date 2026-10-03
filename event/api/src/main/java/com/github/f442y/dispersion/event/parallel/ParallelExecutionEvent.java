package com.github.f442y.dispersion.event.parallel;

import com.github.f442y.dispersion.event.EventTier;
import com.github.f442y.dispersion.event.ExecutionEvent;
import org.jspecify.annotations.NonNull;

/**
 * Category interface for events emitted during parallel forking, execution, and joining.
 */
public sealed interface ParallelExecutionEvent extends ExecutionEvent
        permits ParallelForkStartedEvent, ParallelBranchCompletedEvent, ParallelJoinCompletedEvent {

    @Override
    @NonNull
    default EventTier tier() {
        return EventTier.GRANULAR;
    }
}
