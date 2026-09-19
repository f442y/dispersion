package com.github.f442y.dispersion.event.parallel;

import com.github.f442y.dispersion.event.ExecutionEvent;

/**
 * Category interface for events emitted during parallel forking, execution, and joining.
 */
public sealed interface ParallelExecutionEvent extends ExecutionEvent
        permits ParallelForkStartedEvent, ParallelBranchCompletedEvent, ParallelJoinCompletedEvent {
}
