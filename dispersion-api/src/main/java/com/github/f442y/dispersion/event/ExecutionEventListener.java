package com.github.f442y.dispersion.event;

import org.jspecify.annotations.NonNull;

/**
 * Functional listener interface for observing lifecycle and transition events
 * across state machine executions.
 *
 * <p>Implementations can process events synchronously or delegate to an asynchronous
 * dispatcher such as {@code AsyncExecutionEventDispatcher} to isolate the virtual thread
 * hot-path from external telemetry or storage latency.</p>
 */
@FunctionalInterface
public interface ExecutionEventListener {

    /**
     * Invoked when an {@link ExecutionEvent} is emitted by an executing state machine.
     *
     * @param event The immutable lifecycle event
     */
    void onEvent(@NonNull ExecutionEvent event);

    /**
     * Returns a no-op listener that ignores all emitted events.
     *
     * @return A no-op {@link ExecutionEventListener}
     */
    @NonNull
    static ExecutionEventListener noop() {
        return event -> {};
    }
}
