package com.github.f442y.dispersion.event;

/**
 * Telemetry tier classification for execution events.
 *
 * <p>Tier demarcation governs routing, serialization, and ingestion:
 * <ul>
 *     <li>{@link #LIFECYCLE}: High-level state transitions and control plane events that are
 *     always ingested by the control plane to update execution summaries and broadcast
 *     on the global SSE stream ($O(\text{Executions})$ memory footprint).</li>
 *     <li>{@link #GRANULAR}: High-frequency intermediate micro-events (state visits, action
 *     evaluations, signal queues, retry backoffs) buffered locally at the worker edge
 *     and only streamed on demand when an active live tap exists.</li>
 * </ul>
 * </p>
 */
public enum EventTier {
    /**
     * Primary lifecycle event representing execution turns, completions, failures, or operator commands.
     */
    LIFECYCLE,

    /**
     * Detailed micro-event representing internal state steps, actions, retries, or signal interactions.
     */
    GRANULAR
}
