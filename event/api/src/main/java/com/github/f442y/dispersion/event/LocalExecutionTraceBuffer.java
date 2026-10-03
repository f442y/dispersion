package com.github.f442y.dispersion.event;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Worker-local flight recorder buffer storing recent bounded execution traces
 * for active and recently terminated state machines.
 *
 * <p>Enables low-overhead, on-demand event drill-down queries for the Control Plane
 * without flooding global telemetry channels with high-frequency micro-events.</p>
 */
public interface LocalExecutionTraceBuffer extends AutoCloseable {

    /**
     * Default number of events retained per execution trace buffer.
     */
    int DEFAULT_EVENTS_PER_EXECUTION = 100;

    /**
     * Default time-to-live for terminal execution traces before eviction.
     */
    Duration DEFAULT_TERMINAL_TTL = Duration.ofMinutes(5);

    /**
     * Default interval between background active eviction sweeps.
     */
    Duration DEFAULT_SWEEP_INTERVAL = Duration.ofSeconds(30);

    /**
     * Records an execution event into the machine-specific ring buffer.
     *
     * @param event The event to record
     */
    void record(@NonNull ExecutionEvent event);

    /**
     * Retrieves all recorded events for the specified machine ID in chronological order.
     *
     * @param machineId The UUID of the state machine execution
     * @return An unmodifiable list of chronological events, or an empty list if not found
     */
    @NonNull
    List<ExecutionEvent> getTrace(@NonNull UUID machineId);

    /**
     * Retrieves up to {@code limit} most recent recorded events for the specified machine ID
     * in chronological order.
     *
     * @param machineId The UUID of the state machine execution
     * @param limit     The maximum number of recent events to return
     * @return An unmodifiable list of chronological events, or an empty list if not found
     */
    @NonNull
    List<ExecutionEvent> getTrace(@NonNull UUID machineId, int limit);

    /**
     * Checks if an execution trace exists for the specified machine ID.
     *
     * @param machineId The UUID of the state machine execution
     * @return {@code true} if a trace exists
     */
    boolean containsTrace(@NonNull UUID machineId);

    /**
     * Explicitly evicts the execution trace for the specified machine ID.
     *
     * @param machineId The UUID of the state machine execution
     */
    void evict(@NonNull UUID machineId);

    /**
     * Returns the total count of distinct machine traces currently buffered.
     *
     * @return The count of distinct machine traces
     */
    int size();

    /**
     * Prunes expired terminal execution traces whose retention TTL has elapsed.
     */
    void pruneExpired();

    /**
     * Clears all buffered traces.
     */
    void clear();

    /**
     * Closes the trace buffer, terminating background sweeper threads and clearing state.
     */
    @Override
    default void close() {
    }
}
