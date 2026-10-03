package com.github.f442y.dispersion.control;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.LocalExecutionTraceBuffer;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Service Provider Interface (SPI) for querying chronological execution event timelines.
 *
 * <p>Decouples the Control Plane's lean {@code O(executions)} summary index from granular event logs.
 * Implementations may fetch directly from local worker memory (e.g. {@link LocalExecutionTraceBuffer}),
 * query remote worker nodes over HTTP/gRPC, or load historical traces from durable checkpoint storage.</p>
 */
public interface TraceTimelineProvider {

    /**
     * Default number of events to retrieve when no limit is explicitly specified.
     */
    int DEFAULT_LIMIT = 100;

    /**
     * Creates a read-only {@link TraceTimelineProvider} backed directly by a {@link LocalExecutionTraceBuffer}.
     *
     * @param buffer The underlying trace buffer to query
     * @return A timeline provider querying the given buffer
     */
    static @NonNull TraceTimelineProvider from(@NonNull LocalExecutionTraceBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer must not be null");
        return new TraceTimelineProvider() {
            @Override
            public @NonNull CompletableFuture<List<ExecutionEvent>> fetchTimeline(@NonNull UUID machineId, int limit) {
                return CompletableFuture.completedFuture(getTimeline(machineId, limit));
            }

            @Override
            public @NonNull List<ExecutionEvent> getTimeline(@NonNull UUID machineId, int limit) {
                return buffer.getTrace(machineId, limit);
            }
        };
    }

    /**
     * Asynchronously retrieves up to {@code limit} chronological events for the given state machine execution.
     *
     * @param machineId The UUID of the state machine execution
     * @param limit     The maximum number of recent events to retrieve
     * @return A {@link CompletableFuture} completing with an ordered list of {@link ExecutionEvent} instances
     */
    @NonNull
    CompletableFuture<List<ExecutionEvent>> fetchTimeline(@NonNull UUID machineId, int limit);

    /**
     * Synchronously retrieves up to {@code limit} chronological events for the given state machine execution.
     *
     * @param machineId The UUID of the state machine execution
     * @param limit     The maximum number of recent events to retrieve
     * @return An ordered list of {@link ExecutionEvent} instances
     */
    default @NonNull List<ExecutionEvent> getTimeline(@NonNull UUID machineId, int limit) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        return fetchTimeline(machineId, limit).join();
    }

    /**
     * Synchronously retrieves up to {@link #DEFAULT_LIMIT} chronological events for the given state machine execution.
     *
     * @param machineId The UUID of the state machine execution
     * @return An ordered list of {@link ExecutionEvent} instances
     */
    default @NonNull List<ExecutionEvent> getTimeline(@NonNull UUID machineId) {
        return getTimeline(machineId, DEFAULT_LIMIT);
    }
}
