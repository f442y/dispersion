package com.github.f442y.dispersion.control.core;

import com.github.f442y.dispersion.control.TraceTimelineProvider;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.LocalExecutionTraceBuffer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory implementation of {@link TraceTimelineProvider} providing zero-copy, non-blocking
 * timeline lookups for monolithic and local node deployments.
 *
 * <p>Can delegate directly to an existing worker-local {@link LocalExecutionTraceBuffer} (e.g. from
 * {@link com.github.f442y.dispersion.event.EventBus#traceBuffer()}) or manage an internal non-pinning
 * ring buffer.</p>
 */
public class InMemoryTraceTimelineProvider implements TraceTimelineProvider {

    private final @Nullable LocalExecutionTraceBuffer delegateBuffer;
    private final Map<UUID, InternalTraceEntry> internalTraces;
    private final int defaultEventsPerExecution;

    public InMemoryTraceTimelineProvider() {
        this(LocalExecutionTraceBuffer.DEFAULT_EVENTS_PER_EXECUTION);
    }

    public InMemoryTraceTimelineProvider(int defaultEventsPerExecution) {
        this.delegateBuffer = null;
        this.defaultEventsPerExecution = Math.max(1, defaultEventsPerExecution);
        this.internalTraces = new ConcurrentHashMap<>();
    }

    public InMemoryTraceTimelineProvider(@NonNull LocalExecutionTraceBuffer delegateBuffer) {
        this(delegateBuffer, LocalExecutionTraceBuffer.DEFAULT_EVENTS_PER_EXECUTION);
    }

    public InMemoryTraceTimelineProvider(
            @NonNull LocalExecutionTraceBuffer delegateBuffer,
            int defaultEventsPerExecution
    ) {
        this.delegateBuffer = Objects.requireNonNull(delegateBuffer, "delegateBuffer must not be null");
        this.defaultEventsPerExecution = Math.max(1, defaultEventsPerExecution);
        this.internalTraces = null;
    }

    /**
     * Records an execution event into the timeline buffer.
     *
     * @param event The event to record
     */
    public void record(@NonNull ExecutionEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        if (delegateBuffer != null) {
            delegateBuffer.record(event);
            return;
        }

        if (internalTraces != null) {
            UUID machineId = event.machineId();
            InternalTraceEntry entry = internalTraces.computeIfAbsent(
                    machineId,
                    _ -> new InternalTraceEntry(defaultEventsPerExecution)
            );
            entry.add(event);
        }
    }

    @Override
    @NonNull
    public CompletableFuture<List<ExecutionEvent>> fetchTimeline(@NonNull UUID machineId, int limit) {
        return CompletableFuture.completedFuture(getTimeline(machineId, limit));
    }

    @Override
    @NonNull
    public List<ExecutionEvent> getTimeline(@NonNull UUID machineId, int limit) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        if (limit <= 0) {
            return Collections.emptyList();
        }

        if (delegateBuffer != null) {
            return delegateBuffer.getTrace(machineId, limit);
        }

        if (internalTraces != null) {
            InternalTraceEntry entry = internalTraces.get(machineId);
            if (entry != null) {
                return entry.snapshot(limit);
            }
        }

        return Collections.emptyList();
    }

    public @Nullable LocalExecutionTraceBuffer delegateBuffer() {
        return delegateBuffer;
    }

    private static final class InternalTraceEntry {
        private final int capacity;
        private final Deque<ExecutionEvent> events = new ArrayDeque<>();
        private final ReentrantLock lock = new ReentrantLock();

        InternalTraceEntry(int capacity) {
            this.capacity = capacity;
        }

        void add(@NonNull ExecutionEvent event) {
            lock.lock();
            try {
                if (events.size() >= capacity) {
                    events.pollFirst();
                }
                events.addLast(event);
            } finally {
                lock.unlock();
            }
        }

        @NonNull
        List<ExecutionEvent> snapshot(int limit) {
            lock.lock();
            try {
                int size = events.size();
                int start = Math.max(0, size - limit);
                List<ExecutionEvent> list = new ArrayList<>(size - start);
                int idx = 0;
                for (ExecutionEvent e : events) {
                    if (idx >= start) {
                        list.add(e);
                    }
                    idx++;
                }
                return Collections.unmodifiableList(list);
            } finally {
                lock.unlock();
            }
        }
    }
}
