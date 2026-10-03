package com.github.f442y.dispersion.event.bus;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.LocalExecutionTraceBuffer;
import com.github.f442y.dispersion.event.control.ExecutionCancelledEvent;
import com.github.f442y.dispersion.event.turn.TurnCompensatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnFailedEvent;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe, non-carrier-pinning circular trace buffer storing the last $N$ execution events
 * per state machine on worker nodes with an active background sweeper and hard capacity protection.
 *
 * <p>Each execution maintains an isolated bounded deque guarded by an independent {@link ReentrantLock},
 * preventing contention across concurrent workflows. Completed, failed, or cancelled executions
 * are tagged as terminal and automatically evicted when their time-to-live expires via a background
 * Virtual Thread sweeper. If total execution count exceeds {@code maxMachines}, expired traces are pruned
 * immediately, and any remaining excess is evicted starting with terminal and oldest entries.</p>
 */
public final class ConcurrentRingBufferTraceBuffer implements LocalExecutionTraceBuffer {

    public static final int DEFAULT_MAX_MACHINES = 50_000;

    private final int eventsPerExecution;
    private final Duration terminalTtl;
    private final int maxMachines;
    private final Duration sweepInterval;
    private final ConcurrentHashMap<UUID, MachineTraceEntry> traces = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread sweeperThread;

    public ConcurrentRingBufferTraceBuffer() {
        this(new Builder());
    }

    private ConcurrentRingBufferTraceBuffer(@NonNull Builder builder) {
        this.eventsPerExecution = builder.eventsPerExecution;
        this.terminalTtl = builder.terminalTtl;
        this.maxMachines = builder.maxMachines;
        this.sweepInterval = builder.sweepInterval;

        if (this.sweepInterval != null && !this.sweepInterval.isZero() && !this.sweepInterval.isNegative()) {
            this.sweeperThread = Thread.ofVirtual()
                    .name("dispersion-trace-buffer-sweeper")
                    .start(this::sweepLoop);
        } else {
            this.sweeperThread = null;
        }
    }

    public static @NonNull Builder builder() {
        return new Builder();
    }

    private void sweepLoop() {
        while (!closed.get()) {
            try {
                Thread.sleep(sweepInterval);
                pruneExpired();
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    @Override
    public void record(@NonNull ExecutionEvent event) {
        if (closed.get()) {
            return;
        }
        Objects.requireNonNull(event, "event must not be null");
        UUID machineId = event.machineId();

        MachineTraceEntry entry = traces.computeIfAbsent(machineId, id -> new MachineTraceEntry(eventsPerExecution));
        entry.record(event);

        if (traces.size() > maxMachines) {
            pruneExpired();
            if (traces.size() > maxMachines) {
                evictOldest();
            }
        }
    }

    private void evictOldest() {
        int excess = traces.size() - maxMachines;
        if (excess <= 0) {
            return;
        }

        List<Map.Entry<UUID, MachineTraceEntry>> toRemove = traces.entrySet().stream()
                .sorted(Comparator.<Map.Entry<UUID, MachineTraceEntry>, Boolean>comparing(e -> !e.getValue().isTerminal())
                        .thenComparing(e -> e.getValue().lastUpdated()))
                .limit(excess)
                .toList();

        for (Map.Entry<UUID, MachineTraceEntry> entry : toRemove) {
            traces.remove(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public @NonNull List<ExecutionEvent> getTrace(@NonNull UUID machineId) {
        return getTrace(machineId, Integer.MAX_VALUE);
    }

    @Override
    public @NonNull List<ExecutionEvent> getTrace(@NonNull UUID machineId, int limit) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        if (limit <= 0) {
            return Collections.emptyList();
        }

        MachineTraceEntry entry = traces.get(machineId);
        if (entry == null) {
            return Collections.emptyList();
        }

        return entry.snapshot(limit);
    }

    @Override
    public boolean containsTrace(@NonNull UUID machineId) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        return traces.containsKey(machineId);
    }

    @Override
    public void evict(@NonNull UUID machineId) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        traces.remove(machineId);
    }

    @Override
    public int size() {
        return traces.size();
    }

    @Override
    public void pruneExpired() {
        Instant now = Instant.now();
        traces.entrySet().removeIf(e -> e.getValue().isExpired(now, terminalTtl));
    }

    @Override
    public void clear() {
        traces.clear();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (sweeperThread != null) {
                sweeperThread.interrupt();
            }
            traces.clear();
        }
    }

    public int eventsPerExecution() {
        return eventsPerExecution;
    }

    public @NonNull Duration terminalTtl() {
        return terminalTtl;
    }

    public int maxMachines() {
        return maxMachines;
    }

    public @Nullable Duration sweepInterval() {
        return sweepInterval;
    }

    public boolean isClosed() {
        return closed.get();
    }

    private static final class MachineTraceEntry {
        private final int capacity;
        private final Deque<ExecutionEvent> ring;
        private final ReentrantLock lock = new ReentrantLock();
        private volatile boolean terminal;
        private volatile Instant terminalAt;
        private volatile Instant lastUpdated;

        private MachineTraceEntry(int capacity) {
            this.capacity = capacity;
            this.ring = new ArrayDeque<>(Math.min(capacity, 128));
            this.lastUpdated = Instant.now();
        }

        private void record(@NonNull ExecutionEvent event) {
            lock.lock();
            try {
                if (ring.size() >= capacity) {
                    ring.pollFirst();
                }
                ring.addLast(event);
                lastUpdated = (event.timestamp() != null) ? event.timestamp() : Instant.now();

                if (isTerminalEvent(event)) {
                    terminal = true;
                    terminalAt = lastUpdated;
                }
            } finally {
                lock.unlock();
            }
        }

        private @NonNull List<ExecutionEvent> snapshot(int limit) {
            lock.lock();
            try {
                if (ring.isEmpty()) {
                    return Collections.emptyList();
                }

                List<ExecutionEvent> list = new ArrayList<>(ring);
                if (limit < list.size()) {
                    return Collections.unmodifiableList(list.subList(list.size() - limit, list.size()));
                }
                return Collections.unmodifiableList(list);
            } finally {
                lock.unlock();
            }
        }

        private boolean isTerminal() {
            return terminal;
        }

        private @NonNull Instant lastUpdated() {
            return lastUpdated;
        }

        private boolean isExpired(@NonNull Instant now, @NonNull Duration ttl) {
            if (!terminal || terminalAt == null) {
                return false;
            }
            return now.isAfter(terminalAt.plus(ttl));
        }

        private static boolean isTerminalEvent(@NonNull ExecutionEvent event) {
            return event instanceof TurnCompletedEvent
                    || event instanceof TurnFailedEvent
                    || event instanceof TurnCompensatedEvent
                    || event instanceof ExecutionCancelledEvent;
        }
    }

    public static final class Builder {
        private int eventsPerExecution = DEFAULT_EVENTS_PER_EXECUTION;
        private Duration terminalTtl = DEFAULT_TERMINAL_TTL;
        private int maxMachines = DEFAULT_MAX_MACHINES;
        private Duration sweepInterval = DEFAULT_SWEEP_INTERVAL;

        public Builder() {
        }

        public @NonNull Builder eventsPerExecution(int eventsPerExecution) {
            if (eventsPerExecution <= 0) {
                throw new IllegalArgumentException("eventsPerExecution must be positive");
            }
            this.eventsPerExecution = eventsPerExecution;
            return this;
        }

        public @NonNull Builder terminalTtl(@NonNull Duration terminalTtl) {
            this.terminalTtl = Objects.requireNonNull(terminalTtl, "terminalTtl must not be null");
            return this;
        }

        public @NonNull Builder maxMachines(int maxMachines) {
            if (maxMachines <= 0) {
                throw new IllegalArgumentException("maxMachines must be positive");
            }
            this.maxMachines = maxMachines;
            return this;
        }

        public @NonNull Builder sweepInterval(@Nullable Duration sweepInterval) {
            this.sweepInterval = sweepInterval;
            return this;
        }

        public @NonNull ConcurrentRingBufferTraceBuffer build() {
            return new ConcurrentRingBufferTraceBuffer(this);
        }
    }
}
