package com.github.f442y.dispersion.control.core;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.TraceTimelineProvider;
import com.github.f442y.dispersion.event.DynamicTapManager;
import com.github.f442y.dispersion.event.EventBus;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.control.ExecutionCancelledEvent;
import com.github.f442y.dispersion.event.control.ExecutionPausedEvent;
import com.github.f442y.dispersion.event.control.ExecutionResumedEvent;
import com.github.f442y.dispersion.event.guard.CircuitBreakerTrippedEvent;
import com.github.f442y.dispersion.event.signal.SignalAwaitedEvent;
import com.github.f442y.dispersion.event.signal.SignalDeliveredEvent;
import com.github.f442y.dispersion.event.signal.SignalTimedOutEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.state.StateExitedEvent;
import com.github.f442y.dispersion.event.state.TransitionEvaluatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompensatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnFailedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.event.turn.TurnSuspendedEvent;
import com.github.f442y.dispersion.routing.InspectableRouter;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Thread-safe default implementation of {@link ControlPlane} engineered with segmented O(1) eviction,
 * decoupled {@link TraceTimelineProvider} event retrieval, and Virtual Thread live streaming.
 *
 * <h2>Segmented O(1) Eviction Architecture</h2>
 * <p>To eliminate full-collection linear scans (which burn CPU on high-frequency telemetry events),
 * execution summaries are strictly partitioned into two pools:</p>
 * <ul>
 *   <li><b>Active Pool ({@code activeExecutions}):</b> Tracks in-flight workflows ({@link ExecutionStatus#RUNNING},
 *       {@link ExecutionStatus#SUSPENDED}, or {@link ExecutionStatus#PAUSED}). Active workflows are critical operational
 *       entities waiting for internal steps or external signals; they are never subject to eviction.</li>
 *   <li><b>Terminal Pool ({@code terminalExecutions}):</b> Tracks finished workflows ({@link ExecutionStatus#COMPLETED},
 *       {@link ExecutionStatus#FAILED}, {@link ExecutionStatus#COMPENSATED}, or {@link ExecutionStatus#CANCELLED}).
 *       When the terminal count exceeds {@code maxTrackedExecutions}, the oldest terminal execution is pruned in
 *       strictly <b>O(1)</b> time via a concurrent FIFO queue.</li>
 * </ul>
 *
 * <h2>Decoupled Trace Timeline Provider</h2>
 * <p>Execution event timelines are decoupled from the Control Plane's lean summary index. Event traces
 * are retrieved on-demand via a registered {@link TraceTimelineProvider} (defaulting to zero-copy
 * worker-local buffers), preserving bounded memory regardless of workflow execution volume.</p>
 */
public class DefaultControlPlane implements ControlPlane, ExecutionEventListener, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultControlPlane.class);

    private final Map<String, InspectableMachine> machines = new ConcurrentHashMap<>();

    // Partitioned execution pools
    private final Map<String, ExecutionSummary> activeExecutions = new ConcurrentHashMap<>();
    private final Map<String, ExecutionSummary> terminalExecutions = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> terminalEvictionOrder = new ConcurrentLinkedQueue<>();
    private final AtomicInteger terminalCount = new AtomicInteger(0);

    // Striped locks for thread-safe summary updates without global contention
    private final Object[] executionLocks = new Object[256];

    // Decoupled timeline provider
    private volatile TraceTimelineProvider timelineProvider;
    private volatile @Nullable DynamicTapManager tapManager;
    private volatile boolean attachedToBus;

    // Global recent events with O(1) atomic sizing
    private final Deque<ExecutionEvent> recentEvents = new ConcurrentLinkedDeque<>();
    private final AtomicInteger recentCount = new AtomicInteger(0);

    // Live streaming hub
    private final List<ControlPlaneStream> activeStreams = new CopyOnWriteArrayList<>();

    private final int maxTrackedExecutions;
    private final int maxTimelineEventsPerExecution;
    private final int maxRecentEvents;
    private volatile InspectableRouter router;

    /**
     * Creates a Control Plane with standard capacity defaults (10,000 terminal executions, 100 timeline events, 2,000 recent events).
     */
    public DefaultControlPlane() {
        this(10_000, 100, 2_000, null);
    }

    /**
     * Creates a Control Plane with custom buffer capacities.
     */
    public DefaultControlPlane(
            int maxTrackedExecutions,
            int maxTimelineEventsPerExecution,
            int maxRecentEvents
    ) {
        this(maxTrackedExecutions, maxTimelineEventsPerExecution, maxRecentEvents, null);
    }

    /**
     * Creates a Control Plane with custom buffer capacities and an explicit {@link TraceTimelineProvider}.
     */
    public DefaultControlPlane(
            int maxTrackedExecutions,
            int maxTimelineEventsPerExecution,
            int maxRecentEvents,
            @Nullable TraceTimelineProvider timelineProvider
    ) {
        if (maxTrackedExecutions <= 0 || maxTimelineEventsPerExecution <= 0 || maxRecentEvents <= 0) {
            throw new IllegalArgumentException("Capacities must be strictly positive");
        }
        this.maxTrackedExecutions = maxTrackedExecutions;
        this.maxTimelineEventsPerExecution = maxTimelineEventsPerExecution;
        this.maxRecentEvents = maxRecentEvents;
        this.timelineProvider = (timelineProvider != null)
                ? timelineProvider
                : new InMemoryTraceTimelineProvider(maxTimelineEventsPerExecution);

        for (int i = 0; i < executionLocks.length; i++) {
            executionLocks[i] = new Object();
        }
    }

    // =========================================================================
    // Registration & Topology Discovery
    // =========================================================================

    @Override
    @NonNull
    public ControlPlane registerRouter(@NonNull InspectableRouter router) {
        this.router = Objects.requireNonNull(router, "router must not be null");
        log.atInfo()
                .addKeyValue("services_count", router.registeredServices().size())
                .log("Registered InspectableRouter in ControlPlane");
        return this;
    }

    @Override
    @NonNull
    public Optional<InspectableRouter> getRouter() {
        return Optional.ofNullable(router);
    }

    @Override
    @NonNull
    public DefaultControlPlane register(@NonNull InspectableMachine machine) {
        Objects.requireNonNull(machine, "machine must not be null");
        String name = machine.descriptor().name();
        machines.put(name, machine);
        log.atInfo()
                .addKeyValue("machine_name", name)
                .log("Registered InspectableMachine in Control Plane");
        return this;
    }

    @Override
    public boolean unregister(@NonNull String machineName) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        return machines.remove(machineName) != null;
    }

    /**
     * Registers a generic state machine topology descriptor with a custom signal router.
     */
    public DefaultControlPlane register(
            @NonNull MachineDescriptor descriptor,
            @Nullable SignalRouter signalRouter
    ) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        InspectableMachine inspectable = new InspectableMachine() {
            @Override
            @NonNull
            public MachineDescriptor descriptor() {
                return descriptor;
            }

            @Override
            @NonNull
            public CompletableFuture<SignalDeliveryResult> sendSignal(
                    @NonNull String correlationKey,
                    @NonNull String signalName,
                    @Nullable Object payload
            ) {
                if (signalRouter != null) {
                    return signalRouter.routeSignal(correlationKey, signalName, payload != null ? payload : new Object());
                }
                return CompletableFuture.completedFuture(
                        SignalDeliveryResult.failure(descriptor.name(), correlationKey, signalName, "No signal router configured")
                );
            }

            @Override
            @NonNull
            public Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.empty();
            }
        };

        return register(inspectable);
    }

    /**
     * Registers a custom {@link TraceTimelineProvider} for querying execution event traces.
     */
    @NonNull
    public DefaultControlPlane registerTimelineProvider(@NonNull TraceTimelineProvider timelineProvider) {
        this.timelineProvider = Objects.requireNonNull(timelineProvider, "timelineProvider must not be null");
        return this;
    }

    @Override
    @NonNull
    public Optional<TraceTimelineProvider> getTimelineProvider() {
        return Optional.ofNullable(timelineProvider);
    }

    /**
     * Registers a {@link DynamicTapManager} for managing live execution tap leases.
     */
    @NonNull
    public DefaultControlPlane registerTapManager(@NonNull DynamicTapManager tapManager) {
        this.tapManager = Objects.requireNonNull(tapManager, "tapManager must not be null");
        return this;
    }

    @Override
    @NonNull
    public Optional<DynamicTapManager> getTapManager() {
        return Optional.ofNullable(tapManager);
    }

    /**
     * Wires this Control Plane directly as a listener to the given event bus.
     * If the event bus provides a local trace buffer, it is automatically bound as the timeline provider.
     */
    public DefaultControlPlane attachTo(@NonNull EventBus eventBus) {
        Objects.requireNonNull(eventBus, "eventBus must not be null");
        this.attachedToBus = true;
        eventBus.subscribe(this);
        if (eventBus.traceBuffer() != null) {
            this.timelineProvider = TraceTimelineProvider.from(eventBus.traceBuffer());
        }
        if (eventBus.tapManager() != null) {
            this.tapManager = eventBus.tapManager();
        }
        return this;
    }

    // =========================================================================
    // ControlPlane Query SPI Implementation
    // =========================================================================

    @Override
    @NonNull
    public List<MachineDescriptor> listMachines() {
        return machines.values().stream()
                .map(InspectableMachine::descriptor)
                .toList();
    }

    @Override
    @NonNull
    public Optional<MachineDescriptor> getMachine(@NonNull String machineName) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        InspectableMachine reg = machines.get(machineName);
        return reg != null ? Optional.of(reg.descriptor()) : Optional.empty();
    }

    @Override
    @NonNull
    public Optional<ExecutionSummary> getExecution(@NonNull String executionId) {
        Objects.requireNonNull(executionId, "executionId must not be null");
        ExecutionSummary summary = activeExecutions.get(executionId);
        if (summary == null) {
            summary = terminalExecutions.get(executionId);
        }
        return Optional.ofNullable(summary);
    }

    @Override
    @NonNull
    public List<ExecutionSummary> listExecutions(
            @Nullable String machineName,
            @Nullable ExecutionStatus status,
            int limit
    ) {
        int max = limit <= 0 ? 50 : limit;

        Stream<ExecutionSummary> stream;
        if (status != null) {
            if (isTerminal(status)) {
                stream = terminalExecutions.values().stream();
            } else {
                stream = activeExecutions.values().stream();
            }
        } else {
            stream = Stream.concat(activeExecutions.values().stream(), terminalExecutions.values().stream());
        }

        return stream
                .filter(e -> machineName == null || e.machineName().equals(machineName))
                .filter(e -> status == null || e.status() == status)
                .sorted((a, b) -> b.lastUpdated().compareTo(a.lastUpdated()))
                .limit(max)
                .toList();
    }

    @Override
    @NonNull
    public List<ExecutionEvent> getExecutionTimeline(@NonNull String executionId) {
        return getExecutionTimeline(executionId, maxTimelineEventsPerExecution);
    }

    @Override
    @NonNull
    public List<ExecutionEvent> getExecutionTimeline(@NonNull String executionId, int limit) {
        Objects.requireNonNull(executionId, "executionId must not be null");
        if (limit <= 0) {
            return Collections.emptyList();
        }
        if (timelineProvider != null) {
            try {
                UUID machineId = UUID.fromString(executionId);
                return timelineProvider.getTimeline(machineId, limit);
            } catch (IllegalArgumentException _) {
                log.atWarn()
                        .addKeyValue("execution_id", executionId)
                        .log("Invalid executionId UUID format for timeline lookup");
                return Collections.emptyList();
            }
        }
        return Collections.emptyList();
    }

    @Override
    @NonNull
    public CompletableFuture<List<ExecutionEvent>> fetchExecutionTimeline(@NonNull String executionId, int limit) {
        Objects.requireNonNull(executionId, "executionId must not be null");
        if (limit <= 0) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        if (timelineProvider != null) {
            try {
                UUID machineId = UUID.fromString(executionId);
                return timelineProvider.fetchTimeline(machineId, limit);
            } catch (IllegalArgumentException _) {
                log.atWarn()
                        .addKeyValue("execution_id", executionId)
                        .log("Invalid executionId UUID format for timeline lookup");
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    @NonNull
    public List<ExecutionEvent> getRecentEvents(@Nullable String machineName, int limit) {
        int max = limit <= 0 ? 100 : limit;
        return recentEvents.stream()
                .filter(e -> machineName == null || e.machineName().equals(machineName))
                .limit(max)
                .toList();
    }

    @Override
    @NonNull
    public ExecutionEventListener getEventListener() {
        return this;
    }

    /**
     * Retrieves recent events across all machines.
     */
    @NonNull
    public List<ExecutionEvent> getRecentEvents(int limit) {
        return getRecentEvents(null, limit);
    }

    /**
     * Retrieves recent events for a specific machine name.
     */
    @NonNull
    public List<ExecutionEvent> getRecentEventsForMachine(@NonNull String machineName, int limit) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        return getRecentEvents(machineName, limit);
    }

    @Override
    @NonNull
    public Optional<Object> inspectCheckpoint(@NonNull String machineName, @NonNull String correlationKey) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        InspectableMachine machine = machines.get(machineName);
        if (machine == null) {
            return Optional.empty();
        }
        return machine.inspectCheckpoint(correlationKey);
    }

    @Override
    @NonNull
    public <CHECKPOINT_TYPE> Optional<CHECKPOINT_TYPE> inspectCheckpoint(
            @NonNull String machineName,
            @NonNull String correlationKey,
            @NonNull Class<CHECKPOINT_TYPE> checkpointClass
    ) {
        return inspectCheckpoint(machineName, correlationKey)
                .filter(checkpointClass::isInstance)
                .map(checkpointClass::cast);
    }

    // =========================================================================
    // Live Streaming Observability
    // =========================================================================

    @Override
    @NonNull
    public EventStream watchExecution(@NonNull String executionId) {
        Objects.requireNonNull(executionId, "executionId must not be null");
        return openStream(event -> executionId.equals(event.machineId().toString()));
    }

    @Override
    @NonNull
    public EventStream watchMachine(@NonNull String machineName) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        return openStream(event -> machineName.equals(event.machineName()));
    }

    @Override
    @NonNull
    public EventStream watchAll() {
        return openStream(_ -> true);
    }

    @Override
    @NonNull
    public EventStream openStream(@NonNull Predicate<ExecutionEvent> filter) {
        Objects.requireNonNull(filter, "filter must not be null");
        ControlPlaneStream stream = new ControlPlaneStream(filter, activeStreams::remove);
        activeStreams.add(stream);
        return stream;
    }

    // =========================================================================
    // Signal Dispatching & Execution Triggers
    // =========================================================================

    @Override
    @NonNull
    public CompletableFuture<Object> dispatchExecution(@NonNull String machineName, @Nullable Object input) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        InspectableMachine machine = machines.get(machineName);
        if (machine == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Machine [" + machineName + "] is not registered in Control Plane"));
        }
        return machine.dispatchExecution(input);
    }

    @Override
    @NonNull
    public CompletableFuture<SignalDeliveryResult> sendSignal(
            @NonNull String machineName,
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object payload
    ) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");

        InspectableMachine machine = machines.get(machineName);
        if (machine == null) {
            return CompletableFuture.completedFuture(
                    SignalDeliveryResult.failure(machineName, correlationKey, signalName, "Machine [" + machineName + "] is not registered in Control Plane")
            );
        }

        return machine.sendSignal(correlationKey, signalName, payload);
    }

    // =========================================================================
    // Event Ingestion & Summary Aggregation
    // =========================================================================

    @Override
    public void onEvent(@NonNull ExecutionEvent event) {
        Objects.requireNonNull(event, "event must not be null");

        // 1. Record in global recent ring buffer with O(1) size check
        recentEvents.addFirst(event);
        if (recentCount.incrementAndGet() > maxRecentEvents) {
            if (recentEvents.pollLast() != null) {
                recentCount.decrementAndGet();
            }
        }

        // 2. Delegate to local timeline provider only if standalone and not attached to an EventBus
        if (!attachedToBus && timelineProvider instanceof InMemoryTraceTimelineProvider inMem) {
            inMem.record(event);
        }

        // 3. Update execution summary snapshot with partitioned O(1) eviction
        updateExecutionSummary(event);

        // 4. Publish to active live streams
        for (ControlPlaneStream stream : activeStreams) {
            try {
                stream.offer(event);
            } catch (Exception ex) {
                log.atError()
                        .setCause(ex)
                        .log("Error offering event to ControlPlaneStream");
            }
        }
    }

    private void updateExecutionSummary(@NonNull ExecutionEvent event) {
        String execId = event.machineId().toString();
        String machine = event.machineName();
        Instant time = event.timestamp();

        int stripe = (execId.hashCode() & 0x7FFFFFFF) % executionLocks.length;
        synchronized (executionLocks[stripe]) {
            ExecutionSummary current = activeExecutions.get(execId);
            if (current == null) {
                current = terminalExecutions.get(execId);
            }

            ExecutionSummary updated = computeNextSummary(current, execId, machine, time, event);
            if (updated != null) {
                if (isTerminal(updated.status())) {
                    activeExecutions.remove(execId);
                    boolean isNewTerminal = (terminalExecutions.put(execId, updated) == null);
                    if (isNewTerminal) {
                        terminalEvictionOrder.add(execId);
                        if (terminalCount.incrementAndGet() > maxTrackedExecutions) {
                            evictTerminalExecution();
                        }
                    }
                } else {
                    terminalExecutions.remove(execId);
                    activeExecutions.put(execId, updated);
                }
            }
        }
    }

    private static boolean isTerminal(ExecutionStatus status) {
        return status == ExecutionStatus.COMPLETED
                || status == ExecutionStatus.FAILED
                || status == ExecutionStatus.COMPENSATED
                || status == ExecutionStatus.CANCELLED;
    }

    private void evictTerminalExecution() {
        while (terminalCount.get() > maxTrackedExecutions) {
            String oldestId = terminalEvictionOrder.poll();
            if (oldestId == null) {
                break;
            }
            if (terminalExecutions.remove(oldestId) != null) {
                terminalCount.decrementAndGet();
            }
        }
    }

    private ExecutionSummary computeNextSummary(
            @Nullable ExecutionSummary current,
            @NonNull String execId,
            @NonNull String machine,
            @NonNull Instant time,
            @NonNull ExecutionEvent event
    ) {
        return switch (event) {
            case TurnStartedEvent e -> {
                if (current == null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            "INITIAL",
                            ExecutionStatus.RUNNING,
                            time,
                            null,
                            e.correlationKey(),
                            null,
                            0,
                            time,
                            null
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            current.currentState(),
                            ExecutionStatus.RUNNING,
                            current.startTime(),
                            null,
                            e.correlationKey() != null ? e.correlationKey() : current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            null
                    );
                }
            }
            case StateEnteredEvent e -> {
                if (current == null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.RUNNING,
                            time,
                            null,
                            null,
                            null,
                            0,
                            time,
                            null
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.RUNNING,
                            current.startTime(),
                            null,
                            current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            null
                    );
                }
            }
            case TransitionEvaluatedEvent _ -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            current.currentState(),
                            current.status(),
                            current.startTime(),
                            current.endTime(),
                            current.correlationKey(),
                            current.suspendedSignal(),
                            current.transitionsCount() + 1,
                            time,
                            current.errorMessage()
                    );
                }
                yield null;
            }
            case SignalAwaitedEvent e -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.SUSPENDED,
                            current.startTime(),
                            null,
                            e.correlationKey() != null ? e.correlationKey() : current.correlationKey(),
                            e.expectedSignal(),
                            current.transitionsCount(),
                            time,
                            null
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.SUSPENDED,
                            time,
                            null,
                            e.correlationKey(),
                            e.expectedSignal(),
                            0,
                            time,
                            null
                    );
                }
            }
            case TurnSuspendedEvent e -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.SUSPENDED,
                            current.startTime(),
                            null,
                            e.correlationKey() != null ? e.correlationKey() : current.correlationKey(),
                            e.expectedSignal(),
                            current.transitionsCount(),
                            time,
                            null
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.SUSPENDED,
                            time,
                            null,
                            e.correlationKey(),
                            e.expectedSignal(),
                            0,
                            time,
                            null
                    );
                }
            }
            case TurnCompletedEvent e -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.finalStateName() != null ? e.finalStateName() : current.currentState(),
                            ExecutionStatus.COMPLETED,
                            current.startTime(),
                            time,
                            current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            null
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.finalStateName() != null ? e.finalStateName() : "END",
                            ExecutionStatus.COMPLETED,
                            time,
                            time,
                            e.correlationKey(),
                            null,
                            0,
                            time,
                            null
                    );
                }
            }
            case TurnCompensatedEvent e -> {
                String error = e.cause() != null ? e.cause().getMessage() : "Turn compensated";
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.failedStateName(),
                            ExecutionStatus.COMPENSATED,
                            current.startTime(),
                            time,
                            current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            error
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.failedStateName(),
                            ExecutionStatus.COMPENSATED,
                            time,
                            time,
                            e.correlationKey(),
                            null,
                            0,
                            time,
                            error
                    );
                }
            }
            case TurnFailedEvent e -> {
                String error = e.cause().getMessage() != null ? e.cause().getMessage() : e.cause().getClass().getSimpleName();
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.failedStateName(),
                            ExecutionStatus.FAILED,
                            current.startTime(),
                            time,
                            e.correlationKey() != null ? e.correlationKey() : current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            error
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.failedStateName(),
                            ExecutionStatus.FAILED,
                            time,
                            time,
                            e.correlationKey(),
                            null,
                            0,
                            time,
                            error
                    );
                }
            }
            case SignalDeliveredEvent e -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.RUNNING,
                            current.startTime(),
                            null,
                            e.correlationKey() != null ? e.correlationKey() : current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            null
                    );
                } else {
                    yield null;
                }
            }
            case ExecutionCancelledEvent e -> {
                String error = "Execution cancelled by " + e.operatorId() + ": " + e.reason();
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName() != null ? e.stateName() : current.currentState(),
                            ExecutionStatus.CANCELLED,
                            current.startTime(),
                            time,
                            current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            error
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName() != null ? e.stateName() : "CANCELLED",
                            ExecutionStatus.CANCELLED,
                            time,
                            time,
                            null,
                            null,
                            0,
                            time,
                            error
                    );
                }
            }
            case ExecutionPausedEvent e -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.PAUSED,
                            current.startTime(),
                            current.endTime(),
                            current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            current.errorMessage()
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.PAUSED,
                            time,
                            null,
                            null,
                            null,
                            0,
                            time,
                            null
                    );
                }
            }
            case ExecutionResumedEvent e -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.RUNNING,
                            current.startTime(),
                            null,
                            current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            null
                    );
                } else {
                    yield null;
                }
            }
            case SignalTimedOutEvent e -> {
                String error = "Signal [" + e.expectedSignal() + "] timed out after " + e.timeout().toMillis() + "ms";
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.FAILED,
                            current.startTime(),
                            time,
                            e.correlationKey() != null ? e.correlationKey() : current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            error
                    );
                } else {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            e.stateName(),
                            ExecutionStatus.FAILED,
                            time,
                            time,
                            e.correlationKey(),
                            null,
                            0,
                            time,
                            error
                    );
                }
            }
            case CircuitBreakerTrippedEvent e -> {
                String error = "Circuit breaker tripped: max transitions (" + e.maxTransitions() + ") exceeded";
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            current.currentState(),
                            ExecutionStatus.FAILED,
                            current.startTime(),
                            time,
                            current.correlationKey(),
                            null,
                            current.transitionsCount(),
                            time,
                            error
                    );
                } else {
                    yield null;
                }
            }
            case StateExitedEvent _ -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            current.currentState(),
                            current.status(),
                            current.startTime(),
                            current.endTime(),
                            current.correlationKey(),
                            current.suspendedSignal(),
                            current.transitionsCount(),
                            time,
                            current.errorMessage()
                    );
                }
                yield null;
            }
            default -> {
                if (current != null) {
                    yield new ExecutionSummary(
                            execId,
                            machine,
                            current.currentState(),
                            current.status(),
                            current.startTime(),
                            current.endTime(),
                            current.correlationKey(),
                            current.suspendedSignal(),
                            current.transitionsCount(),
                            time,
                            current.errorMessage()
                    );
                }
                yield null;
            }
        };
    }

    @Override
    public void close() {
        machines.clear();
        activeExecutions.clear();
        terminalExecutions.clear();
        terminalEvictionOrder.clear();
        terminalCount.set(0);
        recentEvents.clear();
        for (ControlPlaneStream stream : activeStreams) {
            stream.close();
        }
        activeStreams.clear();
        attachedToBus = false;
    }

    // =========================================================================
    // Internal Registration Types
    // =========================================================================

    @FunctionalInterface
    public interface SignalRouter {
        CompletableFuture<SignalDeliveryResult> routeSignal(
                @NonNull String correlationKey,
                @NonNull String signalName,
                @NonNull Object payload
        );
    }

    /**
     * Pull-based live stream consumer for the Control Plane.\
     */
    private static final class ControlPlaneStream implements EventStream {
        private final BlockingQueue<ExecutionEvent> queue = new LinkedBlockingQueue<>(1024);
        private final Predicate<ExecutionEvent> filter;
        private final Consumer<ControlPlaneStream> onClose;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        ControlPlaneStream(Predicate<ExecutionEvent> filter, Consumer<ControlPlaneStream> onClose) {
            this.filter = filter;
            this.onClose = onClose;
        }

        void offer(@NonNull ExecutionEvent event) {
            if (!closed.get() && filter.test(event)) {
                while (!queue.offer(event)) {
                    queue.poll();
                }
            }
        }

        @Override
        @Nullable
        public ExecutionEvent poll(@NonNull Duration timeout) throws InterruptedException {
            if (closed.get() && queue.isEmpty()) {
                return null;
            }
            return queue.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        @NonNull
        public ExecutionEvent take() throws InterruptedException {
            while (!closed.get() || !queue.isEmpty()) {
                ExecutionEvent item = queue.poll(100, TimeUnit.MILLISECONDS);
                if (item != null) {
                    return item;
                }
            }
            throw new IllegalStateException("ControlPlaneStream is closed");
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                onClose.accept(this);
            }
        }

        @Override
        @NonNull
        public Iterator<ExecutionEvent> iterator() {
            return new Iterator<>() {
                private ExecutionEvent nextItem = null;

                @Override
                public boolean hasNext() {
                    if (nextItem != null) {
                        return true;
                    }
                    if (closed.get() && queue.isEmpty()) {
                        return false;
                    }
                    try {
                        nextItem = poll(Duration.ofMillis(200));
                        return nextItem != null || (!closed.get() || !queue.isEmpty());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }

                @Override
                public ExecutionEvent next() {
                    if (nextItem != null) {
                        ExecutionEvent item = nextItem;
                        nextItem = null;
                        return item;
                    }
                    try {
                        return take();
                    } catch (IllegalStateException e) {
                        throw new NoSuchElementException("Stream is closed", e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new NoSuchElementException("Interrupted waiting for next event", e);
                    }
                }
            };
        }
    }
}
