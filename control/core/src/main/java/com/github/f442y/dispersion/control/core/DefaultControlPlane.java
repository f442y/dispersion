package com.github.f442y.dispersion.control.core;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.event.EventBus;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.OverflowPolicy;
import com.github.f442y.dispersion.event.bus.VirtualThreadEventBus;
import com.github.f442y.dispersion.event.dispatcher.AsyncExecutionEventDispatcher;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import com.github.f442y.dispersion.orchestration.CheckpointStore;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineConfiguration;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.batch.BatchTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineExecutor;
import com.github.f442y.dispersion.orchestration.core.batch.BatchOrchestrationExecutor;
import com.github.f442y.dispersion.orchestration.core.batch.BatchOrchestrationStepDriver;
import com.github.f442y.dispersion.orchestration.core.batch.BatchOrchestrationStepDriver.BatchConfiguration;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Thread-safe default implementation of {@link ControlPlane} engineered with segmented O(1) eviction
 * and Virtual Thread live streaming.
 *
 * <h2>Segmented O(1) Eviction Architecture</h2>
 * <p>To eliminate full-collection linear scans (which burn CPU on high-frequency telemetry events),
 * execution summaries are strictly partitioned into two pools:</p>
 * <ul>
 *   <li><b>Active Pool ({@code activeExecutions}):</b> Tracks in-flight workflows ({@link ExecutionStatus#RUNNING}
 *       or {@link ExecutionStatus#SUSPENDED}). Active workflows are critical operational entities waiting for
 *       internal steps or external signals; they are never subject to eviction.</li>
 *   <li><b>Terminal Pool ({@code terminalExecutions}):</b> Tracks finished workflows ({@link ExecutionStatus#COMPLETED},
 *       {@link ExecutionStatus#FAILED}, or {@link ExecutionStatus#COMPENSATED}). When the terminal count exceeds
 *       {@code maxTrackedExecutions}, the oldest terminal execution is pruned in strictly <b>O(1)</b> time
 *       via a concurrent FIFO queue.</li>
 * </ul>
 *
 * <h2>Live Streaming Hub</h2>
 * <p>Backed by an internal {@link VirtualThreadEventBus}, the control plane serves real-time, non-blocking
 * {@link EventStream} subscriptions for individual execution instances ({@link #watchExecution(String)})
 * or entire machine topologies ({@link #watchMachine(String)}).</p>
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

    // Timeline events per execution
    private final Map<String, Deque<ExecutionEvent>> executionTimelines = new ConcurrentHashMap<>();

    // Global recent events with O(1) atomic sizing
    private final Deque<ExecutionEvent> recentEvents = new ConcurrentLinkedDeque<>();
    private final AtomicInteger recentCount = new AtomicInteger(0);

    // Live streaming hub
    private final VirtualThreadEventBus streamBus;

    private final int maxTrackedExecutions;
    private final int maxTimelineEventsPerExecution;
    private final int maxRecentEvents;

    /**
     * Creates a Control Plane with standard capacity defaults (10,000 terminal executions, 100 timeline events, 2,000 recent events).
     */
    public DefaultControlPlane() {
        this(10_000, 100, 2_000);
    }

    /**
     * Creates a Control Plane with custom buffer capacities.
     */
    public DefaultControlPlane(
            int maxTrackedExecutions,
            int maxTimelineEventsPerExecution,
            int maxRecentEvents
    ) {
        if (maxTrackedExecutions <= 0 || maxTimelineEventsPerExecution <= 0 || maxRecentEvents <= 0) {
            throw new IllegalArgumentException("Capacities must be strictly positive");
        }
        this.maxTrackedExecutions = maxTrackedExecutions;
        this.maxTimelineEventsPerExecution = maxTimelineEventsPerExecution;
        this.maxRecentEvents = maxRecentEvents;

        for (int i = 0; i < executionLocks.length; i++) {
            executionLocks[i] = new Object();
        }

        this.streamBus = VirtualThreadEventBus.builder()
                .bufferCapacity(8_192)
                .historyCapacity(0) // ControlPlane manages recentEvents directly
                .streamDefaultCapacity(512)
                .overflowPolicy(OverflowPolicy.DROP_OLDEST)
                .build();
    }

    // =========================================================================
    // Registration & Topology Discovery
    // =========================================================================

    @Override
    @NonNull
    public DefaultControlPlane register(@NonNull InspectableMachine machine) {
        Objects.requireNonNull(machine, "machine must not be null");
        String name = machine.descriptor().name();
        machines.put(name, machine);
        log.info("Registered InspectableMachine [{}] in Control Plane", name);
        return this;
    }

    @Override
    public boolean unregister(@NonNull String machineName) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        return machines.remove(machineName) != null;
    }

    /**
     * Registers an {@link OrchestrationStateMachineExecutor}, inspecting its topology
     * and configuring signal routing and checkpoint inspection.
     */
    public <C extends StateMachineContext, S extends Enum<S> & StateKey, I, O> DefaultControlPlane register(
            @NonNull OrchestrationStateMachineExecutor<C, S, I, O> executor
    ) {
        Objects.requireNonNull(executor, "executor must not be null");
        OrchestrationStateMachineConfiguration<C, S, I, O> config = executor.getConfiguration();
        String machineName = config.getMachineName();
        StateMap<C, S> stateMap = config.getStateMap();
        MachineDescriptor descriptor = createDescriptor(machineName, MachineType.ORCHESTRATION, stateMap);

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
                return executor.sendSignalByCorrelationKey(correlationKey, signalName, payload != null ? payload : new Object())
                        .handle((turnResult, throwable) -> mapTurnResultToSignalResult(machineName, correlationKey, signalName, turnResult, throwable));
            }

            @Override
            @NonNull
            public Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                CheckpointStore<C, S> store = config.getCheckpointStore();
                if (store == null) {
                    return Optional.empty();
                }
                return store.findByCorrelationKey(correlationKey).map(cp -> (Object) cp);
            }
        };

        return register(inspectable);
    }

    /**
     * Registers an {@link AtomicStateMachineExecutor}, extracting its topology and graph definition.
     */
    public <C extends StateMachineContext, S extends Enum<S> & StateKey, I, O> DefaultControlPlane register(
            @NonNull AtomicStateMachineExecutor<C, S, I, O> executor
    ) {
        Objects.requireNonNull(executor, "executor must not be null");
        StateMachineConfiguration<C, S, I, O> config = executor.getConfiguration();
        String machineName = config.getMachineName();
        StateMap<C, S> stateMap = config.getStateMap();
        MachineDescriptor descriptor = createDescriptor(machineName, MachineType.ATOMIC, stateMap);

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
                return CompletableFuture.completedFuture(
                        SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                                "Atomic state machines do not support external signal suspension or delivery")
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
     * Registers a {@link BatchOrchestrationExecutor}, inspecting its batch topology
     * and configuring item/batch signal routing and checkpoint inspection.
     */
    public <BC extends StateMachineContext, IC extends StateMachineContext, S extends Enum<S> & StateKey, O> DefaultControlPlane register(
            @NonNull BatchOrchestrationExecutor<BC, IC, S, O> executor
    ) {
        Objects.requireNonNull(executor, "executor must not be null");
        BatchConfiguration<BC, IC, S, O> config = executor.getConfiguration();
        String machineName = config.batchName;

        String initialState = config.initialStateKey != null ? config.initialStateKey.name() : "INITIAL";
        Set<String> endStates = config.endStates.stream()
                .map(Enum::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> allStateNames = new LinkedHashSet<>();
        allStateNames.add(initialState);
        config.stateDefinitions.keySet().forEach(k -> allStateNames.add(k.name()));
        endStates.forEach(allStateNames::add);
        List<String> allStates = List.copyOf(allStateNames);

        StringBuilder sb = new StringBuilder("stateDiagram-v2\n");
        sb.append("    [*] --> ").append(initialState).append("\n");
        for (Map.Entry<S, BatchOrchestrationStepDriver.ItemStateDefinition<IC, S>> entry : config.stateDefinitions.entrySet()) {
            S source = entry.getKey();
            var def = entry.getValue();
            if (def.isBarrier) {
                sb.append("    note right of ").append(source.name()).append(" : Barrier (").append(def.barrierPolicy).append(")\n");
            }
            if (def.expectedSignal != null) {
                sb.append("    note right of ").append(source.name()).append(" : Awaits signal [").append(def.expectedSignal).append("]\n");
            }
        }
        for (String endState : endStates) {
            sb.append("    ").append(endState).append(" --> [*]\n");
        }
        String mermaid = sb.toString();

        MachineDescriptor descriptor = new MachineDescriptor(
                machineName,
                MachineType.BATCH,
                initialState,
                endStates,
                allStates,
                mermaid
        );

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
                CompletableFuture<BatchTurnResult<BC, IC, S, O>> batchFuture;
                if (payload instanceof SignalCommand cmd) {
                    batchFuture = executor.handleCommand(cmd);
                } else if (payload instanceof CommandEnvelope<?> env && env.command() instanceof SignalCommand) {
                    @SuppressWarnings("unchecked")
                    CommandEnvelope<? extends SignalCommand> typedEnv = (CommandEnvelope<? extends SignalCommand>) env;
                    batchFuture = executor.handleCommand(typedEnv);
                } else {
                    batchFuture = executor.sendBatchSignal(correlationKey, signalName, payload);
                }

                return batchFuture.handle((turnResult, throwable) -> {
                    if (throwable != null) {
                        return SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                                "Error executing batch turn upon signal delivery: " + throwable.getMessage());
                    }
                    if (turnResult == null) {
                        return SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                                "No batch execution turn result returned");
                    }
                    String resultingState = turnResult.currentBatchStateKey() != null
                            ? turnResult.currentBatchStateKey().name()
                            : null;
                    String errorMessage = turnResult.error() != null
                            ? turnResult.error().getMessage()
                            : null;
                    return new SignalDeliveryResult(
                            turnResult.error() == null,
                            turnResult.error() != null ? turnResult.error().getMessage() : "Signal delivered to batch successfully",
                            machineName,
                            correlationKey,
                            signalName,
                            turnResult.isCompleted(),
                            turnResult.isSuspended(),
                            resultingState,
                            errorMessage
                    );
                });
            }

            @Override
            @NonNull
            public Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return executor.getCheckpoint(correlationKey).map(cp -> (Object) cp);
            }
        };

        return register(inspectable);
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
     * Wires this Control Plane directly as a listener to the given event bus.
     */
    public DefaultControlPlane attachTo(@NonNull EventBus eventBus) {
        Objects.requireNonNull(eventBus, "eventBus must not be null");
        eventBus.subscribe(this);
        return this;
    }

    /**
     * Wires this Control Plane directly as a listener to the given event dispatcher.
     */
    public DefaultControlPlane attachTo(@NonNull AsyncExecutionEventDispatcher dispatcher) {
        return attachTo((EventBus) dispatcher);
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
        Objects.requireNonNull(executionId, "executionId must not be null");
        Deque<ExecutionEvent> deque = executionTimelines.get(executionId);
        if (deque == null) {
            return Collections.emptyList();
        }
        synchronized (deque) {
            return new ArrayList<>(deque);
        }
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

    /**
     * Inspects a suspended workflow checkpoint by correlation key if supported by the registered machine.
     */
    @SuppressWarnings("unchecked")
    public Optional<OrchestrationCheckpoint<?, ?>> getCheckpoint(
            @NonNull String machineName,
            @NonNull String correlationKey
    ) {
        return inspectCheckpoint(machineName, correlationKey)
                .filter(cp -> cp instanceof OrchestrationCheckpoint)
                .map(cp -> (OrchestrationCheckpoint<?, ?>) cp);
    }

    /**
     * Inspects a suspended batch workflow checkpoint by batch correlation key.
     */
    @SuppressWarnings("unchecked")
    public <BC extends StateMachineContext, IC extends StateMachineContext, S extends Enum<S> & StateKey>
    Optional<BatchOrchestrationCheckpoint<BC, IC, S>> getBatchCheckpoint(
            @NonNull String machineName,
            @NonNull String correlationKey
    ) {
        return inspectCheckpoint(machineName, correlationKey)
                .filter(cp -> cp instanceof BatchOrchestrationCheckpoint)
                .map(cp -> (BatchOrchestrationCheckpoint<BC, IC, S>) cp);
    }

    // =========================================================================
    // Live Streaming Observability
    // =========================================================================

    @Override
    @NonNull
    public EventStream watchExecution(@NonNull String executionId) {
        Objects.requireNonNull(executionId, "executionId must not be null");
        return streamBus.openStream(event -> executionId.equals(event.machineId().toString()));
    }

    @Override
    @NonNull
    public EventStream watchMachine(@NonNull String machineName) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        return streamBus.openStream(event -> machineName.equals(event.machineName()));
    }

    // =========================================================================
    // Signal Dispatching
    // =========================================================================

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
                    SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                            "Machine [" + machineName + "] is not registered in the Control Plane")
            );
        }

        return machine.sendSignal(correlationKey, signalName, payload);
    }

    // =========================================================================
    // ExecutionEventListener Implementation & Segmented O(1) Eviction
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

        // 2. Append to individual execution timeline
        appendTimelineEvent(event);

        // 3. Update execution summary snapshot with partitioned O(1) eviction
        updateExecutionSummary(event);

        // 4. Publish to internal Virtual Thread event stream hub
        streamBus.onEvent(event);
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
                || status == ExecutionStatus.COMPENSATED;
    }

    private void evictTerminalExecution() {
        while (terminalCount.get() > maxTrackedExecutions) {
            String oldestId = terminalEvictionOrder.poll();
            if (oldestId == null) {
                break;
            }
            if (terminalExecutions.remove(oldestId) != null) {
                executionTimelines.remove(oldestId);
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
            case ExecutionEvent.TurnStartedEvent e -> {
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
            case ExecutionEvent.StateEnteredEvent e -> {
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
            case ExecutionEvent.TransitionEvaluatedEvent _ -> {
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
            case ExecutionEvent.SignalAwaitedEvent e -> {
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
            case ExecutionEvent.TurnSuspendedEvent e -> {
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
            case ExecutionEvent.TurnCompletedEvent e -> {
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
            case ExecutionEvent.TurnCompensatedEvent e -> {
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
            case ExecutionEvent.TurnFailedEvent e -> {
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
            case ExecutionEvent.SignalDeliveredEvent e -> {
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
            case ExecutionEvent.StateExitedEvent _ -> {
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
            case ExecutionEvent.CommandDeduplicatedEvent _ -> current;
            case ExecutionEvent.BatchBarrierReachedEvent _ -> current;
            case ExecutionEvent.BatchBarrierUnlockedEvent _ -> current;
        };
    }

    private void appendTimelineEvent(@NonNull ExecutionEvent event) {
        String execId = event.machineId().toString();
        Deque<ExecutionEvent> deque = executionTimelines.computeIfAbsent(execId, _ -> new ArrayDeque<>());
        synchronized (deque) {
            deque.addLast(event);
            if (deque.size() > maxTimelineEventsPerExecution) {
                deque.pollFirst();
            }
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private <C extends StateMachineContext, S extends Enum<S> & StateKey> MachineDescriptor createDescriptor(
            String name,
            MachineType type,
            StateMap<C, S> stateMap
    ) {
        String initialState = stateMap.getInitialState().name();
        Set<String> endStates = stateMap.getEndStates().stream()
                .map(Enum::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> allStateNames = new LinkedHashSet<>();
        stateMap.getAllStates().keySet().forEach(k -> allStateNames.add(k.name()));
        stateMap.getEndStates().forEach(k -> allStateNames.add(k.name()));
        List<String> allStates = List.copyOf(allStateNames);

        String mermaid = stateMap.toMermaid();

        return new MachineDescriptor(
                name,
                type,
                initialState,
                endStates,
                allStates,
                mermaid
        );
    }

    private SignalDeliveryResult mapTurnResultToSignalResult(
            String machineName,
            String correlationKey,
            String signalName,
            OrchestrationTurnResult<?, ?, ?> turnResult,
            Throwable throwable
    ) {
        if (throwable != null) {
            return SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                    "Error executing turn upon signal delivery: " + throwable.getMessage());
        }

        if (turnResult == null) {
            return SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                    "No execution turn result returned");
        }

        String resultingState = turnResult.currentStateKey() != null
                ? turnResult.currentStateKey().name()
                : null;

        String errorMessage = turnResult.error() != null
                ? turnResult.error().getMessage()
                : null;

        return new SignalDeliveryResult(
                true,
                "Signal delivered successfully",
                machineName,
                correlationKey,
                signalName,
                turnResult.isCompleted(),
                turnResult.isSuspended(),
                resultingState,
                errorMessage
        );
    }

    @Override
    public void close() {
        machines.clear();
        activeExecutions.clear();
        terminalExecutions.clear();
        terminalEvictionOrder.clear();
        terminalCount.set(0);
        executionTimelines.clear();
        recentEvents.clear();
        recentCount.set(0);
        streamBus.close();
    }

    // =========================================================================
    // Internal Registration Types
    // =========================================================================

    @FunctionalInterface
    public interface SignalRouter {
        CompletableFuture<SignalDeliveryResult> routeSignal(
                String correlationKey,
                String signalName,
                Object payload
        );
    }
}
