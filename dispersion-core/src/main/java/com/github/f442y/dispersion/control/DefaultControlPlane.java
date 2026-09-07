package com.github.f442y.dispersion.control;

import com.github.f442y.dispersion.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.event.AsyncExecutionEventDispatcher;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.orchestration.CheckpointStore;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineConfiguration;
import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineExecutor;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Thread-safe default implementation of {@link ControlPlane}.
 * <p>
 * Maintains an in-memory registry of state machines, live execution summaries,
 * and a bounded ring-buffer of chronological execution events. Also acts as an
 * {@link ExecutionEventListener} so it can be wired directly to executors or
 * an {@link AsyncExecutionEventDispatcher}.
 */
public class DefaultControlPlane implements ControlPlane, ExecutionEventListener, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultControlPlane.class);

    private final Map<String, RegisteredMachine> machines = new ConcurrentHashMap<>();
    private final Map<String, ExecutionSummary> executions = new ConcurrentHashMap<>();
    private final Map<String, Deque<ExecutionEvent>> executionTimelines = new ConcurrentHashMap<>();
    private final Deque<ExecutionEvent> recentEvents = new ConcurrentLinkedDeque<>();

    private final int maxTrackedExecutions;
    private final int maxTimelineEventsPerExecution;
    private final int maxRecentEvents;

    /**
     * Creates a Control Plane with standard capacity defaults (10,000 executions, 100 timeline events, 2,000 recent events).
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
    }

    // =========================================================================
    // Registration & Topology Discovery
    // =========================================================================

    /**
     * Registers an {@link OrchestrationStateMachineExecutor}, inspecting its topology
     * and configuring signal routing.
     */
    public <C extends StateMachineContext, S extends Enum<S> & StateKey, I, O> DefaultControlPlane register(
            @NonNull OrchestrationStateMachineExecutor<C, S, I, O> executor
    ) {
        Objects.requireNonNull(executor, "executor must not be null");
        OrchestrationStateMachineConfiguration<C, S, I, O> config = executor.getConfiguration();
        String machineName = config.getMachineName();
        StateMap<C, S> stateMap = config.getStateMap();

        MachineDescriptor descriptor = createDescriptor(machineName, MachineType.ORCHESTRATION, stateMap);

        Function<String, Optional<OrchestrationCheckpoint<?, ?>>> checkpointFinder = correlationKey -> {
            CheckpointStore<C, S> store = config.getCheckpointStore();
            if (store == null) return Optional.empty();
            return store.findByCorrelationKey(correlationKey).map(cp -> (OrchestrationCheckpoint<?, ?>) cp);
        };

        RegisteredMachine registration = new RegisteredMachine(
                descriptor,
                (corrKey, signalName, payload) -> executor.sendSignalByCorrelationKey(corrKey, signalName, payload)
                        .handle((turnResult, throwable) -> mapTurnResultToSignalResult(machineName, corrKey, signalName, turnResult, throwable)),
                checkpointFinder
        );

        machines.put(machineName, registration);
        log.info("Registered Orchestration State Machine [{}] in Control Plane", machineName);
        return this;
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

        RegisteredMachine registration = new RegisteredMachine(
                descriptor,
                (corrKey, signalName, _) -> CompletableFuture.completedFuture(
                        SignalDeliveryResult.failure(machineName, corrKey, signalName,
                                "Atomic state machines do not support external signal suspension or delivery")
                ),
                _ -> Optional.empty()
        );

        machines.put(machineName, registration);
        log.info("Registered Atomic State Machine [{}] in Control Plane", machineName);
        return this;
    }

    /**
     * Registers a generic state machine topology descriptor with a custom signal router.
     */
    public DefaultControlPlane register(
            @NonNull MachineDescriptor descriptor,
            @Nullable SignalRouter signalRouter
    ) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        RegisteredMachine registration = new RegisteredMachine(
                descriptor,
                signalRouter != null ? signalRouter : (corrKey, sig, _) -> CompletableFuture.completedFuture(
                        SignalDeliveryResult.failure(descriptor.name(), corrKey, sig, "No signal router configured")
                ),
                _ -> Optional.empty()
        );
        machines.put(descriptor.name(), registration);
        log.info("Registered State Machine [{}] in Control Plane", descriptor.name());
        return this;
    }

    /**
     * Unregisters a state machine from the Control Plane.
     */
    public boolean unregister(@NonNull String machineName) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        return machines.remove(machineName) != null;
    }

    /**
     * Wires this Control Plane directly as a listener to the given event dispatcher.
     */
    public DefaultControlPlane attachTo(@NonNull AsyncExecutionEventDispatcher dispatcher) {
        Objects.requireNonNull(dispatcher, "dispatcher must not be null");
        dispatcher.addListener(this);
        return this;
    }

    // =========================================================================
    // ControlPlane Query SPI Implementation
    // =========================================================================

    @Override
    @NonNull
    public List<MachineDescriptor> listMachines() {
        return machines.values().stream()
                .map(RegisteredMachine::descriptor)
                .toList();
    }

    @Override
    @NonNull
    public Optional<MachineDescriptor> getMachine(@NonNull String machineName) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        RegisteredMachine reg = machines.get(machineName);
        return reg != null ? Optional.of(reg.descriptor()) : Optional.empty();
    }

    @Override
    @NonNull
    public Optional<ExecutionSummary> getExecution(@NonNull String executionId) {
        Objects.requireNonNull(executionId, "executionId must not be null");
        return Optional.ofNullable(executions.get(executionId));
    }

    @Override
    @NonNull
    public List<ExecutionSummary> listExecutions(
            @Nullable String machineName,
            @Nullable ExecutionStatus status,
            int limit
    ) {
        int max = limit <= 0 ? 50 : limit;
        return executions.values().stream()
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

    /**
     * Inspects a suspended workflow checkpoint by correlation key if supported by the registered machine.
     */
    public Optional<OrchestrationCheckpoint<?, ?>> getCheckpoint(
            @NonNull String machineName,
            @NonNull String correlationKey
    ) {
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        RegisteredMachine reg = machines.get(machineName);
        if (reg == null) {
            return Optional.empty();
        }
        return reg.checkpointFinder().apply(correlationKey);
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

        RegisteredMachine reg = machines.get(machineName);
        if (reg == null) {
            return CompletableFuture.completedFuture(
                    SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                            "Machine [" + machineName + "] is not registered in the Control Plane")
            );
        }

        return reg.signalRouter().routeSignal(correlationKey, signalName, payload != null ? payload : new Object());
    }

    // =========================================================================
    // ExecutionEventListener Implementation
    // =========================================================================

    @Override
    public void onEvent(@NonNull ExecutionEvent event) {
        Objects.requireNonNull(event, "event must not be null");

        // 1. Record in global recent ring buffer
        recentEvents.addFirst(event);
        while (recentEvents.size() > maxRecentEvents) {
            recentEvents.pollLast();
        }

        // 2. Append to individual execution timeline
        appendTimelineEvent(event);

        // 3. Update execution summary snapshot
        updateExecutionSummary(event);
    }

    private void updateExecutionSummary(@NonNull ExecutionEvent event) {
        String execId = event.machineId().toString();
        String machine = event.machineName();
        Instant time = event.timestamp();

        executions.compute(execId, (_, current) -> switch (event) {
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
        });

        // Enforce maximum tracked executions limit
        if (executions.size() > maxTrackedExecutions) {
            evictOldestExecution();
        }
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

    private void evictOldestExecution() {
        executions.values().stream()
                .min(Comparator.comparing(ExecutionSummary::lastUpdated))
                .ifPresent(oldest -> {
                    executions.remove(oldest.executionId());
                    executionTimelines.remove(oldest.executionId());
                });
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
        executions.clear();
        executionTimelines.clear();
        recentEvents.clear();
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

    private record RegisteredMachine(
            MachineDescriptor descriptor,
            SignalRouter signalRouter,
            Function<String, Optional<OrchestrationCheckpoint<?, ?>>> checkpointFinder
    ) {}
}
