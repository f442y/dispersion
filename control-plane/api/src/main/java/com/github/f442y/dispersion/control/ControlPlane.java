package com.github.f442y.dispersion.control;

import com.github.f442y.dispersion.event.DynamicTapManager;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.routing.InspectableRouter;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Universal Control Plane SPI for state machine observability, topology inspection,
 * real-time lifecycle event streaming, and decoupled signal injection.
 *
 * <h2>Core Responsibilities</h2>
 * <ul>
 *   <li><b>Topology Discovery:</b> Introspect registered atomic FSMs, discrete Sagas, and batch
 *       orchestrators, including their state diagrams and Mermaid graphs.</li>
 *   <li><b>Execution Observability:</b> Query active and terminal workflow executions, turn counts,
 *       current states, and error diagnostics with bounded O(1) memory guarantees.</li>
 *   <li><b>Live Event Streaming:</b> Open pull-based {@link EventStream} feeds for specific execution
 *       instances or entire machine topologies on Java 25 virtual threads.</li>
 *   <li><b>External Signal Delivery:</b> Route external signals (e.g. HTTP webhooks, Kafka messages)
 *       to suspended workflows awaiting input by domain correlation key.</li>
 * </ul>
 */
public interface ControlPlane extends AutoCloseable {

    // =========================================================================
    // Machine Registration & Topology Discovery
    // =========================================================================

    /**
     * Registers an inspectable machine instance into the Control Plane topology registry.
     *
     * @param machine The inspectable machine instance
     * @return This Control Plane instance for fluent chaining
     */
    @NonNull
    ControlPlane register(@NonNull InspectableMachine machine);

    /**
     * Registers a remote or static state machine topology descriptor with the Control Plane.
     * Signals targeting this machine will fail gracefully unless an external broker or dispatcher is registered.
     *
     * @param descriptor The machine topology descriptor
     * @return this ControlPlane for fluent chaining
     */
    @NonNull
    default ControlPlane registerDescriptor(@NonNull MachineDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        return register(new InspectableMachine() {
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
                        SignalDeliveryResult.failure(descriptor.name(), correlationKey, signalName, "No local signal handler registered for remote machine")
                );
            }

            @Override
            @NonNull
            public Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.empty();
            }
        });
    }

    /**
     * Unregisters a machine topology by its unique name.
     *
     * @param machineName The name of the machine to unregister
     * @return {@code true} if a machine was removed
     */
    boolean unregister(@NonNull String machineName);

    /**
     * Registers an {@link InspectableRouter} for service-level routing discovery and dispatch inspection.
     *
     * @param router The inspectable router
     * @return This Control Plane instance for fluent chaining
     */
    @NonNull
    ControlPlane registerRouter(@NonNull InspectableRouter router);

    /**
     * Retrieves the registered {@link InspectableRouter}, if one has been configured.
     *
     * @return Optional containing the router if registered
     */
    @NonNull
    Optional<InspectableRouter> getRouter();

    /**
     * Lists all registered state machine topologies.
     *
     * @return Unmodifiable list of registered {@link MachineDescriptor} instances
     */
    @NonNull
    List<MachineDescriptor> listMachines();

    /**
     * Retrieves the descriptor for a specific state machine by name.
     *
     * @param machineName The unique machine name
     * @return Optional containing the descriptor if registered
     */
    @NonNull
    Optional<MachineDescriptor> getMachine(@NonNull String machineName);

    // =========================================================================
    // Execution Queries & Diagnostics
    // =========================================================================

    /**
     * Queries tracked executions matching optional filters.
     *
     * @param machineName Optional machine name filter; if null, matches all machines
     * @param status      Optional execution status filter; if null, matches any status
     * @param limit       Maximum number of execution summaries to return
     * @return List of matching {@link ExecutionSummary} instances ordered by recency
     */
    @NonNull
    List<ExecutionSummary> listExecutions(
            @Nullable String machineName,
            @Nullable ExecutionStatus status,
            int limit
    );

    /**
     * Retrieves the execution summary for a specific execution UUID.
     *
     * @param executionId The unique execution identifier
     * @return Optional containing the summary if tracked
     */
    @NonNull
    Optional<ExecutionSummary> getExecution(@NonNull String executionId);

    /**
     * Retrieves up to 100 chronological events from the timeline of a specific execution instance.
     *
     * @param executionId The unique execution identifier
     * @return Ordered list of {@link ExecutionEvent} emitted by the execution
     */
    @NonNull
    default List<ExecutionEvent> getExecutionTimeline(@NonNull String executionId) {
        return getExecutionTimeline(executionId, 100);
    }

    /**
     * Retrieves up to {@code limit} events from the chronological event timeline of a specific execution instance.
     *
     * @param executionId The unique execution identifier
     * @param limit       The maximum number of recent events to return
     * @return Ordered list of {@link ExecutionEvent} emitted by the execution
     */
    @NonNull
    List<ExecutionEvent> getExecutionTimeline(@NonNull String executionId, int limit);

    /**
     * Asynchronously retrieves up to {@code limit} events from the chronological event timeline
     * of a specific execution instance.
     *
     * @param executionId The unique execution identifier
     * @param limit       The maximum number of recent events to return
     * @return A {@link CompletableFuture} completing with the ordered list of {@link ExecutionEvent} emitted by the execution
     */
    @NonNull
    default CompletableFuture<List<ExecutionEvent>> fetchExecutionTimeline(@NonNull String executionId, int limit) {
        return CompletableFuture.completedFuture(getExecutionTimeline(executionId, limit));
    }

    /**
     * Returns the configured {@link TraceTimelineProvider}, if available.
     *
     * @return Optional containing the timeline provider if configured
     */
    @NonNull
    default Optional<TraceTimelineProvider> getTimelineProvider() {
        return Optional.empty();
    }

    /**
     * Returns the configured {@link DynamicTapManager}, if available.
     *
     * @return Optional containing the dynamic tap manager if configured
     */
    @NonNull
    default Optional<DynamicTapManager> getTapManager() {
        return Optional.empty();
    }

    /**
     * Retrieves the most recent lifecycle events globally or filtered by machine name.
     *
     * @param machineName Optional machine name filter; if null, returns global events
     * @param limit       Maximum number of events to return
     * @return Ordered list of recent {@link ExecutionEvent}
     */
    @NonNull
    List<ExecutionEvent> getRecentEvents(@Nullable String machineName, int limit);

    // =========================================================================
    // Signal Dispatching & Execution Triggers
    // =========================================================================

    /**
     * Dispatches a new execution of a registered state machine with optional input.
     *
     * @param machineName The state machine name
     * @param input       Optional input payload
     * @return A {@link CompletableFuture} completing with the execution output or completion result
     */
    @NonNull
    default CompletableFuture<Object> dispatchExecution(@NonNull String machineName, @Nullable Object input) {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("ControlPlane does not support dynamic dispatch"));
    }

    /**
     * Delivers an external signal to a suspended orchestration workflow by its correlation key.
     *
     * @param machineName    The state machine name
     * @param correlationKey The domain correlation key of the suspended workflow
     * @param signalName     The signal identifier
     * @param payload        Optional payload associated with the signal
     * @return A {@link CompletableFuture} completing with the {@link SignalDeliveryResult}
     */
    @NonNull
    CompletableFuture<SignalDeliveryResult> sendSignal(
            @NonNull String machineName,
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object payload
    );

    /**
     * Inspects the durable checkpoint state of a suspended machine execution, if available.
     *
     * @param machineName    The state machine name
     * @param correlationKey The correlation key
     * @return Optional containing the checkpoint payload, or empty if not suspended or absent
     */
    @NonNull
    Optional<Object> inspectCheckpoint(@NonNull String machineName, @NonNull String correlationKey);

    /**
     * Inspects the durable checkpoint state of a suspended machine execution, typed to the expected class.
     *
     * @param machineName     The state machine name
     * @param correlationKey  The correlation key
     * @param checkpointClass The expected checkpoint type class
     * @param <CHECKPOINT_TYPE> The checkpoint payload type
     * @return Optional containing the typed checkpoint payload, or empty if absent or incompatible
     */
    @NonNull
    default <CHECKPOINT_TYPE> Optional<CHECKPOINT_TYPE> inspectCheckpoint(
            @NonNull String machineName,
            @NonNull String correlationKey,
            @NonNull Class<CHECKPOINT_TYPE> checkpointClass
    ) {
        return inspectCheckpoint(machineName, correlationKey)
                .filter(checkpointClass::isInstance)
                .map(checkpointClass::cast);
    }

    // =========================================================================
    // Live Event Streaming
    // =========================================================================

    /**
     * Opens a live pull-based {@link EventStream} for a specific execution instance.
     *
     * @param executionId The unique execution UUID
     * @return An open {@link EventStream} receiving real-time events for this execution
     */
    @NonNull
    EventStream watchExecution(@NonNull String executionId);

    /**
     * Opens a live pull-based {@link EventStream} for all executions of a specific state machine topology.
     *
     * @param machineName The state machine name
     * @return An open {@link EventStream} receiving real-time events for this machine
     */
    @NonNull
    EventStream watchMachine(@NonNull String machineName);

    /**
     * Opens a live pull-based {@link EventStream} for observing all events globally across all machines.
     *
     * @return An open {@link EventStream} receiving real-time events across all machines
     */
    @NonNull
    default EventStream watchAll() {
        return watchMachine("");
    }

    /**
     * Opens a live pull-based {@link EventStream} filtered by a custom predicate.
     *
     * @param filter The filter predicate
     * @return An open {@link EventStream}
     */
    @NonNull
    default EventStream openStream(@NonNull Predicate<ExecutionEvent> filter) {
        return watchAll();
    }

    /**
     * Returns an {@link ExecutionEventListener} that can be attached to any {@link com.github.f442y.dispersion.event.EventBus}
     * to feed telemetry directly into this Control Plane.
     *
     * @return The listener instance
     */
    @NonNull
    ExecutionEventListener getEventListener();

    @Override
    default void close() {}
}
