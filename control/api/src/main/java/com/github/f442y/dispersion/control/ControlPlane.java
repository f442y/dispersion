package com.github.f442y.dispersion.control;

import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Universal Control Plane SPI for inspecting state machine topologies, tracking active and suspended
 * execution lifecycles, querying event timelines, streaming live transitions, and dispatching control signals.
 */
public interface ControlPlane extends AutoCloseable {

    /**
     * Registers an inspectable state machine with the Control Plane.
     *
     * @param machine The inspectable machine definition and router
     * @return this ControlPlane for fluent chaining
     */
    @NonNull
    ControlPlane register(@NonNull InspectableMachine machine);

    /**
     * Unregisters a state machine from the Control Plane by name.
     *
     * @param machineName The state machine name
     * @return true if the machine was registered and removed, false otherwise
     */
    boolean unregister(@NonNull String machineName);

    /**
     * Lists all registered state machine topology descriptors.
     *
     * @return List of {@link MachineDescriptor} representing known state machines
     */
    @NonNull
    List<MachineDescriptor> listMachines();

    /**
     * Retrieves the topology descriptor for a registered state machine by its name.
     *
     * @param machineName The state machine name
     * @return Optional containing the descriptor if registered
     */
    @NonNull
    Optional<MachineDescriptor> getMachine(@NonNull String machineName);

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
     * Retrieves the chronological event timeline for a specific execution instance.
     *
     * @param executionId The unique execution identifier
     * @return Ordered list of {@link ExecutionEvent} emitted by the execution
     */
    @NonNull
    List<ExecutionEvent> getExecutionTimeline(@NonNull String executionId);

    /**
     * Retrieves the most recent lifecycle events globally or filtered by machine name.
     *
     * @param machineName Optional machine name filter; if null, returns global events
     * @param limit       Maximum number of events to return
     * @return Ordered list of recent {@link ExecutionEvent}
     */
    @NonNull
    List<ExecutionEvent> getRecentEvents(@Nullable String machineName, int limit);

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
     * Inspects a saved checkpoint snapshot for a specific machine and correlation key.
     *
     * @param machineName    The state machine name
     * @param correlationKey The correlation key
     * @return Optional containing the checkpoint object if found
     */
    @NonNull
    Optional<Object> inspectCheckpoint(@NonNull String machineName, @NonNull String correlationKey);

    /**
     * Inspects a saved checkpoint snapshot for a specific machine and correlation key,
     * casting to the expected type if present and matching.
     *
     * @param machineName    The state machine name
     * @param correlationKey The correlation key
     * @param checkpointType The expected checkpoint class
     * @param <T>            The checkpoint type
     * @return Optional containing the typed checkpoint if found and of expected type
     */
    @NonNull
    default <T> Optional<T> inspectCheckpoint(
            @NonNull String machineName,
            @NonNull String correlationKey,
            @NonNull Class<T> checkpointType
    ) {
        java.util.Objects.requireNonNull(checkpointType, "checkpointType must not be null");
        return inspectCheckpoint(machineName, correlationKey)
                .filter(checkpointType::isInstance)
                .map(checkpointType::cast);
    }

    /**
     * Opens a real-time, pull-based {@link EventStream} for observing lifecycle events of a specific execution.
     *
     * @param executionId The execution UUID
     * @return An open {@link EventStream} on a dedicated Virtual Thread queue
     */
    @NonNull
    EventStream watchExecution(@NonNull String executionId);

    /**
     * Opens a real-time, pull-based {@link EventStream} for observing all events of a specific state machine.
     *
     * @param machineName The state machine name
     * @return An open {@link EventStream} on a dedicated Virtual Thread queue
     */
    @NonNull
    EventStream watchMachine(@NonNull String machineName);

    /**
     * Returns the telemetry event listener that state machine builders and dispatchers can hook into
     * to keep this Control Plane synchronized in real time.
     *
     * @return The {@link ExecutionEventListener}
     */
    @NonNull
    ExecutionEventListener getEventListener();

    @Override
    default void close() {}
}
