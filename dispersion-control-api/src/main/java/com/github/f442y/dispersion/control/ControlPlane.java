package com.github.f442y.dispersion.control;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Universal Control Plane SPI for inspecting state machine topologies, tracking active and suspended
 * execution lifecycles, querying event timelines, and dispatching external control signals.
 */
public interface ControlPlane {

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
     * Returns the telemetry event listener that state machine builders and dispatchers can hook into
     * to keep this Control Plane synchronized in real time.
     *
     * @return The {@link ExecutionEventListener}
     */
    @NonNull
    ExecutionEventListener getEventListener();
}
