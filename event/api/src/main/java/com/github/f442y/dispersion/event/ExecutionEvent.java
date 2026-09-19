package com.github.f442y.dispersion.event;

import org.jspecify.annotations.NonNull;

import java.time.Instant;
import java.util.UUID;

/**
 * Foundational contract for immutable telemetry and lifecycle events emitted during state machine execution.
 *
 * <p>Standard lifecycle events are categorized into specialized sealed sub-interfaces:
 * <ul>
 *     <li>{@link TurnLifecycleEvent}: turn start, completion, failure, compensation, suspension</li>
 *     <li>{@link StateLifecycleEvent}: state entry, exit, transitions, action executions</li>
 *     <li>{@link SignalEvent}: external signal awaiting, delivery, timeouts, discards</li>
 *     <li>{@link CompensationEvent}: individual saga rollback steps and turn compensation</li>
 *     <li>{@link RetryLifecycleEvent}: retries and exhaustion</li>
 *     <li>{@link ChildMachineEvent}: sub-workflow lifecycle and lineage</li>
 *     <li>{@link ParallelExecutionEvent}: parallel forks, branch completions, joins</li>
 *     <li>{@link ExecutionGuardEvent}: visit limits and circuit breaker trip events</li>
 *     <li>{@link ControlPlaneEvent}: execution pause, resume, and cancellation</li>
 * </ul>
 * </p>
 *
 * <p>Domain-specific events (such as batch barrier synchronization or messaging deduplication)
 * can implement this interface directly from their respective modules.</p>
 *
 * <p>Example pattern matching with Java pattern matching switches:</p>
 * <pre>{@code
 * switch (event) {
 *     case TurnLifecycleEvent turn -> log.atInfo().addKeyValue("turn_event", turn.getClass().getSimpleName()).log("Turn event");
 *     case StateLifecycleEvent state -> log.atDebug().addKeyValue("state_event", state.getClass().getSimpleName()).log("State event");
 *     case SignalEvent signal -> log.atInfo().addKeyValue("signal_event", signal.getClass().getSimpleName()).log("Signal event");
 *     case CompensationEvent comp -> log.atWarn().addKeyValue("comp_event", comp.getClass().getSimpleName()).log("Compensation event");
 *     case ControlPlaneEvent cp -> log.atWarn().addKeyValue("control_event", cp.getClass().getSimpleName()).log("Control plane event");
 *     default -> log.atDebug().addKeyValue("event_type", event.getClass().getSimpleName()).log("Custom event received");
 * }
 * }</pre>
 */
public interface ExecutionEvent {

    /**
     * Unique identifier of the state machine execution instance.
     */
    @NonNull
    UUID machineId();

    /**
     * Configured name of the state machine definition.
     */
    @NonNull
    String machineName();

    /**
     * Timestamp when the event was generated.
     */
    @NonNull
    Instant timestamp();
}
