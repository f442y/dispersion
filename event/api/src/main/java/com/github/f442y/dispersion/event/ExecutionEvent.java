package com.github.f442y.dispersion.event;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Foundational contract for immutable telemetry and lifecycle events emitted during state machine execution.
 *
 * <p>Core lifecycle events are provided as nested records within this interface. Domain-specific events
 * (such as batch barrier synchronization or messaging deduplication) can implement this interface directly
 * from their respective modules.</p>
 *
 * <p>Example pattern matching with Java pattern matching switches:</p>
 * <pre>{@code
 * switch (event) {
 *     case ExecutionEvent.TurnStartedEvent started -> log.atInfo().addKeyValue("machine_id", started.machineId()).log("Turn started");
 *     case ExecutionEvent.StateEnteredEvent entered -> log.atDebug().addKeyValue("state", entered.stateName()).log("State entered");
 *     case ExecutionEvent.StateExitedEvent exited -> log.atDebug().addKeyValue("state", exited.stateName()).addKeyValue("duration_ms", exited.duration().toMillis()).log("State exited");
 *     case ExecutionEvent.TransitionEvaluatedEvent trans -> log.atDebug().addKeyValue("source", trans.sourceState()).addKeyValue("target", trans.targetState()).log("Transition evaluated");
 *     case ExecutionEvent.SignalAwaitedEvent awaited -> log.atInfo().addKeyValue("signal", awaited.expectedSignal()).log("Awaiting signal");
 *     case ExecutionEvent.SignalDeliveredEvent delivered -> log.atInfo().addKeyValue("signal", delivered.signalName()).log("Signal delivered");
 *     case ExecutionEvent.TurnSuspendedEvent suspended -> log.atInfo().addKeyValue("state", suspended.stateName()).log("Turn suspended");
 *     case ExecutionEvent.TurnCompletedEvent completed -> log.atInfo().addKeyValue("machine_id", completed.machineId()).log("Turn completed");
 *     case ExecutionEvent.TurnCompensatedEvent compensated -> log.atWarn().addKeyValue("failed_state", compensated.failedStateName()).log("Turn compensated");
 *     case ExecutionEvent.TurnFailedEvent failed -> log.atError().addKeyValue("failed_state", failed.failedStateName()).setCause(failed.cause()).log("Turn failed");
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

    /**
     * Emitted when an orchestration or state machine turn begins execution.
     */
    record TurnStartedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @Nullable String correlationKey,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public TurnStartedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a turn suspends execution (e.g. waiting for an inbound signal or human input).
     */
    record TurnSuspendedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @Nullable String expectedSignal,
            @Nullable String correlationKey,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public TurnSuspendedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when an execution reaches a terminal end state and completes successfully.
     */
    record TurnCompletedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @Nullable String finalStateName,
            @Nullable String correlationKey,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public TurnCompletedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a turn fails and automated LIFO Saga compensations are executed.
     */
    record TurnCompensatedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String failedStateName,
            @NonNull List<String> compensatedStates,
            @Nullable Throwable cause,
            @Nullable String correlationKey,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public TurnCompensatedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(failedStateName, "failedStateName must not be null");
            Objects.requireNonNull(compensatedStates, "compensatedStates must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
            compensatedStates = List.copyOf(compensatedStates);
        }
    }

    /**
     * Emitted when a turn fails due to an unhandled exception without Saga compensation.
     */
    record TurnFailedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String failedStateName,
            @NonNull Throwable cause,
            @Nullable String correlationKey,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public TurnFailedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(failedStateName, "failedStateName must not be null");
            Objects.requireNonNull(cause, "cause must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted immediately before executing a state's business logic action.
     */
    record StateEnteredEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public StateEnteredEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted immediately after completing a state's business logic action.
     */
    record StateExitedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public StateExitedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a transition routing function evaluates and resolves the next target state.
     */
    record TransitionEvaluatedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String sourceState,
            @NonNull String targetState,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public TransitionEvaluatedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(sourceState, "sourceState must not be null");
            Objects.requireNonNull(targetState, "targetState must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a workflow halts at a signal wait state expecting an external event.
     */
    record SignalAwaitedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull String expectedSignal,
            @Nullable String correlationKey,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public SignalAwaitedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(expectedSignal, "expectedSignal must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when an incoming signal arrives and is dispatched to the waiting state machine.
     */
    record SignalDeliveredEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull String signalName,
            @Nullable String correlationKey,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public SignalDeliveredEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(signalName, "signalName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted immediately after executing a state's business logic action.
     */
    record ActionExecutedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ActionExecutedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted immediately before executing a single state's compensation rollback action.
     */
    record CompensationStepStartedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            boolean isRouted,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public CompensationStepStartedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted immediately after a single state's compensation rollback action completes successfully.
     */
    record CompensationStepCompletedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public CompensationStepCompletedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when an individual compensation step fails.
     */
    record CompensationStepFailedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull Throwable cause,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public CompensationStepFailedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(cause, "cause must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when an action fails and is scheduled for retry after a backoff delay.
     */
    record RetryAttemptedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            int attempt,
            int maxAttempts,
            @NonNull Duration delay,
            @NonNull Throwable lastCause,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public RetryAttemptedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(delay, "delay must not be null");
            Objects.requireNonNull(lastCause, "lastCause must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when all configured retry attempts have failed.
     */
    record RetryExhaustedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            int attempts,
            @NonNull Throwable finalCause,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public RetryExhaustedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(finalCause, "finalCause must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when an orchestration state invokes a sub-workflow, establishing parent-child lineage.
     */
    record ChildMachineSpawnedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull UUID childMachineId,
            @NonNull String childMachineName,
            @NonNull String parentStateName,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ChildMachineSpawnedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(childMachineId, "childMachineId must not be null");
            Objects.requireNonNull(childMachineName, "childMachineName must not be null");
            Objects.requireNonNull(parentStateName, "parentStateName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a child sub-workflow finishes execution.
     */
    record ChildMachineCompletedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull UUID childMachineId,
            @NonNull String childMachineName,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ChildMachineCompletedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(childMachineId, "childMachineId must not be null");
            Objects.requireNonNull(childMachineName, "childMachineName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a parallel state forks concurrent execution branches on virtual threads.
     */
    record ParallelForkStartedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull List<String> branchNames,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ParallelForkStartedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(branchNames, "branchNames must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
            branchNames = List.copyOf(branchNames);
        }
    }

    /**
     * Emitted when an individual parallel branch finishes its action.
     */
    record ParallelBranchCompletedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull String branchName,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ParallelBranchCompletedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(branchName, "branchName must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when all parallel branches join and reduction completes.
     */
    record ParallelJoinCompletedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            int totalBranches,
            @NonNull Duration duration,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ParallelJoinCompletedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(duration, "duration must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a workflow halts waiting for an external signal and the deadline expires.
     */
    record SignalTimedOutEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull String expectedSignal,
            @Nullable String correlationKey,
            @NonNull Duration timeout,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public SignalTimedOutEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(expectedSignal, "expectedSignal must not be null");
            Objects.requireNonNull(timeout, "timeout must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when an incoming signal arrives but cannot be delivered.
     */
    record SignalDiscardedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String signalName,
            @Nullable String correlationKey,
            @NonNull String reason,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public SignalDiscardedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(signalName, "signalName must not be null");
            Objects.requireNonNull(reason, "reason must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a state's loop visit count exceeds maxVisits.
     */
    record StateVisitLimitExceededEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            int visitLimit,
            @Nullable String fallbackState,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public StateVisitLimitExceededEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when total transitions breach the safety circuit breaker threshold.
     */
    record CircuitBreakerTrippedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            int maxTransitions,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public CircuitBreakerTrippedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when an operator or control plane cancels an active execution.
     */
    record ExecutionCancelledEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @Nullable String stateName,
            @NonNull String operatorId,
            @NonNull String reason,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ExecutionCancelledEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(operatorId, "operatorId must not be null");
            Objects.requireNonNull(reason, "reason must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when an execution is paused by an operator or control plane.
     */
    record ExecutionPausedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull String reason,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ExecutionPausedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(reason, "reason must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted when a paused execution is resumed.
     */
    record ExecutionResumedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public ExecutionResumedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }
}
