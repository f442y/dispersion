package com.github.f442y.dispersion.event;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Sealed hierarchy of immutable lifecycle events emitted during state machine execution.
 *
 * <p>Supports exhaustive pattern matching in Java 25+ without requiring a default branch:</p>
 * <pre>{@code
 * switch (event) {
 *     case ExecutionEvent.TurnStartedEvent started -> log.info("Turn started: {}", started.machineId());
 *     case ExecutionEvent.StateEnteredEvent entered -> log.debug("Entered: {}", entered.stateName());
 *     case ExecutionEvent.StateExitedEvent exited -> log.debug("Exited: {} in {}ms", exited.stateName(), exited.duration().toMillis());
 *     case ExecutionEvent.TransitionEvaluatedEvent trans -> log.debug("Transition: {} -> {}", trans.sourceState(), trans.targetState());
 *     case ExecutionEvent.SignalAwaitedEvent awaited -> log.info("Awaiting signal [{}]", awaited.expectedSignal());
 *     case ExecutionEvent.SignalDeliveredEvent delivered -> log.info("Delivered signal [{}]", delivered.signalName());
 *     case ExecutionEvent.CommandDeduplicatedEvent dedup -> log.warn("Deduplicated command [{}]", dedup.commandId());
 *     case ExecutionEvent.TurnSuspendedEvent suspended -> log.info("Turn suspended at [{}]", suspended.stateName());
 *     case ExecutionEvent.TurnCompletedEvent completed -> log.info("Turn completed: {}", completed.machineId());
 *     case ExecutionEvent.TurnCompensatedEvent compensated -> log.warn("Turn compensated: {}", compensated.failedStateName());
 *     case ExecutionEvent.TurnFailedEvent failed -> log.error("Turn failed at [{}]: {}", failed.failedStateName(), failed.cause().getMessage());
 *     case ExecutionEvent.BatchBarrierReachedEvent barrier -> log.debug("Item [{}] reached barrier", barrier.itemKey());
 *     case ExecutionEvent.BatchBarrierUnlockedEvent unlocked -> log.debug("Barrier [{}] unlocked", unlocked.stateName());
 * }
 * }</pre>
 */
public sealed interface ExecutionEvent permits
        ExecutionEvent.TurnStartedEvent,
        ExecutionEvent.TurnSuspendedEvent,
        ExecutionEvent.TurnCompletedEvent,
        ExecutionEvent.TurnCompensatedEvent,
        ExecutionEvent.TurnFailedEvent,
        ExecutionEvent.StateEnteredEvent,
        ExecutionEvent.StateExitedEvent,
        ExecutionEvent.TransitionEvaluatedEvent,
        ExecutionEvent.SignalAwaitedEvent,
        ExecutionEvent.SignalDeliveredEvent,
        ExecutionEvent.CommandDeduplicatedEvent,
        ExecutionEvent.BatchBarrierReachedEvent,
        ExecutionEvent.BatchBarrierUnlockedEvent {

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
     * Emitted when an incoming command envelope is recognized as a duplicate and ignored.
     */
    record CommandDeduplicatedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull UUID commandId,
            @Nullable String correlationKey,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public CommandDeduplicatedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(commandId, "commandId must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted in batch orchestrations when an item reaches a synchronization barrier.
     */
    record BatchBarrierReachedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String itemKey,
            @NonNull String stateName,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public BatchBarrierReachedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(itemKey, "itemKey must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }

    /**
     * Emitted in batch orchestrations when a barrier unlocks and all items proceed.
     */
    record BatchBarrierUnlockedEvent(
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull String stateName,
            int itemCount,
            @NonNull Instant timestamp
    ) implements ExecutionEvent {
        public BatchBarrierUnlockedEvent {
            Objects.requireNonNull(machineId, "machineId must not be null");
            Objects.requireNonNull(machineName, "machineName must not be null");
            Objects.requireNonNull(stateName, "stateName must not be null");
            Objects.requireNonNull(timestamp, "timestamp must not be null");
        }
    }
}
