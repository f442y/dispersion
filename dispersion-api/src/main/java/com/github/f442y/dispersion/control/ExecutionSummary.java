package com.github.f442y.dispersion.control;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;

/**
 * Snapshot summary of an execution instance tracked by the Control Plane.
 *
 * @param executionId      The unique identifier of the execution (UUID string)
 * @param machineName      The state machine name
 * @param currentState     The current or terminal state key name
 * @param status           The current {@link ExecutionStatus}
 * @param startTime        Timestamp when execution or turn began
 * @param endTime          Timestamp when execution completed or compensated, or null if active
 * @param correlationKey   Domain correlation key, if assigned
 * @param suspendedSignal  Name of the signal being awaited when suspended, or null
 * @param transitionsCount Number of state transitions evaluated
 * @param lastUpdated      Timestamp of the most recent lifecycle event
 * @param errorMessage     Error message if execution failed or triggered Saga rollback, or null
 */
public record ExecutionSummary(
        @NonNull String executionId,
        @NonNull String machineName,
        @NonNull String currentState,
        @NonNull ExecutionStatus status,
        @NonNull Instant startTime,
        @Nullable Instant endTime,
        @Nullable String correlationKey,
        @Nullable String suspendedSignal,
        long transitionsCount,
        @NonNull Instant lastUpdated,
        @Nullable String errorMessage
) {
    public ExecutionSummary {
        Objects.requireNonNull(executionId, "executionId must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(currentState, "currentState must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(startTime, "startTime must not be null");
        Objects.requireNonNull(lastUpdated, "lastUpdated must not be null");
    }

    /**
     * Creates a new updated copy of this summary reflecting a transition to a new state.
     */
    @NonNull
    public ExecutionSummary withState(@NonNull String newState, @NonNull Instant eventTime) {
        return new ExecutionSummary(
                executionId,
                machineName,
                newState,
                status,
                startTime,
                endTime,
                correlationKey,
                suspendedSignal,
                transitionsCount + 1,
                eventTime,
                errorMessage
        );
    }

    /**
     * Creates a new updated copy of this summary reflecting a status change.
     */
    @NonNull
    public ExecutionSummary withStatus(
            @NonNull ExecutionStatus newStatus,
            @Nullable Instant completedEndTime,
            @NonNull Instant eventTime,
            @Nullable String newErrorMessage
    ) {
        return new ExecutionSummary(
                executionId,
                machineName,
                currentState,
                newStatus,
                startTime,
                completedEndTime != null ? completedEndTime : endTime,
                correlationKey,
                suspendedSignal,
                transitionsCount,
                eventTime,
                newErrorMessage != null ? newErrorMessage : errorMessage
        );
    }

    /**
     * Creates a new updated copy of this summary reflecting a suspension point.
     */
    @NonNull
    public ExecutionSummary withSuspension(
            @NonNull String suspendedState,
            @NonNull String expectedSignal,
            @Nullable String updatedCorrelationKey,
            @NonNull Instant eventTime
    ) {
        return new ExecutionSummary(
                executionId,
                machineName,
                suspendedState,
                ExecutionStatus.SUSPENDED,
                startTime,
                endTime,
                updatedCorrelationKey != null ? updatedCorrelationKey : correlationKey,
                expectedSignal,
                transitionsCount,
                eventTime,
                errorMessage
        );
    }
}
