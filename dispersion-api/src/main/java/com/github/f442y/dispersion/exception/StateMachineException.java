package com.github.f442y.dispersion.exception;

import org.jspecify.annotations.Nullable;

/**
 * Sealed root abstract exception class for all errors and exceptional conditions originating
 * within the Finite State Machine engine.
 * <p>
 * Using a sealed class hierarchy ensures compile-time exhaustiveness in {@code switch} pattern matching.
 */
public abstract sealed class StateMachineException extends Exception
        permits ActionException, TransitionException, BackpressureException,
                MaxTransitionsExceededException, MaxStateVisitsExceededException {

    /**
     * Constructs a new state machine exception with the specified detail message.
     *
     * @param message The detail message explaining the cause of the failure
     */
    public StateMachineException(@Nullable String message) {
        super(message);
    }

    /**
     * Constructs a new state machine exception with the specified detail message and root cause.
     *
     * @param message The detail message explaining the cause of the failure
     * @param cause   The underlying cause of the failure
     */
    public StateMachineException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new state machine exception with the specified root cause.
     *
     * @param cause The underlying cause of the failure
     */
    public StateMachineException(@Nullable Throwable cause) {
        super(cause);
    }
}
