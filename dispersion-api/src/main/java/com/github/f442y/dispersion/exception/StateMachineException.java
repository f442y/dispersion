package com.github.f442y.dispersion.exception;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Sealed root base class for all Finite State Machine runtime exceptions in Dispersion.
 * <p>
 * Using a sealed class enables exhaustive pattern matching with Java 25 {@code switch} expressions
 * across all possible failure modes of state machine execution.
 */
public abstract sealed class StateMachineException extends RuntimeException
        permits ActionException,
                TransitionException,
                BackpressureException,
                MaxTransitionsExceededException,
                MaxStateVisitsExceededException,
                CompensationException {

    protected StateMachineException(@NonNull String message) {
        super(message);
    }

    protected StateMachineException(@NonNull String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
