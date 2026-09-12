package com.github.f442y.dispersion.fsm.exception;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when an error occurs during the execution of a Saga compensation rollback action.
 */
public final class CompensationException extends StateMachineException {

    private final String stateName;

    public CompensationException(@NonNull String stateName, @NonNull String message, @Nullable Throwable cause) {
        super("Error executing compensation rollback for state [" + stateName + "]: " + message, cause);
        this.stateName = stateName;
    }

    public CompensationException(@NonNull String stateName, @Nullable Throwable cause) {
        this(stateName, cause != null ? cause.getMessage() : "Unknown error", cause);
    }

    @NonNull
    public String getStateName() {
        return stateName;
    }
}
