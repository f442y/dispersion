package com.github.f442y.dispersion.exception;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when an unhandled exception or error occurs during the execution of a state's {@link com.github.f442y.dispersion.state.Action}.
 */
public final class ActionException extends StateMachineException {

    private final String stateName;

    public ActionException(@NonNull String stateName, @NonNull String message, @Nullable Throwable cause) {
        super("Error executing action in state [" + stateName + "]: " + message, cause);
        this.stateName = stateName;
    }

    public ActionException(@NonNull String stateName, @Nullable Throwable cause) {
        this(stateName, cause != null ? cause.getMessage() : "Unknown error", cause);
    }

    @NonNull
    public String getStateName() {
        return stateName;
    }
}
