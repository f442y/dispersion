package com.github.f442y.dispersion.exception;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when an error occurs during transition resolution or when a transition returns an illegal target state
 * not declared in the state's permitted target set (runtime graph adjacency validation failure).
 */
public final class TransitionException extends StateMachineException {

    private final String sourceStateName;
    private final String targetStateName;

    public TransitionException(@NonNull String sourceStateName, @Nullable String targetStateName, @NonNull String message) {
        super("Transition error from state [" + sourceStateName + "] to [" + targetStateName + "]: " + message);
        this.sourceStateName = sourceStateName;
        this.targetStateName = targetStateName;
    }

    public TransitionException(@NonNull String sourceStateName, @NonNull String message, @Nullable Throwable cause) {
        super("Transition error in state [" + sourceStateName + "]: " + message, cause);
        this.sourceStateName = sourceStateName;
        this.targetStateName = null;
    }

    @NonNull
    public String getSourceStateName() {
        return sourceStateName;
    }

    @Nullable
    public String getTargetStateName() {
        return targetStateName;
    }
}
