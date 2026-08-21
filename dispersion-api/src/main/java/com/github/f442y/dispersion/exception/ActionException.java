package com.github.f442y.dispersion.exception;

import org.jspecify.annotations.Nullable;

/**
 * Thrown when an exception occurs during the execution of a state's {@link com.github.f442y.dispersion.state.Action}.
 */
public final class ActionException extends StateMachineException {

    public ActionException(@Nullable String message) {
        super(message);
    }

    public ActionException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public ActionException(@Nullable Throwable cause) {
        super(cause);
    }
}
