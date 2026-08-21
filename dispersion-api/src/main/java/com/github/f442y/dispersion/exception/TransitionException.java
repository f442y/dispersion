package com.github.f442y.dispersion.exception;

import org.jspecify.annotations.Nullable;

/**
 * Thrown when an exception occurs during the resolution of a state's {@link com.github.f442y.dispersion.state.Transition}.
 */
public final class TransitionException extends StateMachineException {

    public TransitionException(@Nullable String message) {
        super(message);
    }

    public TransitionException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public TransitionException(@Nullable Throwable cause) {
        super(cause);
    }
}
