package com.github.f442y.dispersion.exception;

import org.jspecify.annotations.Nullable;

/**
 * Thrown when a state machine execution request is rejected due to admission control saturation or timeout.
 */
public final class BackpressureException extends StateMachineException {

    public BackpressureException(@Nullable String message) {
        super(message);
    }

    public BackpressureException(@Nullable String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public BackpressureException(@Nullable Throwable cause) {
        super(cause);
    }
}
