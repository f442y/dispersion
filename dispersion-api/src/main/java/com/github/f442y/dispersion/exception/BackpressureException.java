package com.github.f442y.dispersion.exception;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when an executor's admission controller rejects or times out attempting to admit a new state machine task
 * due to capacity saturation or backpressure policy constraints.
 */
public final class BackpressureException extends StateMachineException {

    public BackpressureException(@NonNull String message) {
        super(message);
    }

    public BackpressureException(@NonNull String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
