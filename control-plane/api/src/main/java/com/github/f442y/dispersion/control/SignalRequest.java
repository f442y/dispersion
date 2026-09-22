package com.github.f442y.dispersion.control;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Incoming request payload for delivering a control plane signal to an execution.
 *
 * @param machineName    The state machine name
 * @param correlationKey The correlation key of the target workflow instance
 * @param signalName     The identifier of the signal
 * @param payload        Optional signal payload (can be null)
 */
public record SignalRequest(
        @NonNull String machineName,
        @NonNull String correlationKey,
        @NonNull String signalName,
        @Nullable Object payload
) {
    public SignalRequest {
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");
    }
}
