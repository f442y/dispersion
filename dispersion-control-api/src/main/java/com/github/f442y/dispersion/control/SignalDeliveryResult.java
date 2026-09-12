package com.github.f442y.dispersion.control;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Result of delivering an external signal to a suspended workflow via the Control Plane.
 *
 * @param delivered      True if the signal was successfully dispatched to a matching execution
 * @param message        Human-readable diagnostic or informational message
 * @param machineName    The state machine name
 * @param correlationKey The domain correlation key targeted
 * @param signalName     The signal identifier
 * @param completed      True if the resulting turn brought the workflow to a terminal state
 * @param suspended      True if the resulting turn suspended the workflow at another signal point
 * @param resultingState The state key name after turn completion or suspension, or null if failed
 * @param errorMessage   Error message if the turn failed or compensated, or null
 */
public record SignalDeliveryResult(
        boolean delivered,
        @NonNull String message,
        @NonNull String machineName,
        @NonNull String correlationKey,
        @NonNull String signalName,
        boolean completed,
        boolean suspended,
        @Nullable String resultingState,
        @Nullable String errorMessage
) {
    public SignalDeliveryResult {
        Objects.requireNonNull(message, "message must not be null");
        Objects.requireNonNull(machineName, "machineName must not be null");
        Objects.requireNonNull(correlationKey, "correlationKey must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");
    }

    /**
     * Factory for an unsuccessful signal delivery attempt (e.g. machine not found or no matching instance).
     */
    @NonNull
    public static SignalDeliveryResult failure(
            @NonNull String machineName,
            @NonNull String correlationKey,
            @NonNull String signalName,
            @NonNull String message
    ) {
        return new SignalDeliveryResult(
                false,
                message,
                machineName,
                correlationKey,
                signalName,
                false,
                false,
                null,
                message
        );
    }
}
