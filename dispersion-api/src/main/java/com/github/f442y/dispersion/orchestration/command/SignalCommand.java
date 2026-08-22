package com.github.f442y.dispersion.orchestration.command;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Marker and contract interface for external signal commands delivered into an orchestration.
 */
public interface SignalCommand {

    /**
     * Returns the signal identifier name, defaulting to the simple class name of the command.
     *
     * @return The signal name
     */
    @NonNull
    default String signalName() {
        return getClass().getSimpleName();
    }

    /**
     * Optional domain correlation key (e.g. orderId, transactionId) used to route this command
     * to the matching suspended orchestration instance.
     *
     * @return The correlation key, or {@code null} if addressed by machine ID
     */
    @Nullable
    default String correlationKey() {
        return null;
    }
}
