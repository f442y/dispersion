package com.github.f442y.dispersion.orchestration.command;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Contract representing an incoming external command or signal intended to advance
 * or instruct a suspended Orchestration State Machine.
 */
public interface SignalCommand {

    /**
     * Returns the name identifier of this signal command. Defaults to the class's simple name.
     *
     * @return The signal name
     */
    @NonNull
    default String signalName() {
        return getClass().getSimpleName();
    }

    /**
     * Returns the domain correlation key (e.g. orderId, accountId) used to locate the target workflow instance.
     *
     * @return The correlation key, or {@code null} if targeted by machine UUID directly
     */
    @Nullable
    default String correlationKey() {
        return null;
    }
}
