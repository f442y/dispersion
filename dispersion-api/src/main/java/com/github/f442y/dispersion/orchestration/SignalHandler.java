package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;

/**
 * Functional interface responsible for merging an incoming external signal or event payload
 * into the state machine context upon workflow rehydration.
 *
 * @param <CONTEXT> The orchestration state machine context type
 * @param <SIGNAL>  The incoming signal payload type
 */
@FunctionalInterface
public interface SignalHandler<CONTEXT extends StateMachineContext, SIGNAL> {

    /**
     * Merges the incoming signal payload into the rehydrated state machine context.
     *
     * @param context The current context of the rehydrated state machine
     * @param signal  The external signal payload
     * @return The updated context
     * @throws Exception If an error occurs while processing the signal
     */
    @NonNull
    CONTEXT handleSignal(@NonNull CONTEXT context, @NonNull SIGNAL signal) throws Exception;
}
