package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Functional interface responsible for merging an incoming external signal or command payload
 * into a suspended orchestration context upon wake-up.
 *
 * @param <CONTEXT> The context type
 * @param <SIGNAL>  The signal or command payload type
 */
@FunctionalInterface
public interface SignalHandler<CONTEXT extends StateMachineContext, SIGNAL> {

    /**
     * Integrates the delivered signal payload into the state machine context.
     *
     * @param context       The suspended state machine context
     * @param signalPayload The external signal or command payload
     * @return The updated context ready to resume execution
     * @throws Exception If signal processing fails
     */
    @NonNull
    CONTEXT handleSignal(@NonNull CONTEXT context, @Nullable SIGNAL signalPayload) throws Exception;
}
