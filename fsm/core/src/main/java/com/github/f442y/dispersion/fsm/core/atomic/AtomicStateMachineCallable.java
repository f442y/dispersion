package com.github.f442y.dispersion.fsm.core.atomic;

import com.github.f442y.dispersion.fsm.core.AbstractStateMachineCallable;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.UUID;

/**
 * Thread-confined callable for high-throughput in-memory Atomic Finite State Machines on Virtual Threads.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
 * @param <STATE_KEY> The state identifier enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output return type
 */
public class AtomicStateMachineCallable<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> extends AbstractStateMachineCallable<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

    public AtomicStateMachineCallable(
            @Nullable UUID uuid,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) {
        super(uuid, configuration, initialContext, input);
    }
}
