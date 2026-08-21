package com.github.f442y.dispersion.atomic;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.executor.AdmissionController;
import com.github.f442y.dispersion.executor.BufferedStateMachineExecutor;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;

/**
 * Standard execution coordinator for Atomic (Micro / Thread-Bound) State Machines on Java Virtual Threads.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by the state machine
 * @param <OUTPUT>    The type of output result produced upon state machine completion
 */
public class AtomicStateMachineExecutor<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        extends BufferedStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

    public AtomicStateMachineExecutor(
            @NonNull String machineName,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration
    ) {
        super(machineName, configuration, new AdmissionController(10_000));
    }

    public AtomicStateMachineExecutor(
            @NonNull String machineName,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            int bufferSize
    ) {
        super(machineName, configuration, new AdmissionController(bufferSize));
    }

    public AtomicStateMachineExecutor(
            @NonNull String machineName,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull AdmissionController admissionController
    ) {
        super(machineName, configuration, admissionController);
    }
}
