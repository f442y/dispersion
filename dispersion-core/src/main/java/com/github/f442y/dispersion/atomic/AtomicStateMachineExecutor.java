package com.github.f442y.dispersion.atomic;

import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.executor.AdmissionController;
import com.github.f442y.dispersion.executor.BufferedStateMachineExecutor;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;

/**
 * High-throughput Atomic State Machine Executor dispatching lightweight execution graphs
 * onto dedicated Java 25 Virtual Threads with admission control.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
 * @param <STATE_KEY> The state identifier enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output return type
 */
public class AtomicStateMachineExecutor<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> extends BufferedStateMachineExecutor<CONTEXT, STATE_KEY, INPUT, OUTPUT> {

    public AtomicStateMachineExecutor(
            @NonNull String name,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration
    ) {
        this(name, configuration, Runtime.getRuntime().availableProcessors() * 3);
    }

    public AtomicStateMachineExecutor(
            @NonNull String name,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            int maxConcurrent
    ) {
        this(name, configuration, new AdmissionController(maxConcurrent));
    }

    public AtomicStateMachineExecutor(
            @NonNull String name,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull AdmissionController admissionController
    ) {
        super(name, configuration, admissionController);
    }
}
