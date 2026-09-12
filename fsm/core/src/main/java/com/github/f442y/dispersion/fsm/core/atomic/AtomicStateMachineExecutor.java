package com.github.f442y.dispersion.fsm.core.atomic;

import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.executor.AdmissionController;
import com.github.f442y.dispersion.fsm.core.executor.BufferedStateMachineExecutor;
import com.github.f442y.dispersion.fsm.state.StateKey;
import org.jspecify.annotations.NonNull;

/**
 * High-throughput Atomic (Micro) State Machine Executor dispatching lightweight execution graphs
 * onto dedicated Java 25 Virtual Threads with admission backpressure control.
 *
 * <h2>Micro State Machine Architecture</h2>
 * Designed for microsecond-scale transactional pipelines, protocol parsing, and business rule evaluation:
 * <ul>
 *   <li>Traverses state graphs on a single thread (synchronously or on unpinned Virtual Threads).</li>
 *   <li>Guarantees strict thread-confinement of {@link StateMachineContext} without synchronization locks.</li>
 *   <li>Protected by loop circuit breakers ({@code maxTransitions}) and per-state visit limits ({@code maxVisits}).</li>
 *   <li>Easily embeddable inside macro orchestrations as sub-state machines via child machine steps.</li>
 * </ul>
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
