package com.github.f442y.dispersion.fsm.core;

import com.github.f442y.dispersion.fsm.config.InputFunction;
import com.github.f442y.dispersion.fsm.config.OutputFunction;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.context.StateMachineContextFactory;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.fsm.exception.ActionException;
import com.github.f442y.dispersion.fsm.exception.MaxStateVisitsExceededException;
import com.github.f442y.dispersion.fsm.exception.MaxTransitionsExceededException;
import com.github.f442y.dispersion.fsm.exception.StateMachineException;
import com.github.f442y.dispersion.fsm.exception.TransitionException;
import com.github.f442y.dispersion.fsm.state.State;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Ultra-lightweight, thread-confined execution loop traversing a Finite State Machine graph.
 *
 * <h2>Performance Architecture &amp; Virtual Thread Optimizations</h2>
 * This execution engine is engineered specifically for high-throughput Java 25+ Virtual Threads
 * (Project Loom) and low-latency microservices:
 * <ul>
 *   <li><b>Zero-Allocation Hot Path:</b> The inner transition loop allocates <i>zero objects on the heap</i>.
 *       State lookups and terminal state validations operate directly on precomputed, flat primitive arrays
 *       via {@link StateMap#getStateFast(int)} and {@link StateMap#isEndStateFast(int)}.</li>
 *   <li><b>Thread Confinement (Zero-Sync):</b> State machine contexts are strictly confined to the executing thread
 *       for the duration of the turn. This eliminates memory barriers, volatile read/write overhead, and mutex locks,
 *       enabling raw memory mutation speeds.</li>
 *   <li><b>Conditional Primitive Visit Tracking:</b> If a state machine does not configure per-state visit limits
 *       ({@link StateMap#hasVisitLimits()} is {@code false}), the visit tracking array allocation is completely skipped.
 *       When limits are present, a flat primitive {@code int[]} indexed by enum ordinal is used, avoiding boxed
 *       {@link Integer} allocations and hash lookups entirely.</li>
 *   <li><b>Direct Synchronous Fast-Path:</b> Via {@link #executeDirect(UUID, StateMachineConfiguration, StateMachineContext, Object)},
 *       synchronous executions run directly on the caller's virtual thread without thread-hopping, context switches, or
 *       {@link java.util.concurrent.CompletableFuture} allocation overhead.</li>
 *   <li><b>Lazy UUID Generation:</b> Cryptographic pseudo-random number generator (PRNG) entropy locks are bypassed
 *       by deferring {@link UUID#randomUUID()} generation until explicitly requested.</li>
 * </ul>
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext}
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The optional turn input type
 * @param <OUTPUT>    The terminal output type
 */
public abstract class AbstractStateMachineCallable<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> implements Callable<OUTPUT> {

    private static final Logger log = LoggerFactory.getLogger(AbstractStateMachineCallable.class);

    protected final StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;
    protected final CONTEXT initialContext;
    protected final INPUT input;
    protected UUID executionId;

    protected AbstractStateMachineCallable(
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.initialContext = initialContext;
        this.input = input;
    }

    protected AbstractStateMachineCallable(
            @Nullable UUID executionId,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) {
        this(configuration, initialContext, input);
        this.executionId = executionId;
    }

    @NonNull
    public UUID executionId() {
        if (executionId == null) {
            executionId = UUID.randomUUID();
        }
        return executionId;
    }

    public StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration() {
        return configuration;
    }

    @Override
    public OUTPUT call() throws Exception {
        return executeDirect(executionId(), configuration, initialContext, input);
    }

    /**
     * Direct zero-overhead synchronous execution of a state machine turn on the current thread.
     *
     * @param executionId   Optional execution ID (generated lazily if null)
     * @param configuration The state machine configuration
     * @param initialContext Optional initial context
     * @param input         Optional turn input
     * @return The output produced by the state machine
     * @throws Exception If an unhandled action or transition exception occurs
     */
    @Nullable
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT> OUTPUT executeDirect(
            @Nullable UUID executionId,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) throws Exception {
        Objects.requireNonNull(configuration, "configuration must not be null");

        ExecutionEventListener eventListener = configuration.eventListener();
        UUID effectiveId = executionId != null ? executionId : (eventListener != null ? UUID.randomUUID() : null);
        long turnStartNanos = (eventListener != null) ? System.nanoTime() : 0L;

        StateMap<CONTEXT, STATE_KEY> stateMap = configuration.getStateMap();
        CONTEXT context = initialContext;

        // 1. Resolve context
        if (context == null) {
            StateMachineContextFactory<CONTEXT> factory = configuration.stateMachineContextFactory();
            if (factory != null) {
                context = factory.newInstance();
            }
        }

        if (context == null) {
            throw new IllegalStateException("StateMachine execution failed: Context is null and no contextFactory is configured");
        }

        InputFunction<CONTEXT, INPUT> inputFn = configuration.inputFunction();
        if (inputFn != null) {
            context = inputFn.apply(context, input);
        }

        // 2. Loop state (allocated conditionally for zero heap waste)
        int[] visitCounts = stateMap.hasVisitLimits() ? new int[stateMap.stateCount()] : null;
        int totalTransitions = 0;
        int maxTransitions = configuration.getMaxTransitions();

        STATE_KEY currentStateKey = stateMap.getInitialState();

        if (eventListener != null) {
            safeNotify(eventListener, new ExecutionEvent.TurnStartedEvent(
                    effectiveId,
                    configuration.getMachineName(),
                    null,
                    Instant.now()
            ));
        }

        try {
            while (true) {
                int ordinal = currentStateKey.ordinal();

                // 1. Global circuit breaker check
                if (maxTransitions > 0 && totalTransitions >= maxTransitions) {
                    throw new MaxTransitionsExceededException(maxTransitions);
                }

                // 2. Fast $O(1)$ terminal end-state check (1 CPU instruction)
                if (stateMap.isEndStateFast(ordinal)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Reached terminal state [{}]", executionId, currentStateKey);
                    }
                    break;
                }

                // 3. Fast $O(1)$ array dereference
                State<CONTEXT, STATE_KEY> currentState = stateMap.getStateFast(ordinal);

                // 4. Per-state visit limit check (primitive array index, zero boxing)
                if (visitCounts != null) {
                    int maxVisits = currentState.maxVisits();
                    if (maxVisits > 0) {
                        int currentCount = ++visitCounts[ordinal];
                        if (currentCount > maxVisits) {
                            STATE_KEY fallback = currentState.maxVisitsFallback();
                            if (fallback != null) {
                                if (log.isWarnEnabled()) {
                                    log.warn("[{}] State [{}] exceeded visit limit ({}); diverting to fallback [{}]",
                                            executionId, currentStateKey, maxVisits, fallback);
                                }
                                currentStateKey = fallback;
                                continue;
                            } else {
                                throw new MaxStateVisitsExceededException(currentStateKey.name(), maxVisits);
                            }
                        }
                    }
                }

                if (eventListener != null) {
                    safeNotify(eventListener, new ExecutionEvent.StateEnteredEvent(
                            effectiveId,
                            configuration.getMachineName(),
                            currentStateKey.name(),
                            Instant.now()
                    ));
                }

                long stateStartNanos = (eventListener != null) ? System.nanoTime() : 0L;

                // 5. Execute state business logic
                try {
                    context = currentState.action().execute(context);
                } catch (StateMachineException sme) {
                    throw sme;
                } catch (Throwable t) {
                    throw new ActionException(currentStateKey.name(), t);
                }

                if (eventListener != null) {
                    safeNotify(eventListener, new ExecutionEvent.StateExitedEvent(
                            effectiveId,
                            configuration.getMachineName(),
                            currentStateKey.name(),
                            Duration.ofNanos(System.nanoTime() - stateStartNanos),
                            Instant.now()
                    ));
                }

                // 6. Evaluate transition logic
                STATE_KEY nextStateKey;
                try {
                    nextStateKey = currentState.transition().nextState(context);
                } catch (Throwable t) {
                    throw new TransitionException(currentStateKey.name(), t.getMessage(), t);
                }

                if (eventListener != null && nextStateKey != null) {
                    safeNotify(eventListener, new ExecutionEvent.TransitionEvaluatedEvent(
                            effectiveId,
                            configuration.getMachineName(),
                            currentStateKey.name(),
                            nextStateKey.name(),
                            Instant.now()
                    ));
                }

                totalTransitions++;

                if (nextStateKey == null) {
                    break;
                }

                // 7. Runtime Graph Adjacency Verification Guard
                if (stateMap.hasAdjacencyConstraints() &&
                    !currentState.permittedTargets().isEmpty() &&
                    !currentState.permittedTargets().contains(nextStateKey) &&
                    !stateMap.isEndStateFast(nextStateKey.ordinal())) {
                    throw new TransitionException(
                            currentStateKey.name(),
                            nextStateKey.name(),
                            "Transition returned undeclared target state"
                    );
                }

                currentStateKey = nextStateKey;
            }

            if (eventListener != null) {
                safeNotify(eventListener, new ExecutionEvent.TurnCompletedEvent(
                        effectiveId,
                        configuration.getMachineName(),
                        (currentStateKey != null) ? currentStateKey.name() : null,
                        null,
                        Duration.ofNanos(System.nanoTime() - turnStartNanos),
                        Instant.now()
                ));
            }

            Consumer<CONTEXT> finishTrigger = configuration.stateMachineFinishTrigger();
            if (finishTrigger != null) {
                finishTrigger.accept(context);
            }

            OutputFunction<CONTEXT, OUTPUT> outputFn = configuration.outputFunction();
            return (outputFn != null) ? outputFn.apply(context) : null;

        } catch (Throwable t) {
            if (eventListener != null) {
                safeNotify(eventListener, new ExecutionEvent.TurnCompensatedEvent(
                        effectiveId,
                        configuration.getMachineName(),
                        (currentStateKey != null) ? currentStateKey.name() : "UNKNOWN",
                        List.of(),
                        t,
                        null,
                        Duration.ofNanos(System.nanoTime() - turnStartNanos),
                        Instant.now()
                ));
            }

            BiConsumer<CONTEXT, Throwable> exceptionTrigger = configuration.stateMachineExceptionTrigger();
            if (exceptionTrigger != null) {
                try {
                    exceptionTrigger.accept(context, t);
                } catch (Throwable triggerEx) {
                    log.error("[{}] Exception in stateMachineExceptionTrigger callback", executionId, triggerEx);
                }
            }

            if (t instanceof Exception e) {
                throw e;
            }
            throw new RuntimeException(t);
        }
    }

    private static void safeNotify(@NonNull ExecutionEventListener listener, @NonNull ExecutionEvent event) {
        try {
            listener.onEvent(event);
        } catch (Throwable t) {
            log.error("ExecutionEventListener [{}] threw exception: {}", listener.getClass().getName(), t.getMessage(), t);
        }
    }
}
