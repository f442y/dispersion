package com.github.f442y.dispersion;

import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.ActionException;
import com.github.f442y.dispersion.exception.MaxStateVisitsExceededException;
import com.github.f442y.dispersion.exception.MaxTransitionsExceededException;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.exception.TransitionException;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;

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
 * @param <STATE_KEY> The state identifier enum type
 * @param <INPUT>     The input payload type
 * @param <OUTPUT>    The output return type
 */
public abstract class AbstractStateMachineCallable<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> implements Callable<OUTPUT>, StateMachine<CONTEXT, STATE_KEY> {

    private static final Logger log = LoggerFactory.getLogger(AbstractStateMachineCallable.class);

    private UUID uuid;
    private final StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;
    private final CONTEXT initialContext;
    private final INPUT input;

    protected AbstractStateMachineCallable(
            @Nullable UUID uuid,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) {
        this.uuid = uuid;
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.initialContext = initialContext;
        this.input = input;
    }

    /**
     * Lazily retrieves or generates the unique execution identifier.
     *
     * @return The non-null {@link UUID} of this state machine execution
     */
    @NonNull
    @Override
    public UUID uuid() {
        if (uuid == null) {
            uuid = UUID.randomUUID();
        }
        return uuid;
    }

    @NonNull
    public StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration() {
        return configuration;
    }

    @Override
    @Nullable
    public OUTPUT call() throws Exception {
        return executeDirect(uuid, configuration, initialContext, input);
    }

    /**
     * Ultra-high-speed direct execution path with zero object allocations in the traversal loop.
     * <p>
     * Runs directly within the calling thread context, checking circuit breakers, evaluating actions,
     * and computing transitions via $O(1)$ array lookups.
     *
     * @param executionId    Optional execution UUID for logging (nullable for zero-allocation runs)
     * @param configuration  The state machine topology and lifecycle configuration
     * @param initialContext Optional pre-constructed context (if null, factory creates a fresh instance)
     * @param input          Optional domain input payload
     * @param <CONTEXT>      Context type
     * @param <STATE_KEY>    State enum type
     * @param <INPUT>        Input type
     * @param <OUTPUT>       Output type
     * @return The computed output, or {@code null}
     * @throws Exception If an unhandled action exception or transition guard failure occurs
     */
    @Nullable
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    OUTPUT executeDirect(
            @Nullable UUID executionId,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) throws Exception {

        StateMap<CONTEXT, STATE_KEY> stateMap = configuration.getStateMap();

        // 1. Initialize Context
        CONTEXT context = initialContext;
        if (context == null) {
            var factory = configuration.stateMachineContextFactory();
            if (factory == null) {
                throw new IllegalStateException("StateMachineContextFactory must be configured or initialContext provided");
            }
            context = factory.newInstance();
        }

        InputFunction<CONTEXT, INPUT> inputFn = configuration.inputFunction();
        if (inputFn != null) {
            context = inputFn.apply(context, input);
        }

        // 2. Loop state (allocated conditionally for zero heap waste)
        int[] visitCounts = stateMap.hasVisitLimits() ? new int[stateMap.getStateKeyClass().getEnumConstants().length] : null;
        int totalTransitions = 0;
        int maxTransitions = configuration.getMaxTransitions();

        STATE_KEY currentStateKey = stateMap.getInitialState();

        try {
            while (currentStateKey != null) {
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
                    int visits = ++visitCounts[ordinal];
                    int maxVisits = currentState.maxVisits();

                    if (maxVisits > 0 && visits > maxVisits) {
                        STATE_KEY fallback = currentState.maxVisitsFallback();
                        if (fallback != null) {
                            if (log.isWarnEnabled()) {
                                log.warn("[{}] State [{}] exceeded max visits ({}); diverting to fallback [{}]",
                                        executionId, currentStateKey, maxVisits, fallback);
                            }
                            currentStateKey = fallback;
                            totalTransitions++;
                            continue;
                        } else {
                            throw new MaxStateVisitsExceededException(currentStateKey.name(), maxVisits);
                        }
                    }
                }

                // 5. Execute state business logic
                try {
                    context = currentState.action().execute(context);
                } catch (StateMachineException sme) {
                    throw sme;
                } catch (Throwable t) {
                    throw new ActionException(currentStateKey.name(), t);
                }

                // 6. Evaluate transition logic
                STATE_KEY nextStateKey;
                try {
                    nextStateKey = currentState.transition().nextState(context);
                } catch (Throwable t) {
                    throw new TransitionException(currentStateKey.name(), t.getMessage(), t);
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

            if (configuration.stateMachineFinishTrigger() != null) {
                configuration.stateMachineFinishTrigger().accept(context);
            }

            OutputFunction<CONTEXT, OUTPUT> outputFn = configuration.outputFunction();
            return (outputFn != null) ? outputFn.apply(context) : null;

        } catch (Throwable t) {
            if (configuration.stateMachineExceptionTrigger() != null) {
                try {
                    configuration.stateMachineExceptionTrigger().accept(context, t);
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
}
