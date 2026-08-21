package com.github.f442y.dispersion;

import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.MaxStateVisitsExceededException;
import com.github.f442y.dispersion.exception.MaxTransitionsExceededException;
import com.github.f442y.dispersion.exception.TransitionException;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unified abstract execution engine for both Atomic (Micro) and Orchestration (Macro) State Machines.
 * Provides the shared, bulletproof directed-graph traversal loop with visit limiter safeguards,
 * global transition circuit breakers, and strict runtime adjacency enforcement.
 *
 * @param <CONTEXT>   The concrete type of {@link StateMachineContext} managed by the state machine
 * @param <STATE_KEY> The enum type representing state identifiers in the state machine
 * @param <INPUT>     The type of input payload accepted by the state machine
 * @param <OUTPUT>    The type of output result produced upon state machine completion
 */
public abstract class AbstractStateMachineCallable<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        implements StateMachine<CONTEXT, STATE_KEY>, Callable<OUTPUT> {

    private static final Logger log = LoggerFactory.getLogger(AbstractStateMachineCallable.class);

    @NonNull
    protected final UUID uuid;

    @NonNull
    protected final StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;

    @Nullable
    protected final INPUT input;

    @Nullable
    protected final CONTEXT initialContext;

    @NonNull
    protected final AtomicBoolean active = new AtomicBoolean(true);

    protected AbstractStateMachineCallable(
            @NonNull UUID uuid,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable INPUT input,
            @Nullable CONTEXT initialContext
    ) {
        this.uuid = Objects.requireNonNull(uuid, "uuid must not be null");
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.input = input;
        this.initialContext = initialContext;
    }

    @NonNull
    @Override
    public UUID uuid() {
        return uuid;
    }

    /**
     * Signals cooperative cancellation to this state machine execution loop.
     */
    public void cancel() {
        this.active.set(false);
    }

    /**
     * Checks whether this state machine execution has been cancelled.
     *
     * @return {@code true} if cancelled; otherwise {@code false}
     */
    public boolean isCancelled() {
        return !this.active.get();
    }

    @Override
    @Nullable
    public OUTPUT call() throws Exception {
        STATE_KEY currentStateKey = configuration.getStateMap().getInitialStateKey();
        State<CONTEXT, STATE_KEY> currentState = configuration.getStateMap().getInitialState();

        int transitionCount = 0;
        int maxTransitions = configuration.getMaxTransitions();
        Map<STATE_KEY, Integer> stateVisits = new EnumMap<>(currentStateKey.getDeclaringClass());

        CONTEXT context = (initialContext != null)
                ? initialContext
                : configuration.stateMachineContextFactoryTrigger(this).newInstance();

        try {
            InputFunction<CONTEXT, INPUT> inputFunction =
                    configuration.inputFunctionTrigger(this).orElse(null);
            if (inputFunction != null) {
                context = inputFunction.apply(context, input);
            }

            onExecutionStarted(currentStateKey, context);

            stateMachineLoop:
            while (active.get()) {
                if (currentState == null || currentState.isTerminal()) {
                    break stateMachineLoop;
                }

                // 1. Per-State Visit / Retry Limiter Safeguard
                int visits = stateVisits.merge(currentStateKey, 1, Integer::sum);
                if (currentState.maxVisits() > 0 && visits > currentState.maxVisits()) {
                    if (currentState.maxVisitsFallback() != null) {
                        log.warn("State '{}' reached maximum visits limit ({}). Diverting to fallback state '{}'.",
                                currentStateKey, currentState.maxVisits(), currentState.maxVisitsFallback());
                        currentStateKey = currentState.maxVisitsFallback();
                        currentState = configuration.getStateMap().getState(currentStateKey);
                        continue stateMachineLoop;
                    } else {
                        throw new MaxStateVisitsExceededException(currentStateKey, currentState.maxVisits());
                    }
                }

                // 2. Global Transition Circuit Breaker Safeguard
                if (++transitionCount > maxTransitions) {
                    throw new MaxTransitionsExceededException(currentStateKey, maxTransitions, transitionCount);
                }

                onBeforeStateExecution(currentStateKey, currentState, context);

                // 3. Template Step: State Execution
                context = executeState(currentStateKey, currentState, context);

                onAfterStateExecution(currentStateKey, currentState, context);

                // 4. Transition Resolution
                STATE_KEY nextStateKey;
                try {
                    nextStateKey = currentState.transition().nextState(context);
                } catch (Exception e) {
                    throw new TransitionException(e);
                }

                if (nextStateKey == null) {
                    break stateMachineLoop;
                }

                // 5. Strict runtime adjacency guard: verify target is permitted
                if (!currentState.permittedTargets().contains(nextStateKey)) {
                    throw new TransitionException(String.format(
                            "Illegal state transition: State '%s' attempted to transition to '%s', which is not in its permitted targets %s",
                            currentStateKey, nextStateKey, currentState.permittedTargets()
                    ));
                }

                currentStateKey = nextStateKey;
                currentState = configuration.getStateMap().getState(nextStateKey);
            }

            onExecutionCompleted(context);

            OutputFunction<CONTEXT, OUTPUT> outputFunction =
                    configuration.outputFunctionTrigger(this).orElse(null);
            return (outputFunction != null) ? outputFunction.apply(context) : null;

        } catch (Throwable failure) {
            return handleExecutionFailure(currentStateKey, context, failure);
        } finally {
            configuration.stateMachineFinishTrigger(this);
            cleanup();
        }
    }

    /**
     * Executes the business action or child machine for the active state node.
     *
     * @param stateKey The active state identifier
     * @param state    The active state definition
     * @param context  The current context
     * @return The updated context
     * @throws Exception If an error occurs during execution
     */
    @NonNull
    protected abstract CONTEXT executeState(
            @NonNull STATE_KEY stateKey,
            @NonNull State<CONTEXT, STATE_KEY> state,
            @NonNull CONTEXT context
    ) throws Exception;

    /**
     * Hook invoked when the state machine begins execution.
     *
     * @param initialStateKey The initial starting state key
     * @param context         The initial context
     */
    protected void onExecutionStarted(@NonNull STATE_KEY initialStateKey, @NonNull CONTEXT context) {}

    /**
     * Hook invoked immediately before a state's action is executed.
     *
     * @param stateKey The state key being entered
     * @param state    The state definition
     * @param context  The current context
     */
    protected void onBeforeStateExecution(@NonNull STATE_KEY stateKey, @NonNull State<CONTEXT, STATE_KEY> state, @NonNull CONTEXT context) {}

    /**
     * Hook invoked immediately after a state's action completes successfully.
     *
     * @param stateKey The state key that completed
     * @param state    The state definition
     * @param context  The updated context
     */
    protected void onAfterStateExecution(@NonNull STATE_KEY stateKey, @NonNull State<CONTEXT, STATE_KEY> state, @NonNull CONTEXT context) {}

    /**
     * Hook invoked when the state machine finishes traversing all states successfully.
     *
     * @param context The final context
     */
    protected void onExecutionCompleted(@NonNull CONTEXT context) {}

    /**
     * Hook handling state machine failure, supporting rollbacks, checkpoints, and exception transformation.
     *
     * @param currentStateKey The state key where failure occurred
     * @param context         The context at the point of failure
     * @param failure         The error thrown
     * @return An output value if recovery is possible, or throws the failure
     * @throws Exception The failure to propagate
     */
    @Nullable
    protected abstract OUTPUT handleExecutionFailure(
            @NonNull STATE_KEY currentStateKey,
            @NonNull CONTEXT context,
            @NonNull Throwable failure
    ) throws Exception;

    /**
     * Cleanup hook invoked in the finally block.
     */
    protected void cleanup() {}
}
