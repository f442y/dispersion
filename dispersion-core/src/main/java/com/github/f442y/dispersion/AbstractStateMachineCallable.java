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

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Core thread-confined execution loop traversing a Finite State Machine graph on a dedicated Virtual Thread.
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

    private final UUID uuid;
    private final StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;
    private final CONTEXT initialContext;
    private final INPUT input;

    protected AbstractStateMachineCallable(
            @NonNull UUID uuid,
            @NonNull StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) {
        this.uuid = Objects.requireNonNull(uuid, "uuid must not be null");
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.initialContext = initialContext;
        this.input = input;
    }

    @NonNull
    @Override
    public UUID uuid() {
        return uuid;
    }

    @NonNull
    public StateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration() {
        return configuration;
    }

    @Override
    @Nullable
    public OUTPUT call() throws Exception {
        StateMap<CONTEXT, STATE_KEY> stateMap = configuration.getStateMap();
        CONTEXT context = initializeContext();

        Map<STATE_KEY, Integer> visitCounts = new EnumMap<>(stateMap.getStateKeyClass());
        int totalTransitions = 0;
        int maxTransitions = configuration.getMaxTransitions();

        STATE_KEY currentStateKey = stateMap.getInitialState();

        try {
            while (currentStateKey != null) {
                // 1. Check global circuit breaker limit
                if (maxTransitions > 0 && totalTransitions >= maxTransitions) {
                    throw new MaxTransitionsExceededException(maxTransitions);
                }

                // 2. Check terminal state
                if (stateMap.getEndStates().contains(currentStateKey)) {
                    log.debug("[{}] Reached terminal state [{}]", uuid, currentStateKey);
                    break;
                }

                State<CONTEXT, STATE_KEY> currentState = stateMap.getState(currentStateKey);

                // 3. Check per-state visit limit
                int visits = visitCounts.merge(currentStateKey, 1, Integer::sum);
                int maxVisits = currentState.maxVisits();

                if (maxVisits > 0 && visits > maxVisits) {
                    STATE_KEY fallback = currentState.maxVisitsFallback();
                    if (fallback != null) {
                        log.warn("[{}] State [{}] exceeded max visits ({}); diverting to fallback [{}]",
                                uuid, currentStateKey, maxVisits, fallback);
                        currentStateKey = fallback;
                        totalTransitions++;
                        continue;
                    } else {
                        throw new MaxStateVisitsExceededException(currentStateKey.name(), maxVisits);
                    }
                }

                // 4. Execute state business logic
                try {
                    context = executeStateAction(currentState, context);
                } catch (StateMachineException sme) {
                    throw sme;
                } catch (Throwable t) {
                    throw new ActionException(currentStateKey.name(), t);
                }

                // 5. Evaluate transition logic
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

                // 6. Runtime Graph Adjacency Verification Guard
                if (!currentState.permittedTargets().isEmpty() &&
                    !currentState.permittedTargets().contains(nextStateKey) &&
                    !stateMap.getEndStates().contains(nextStateKey)) {
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
                    log.error("[{}] Exception in stateMachineExceptionTrigger callback", uuid, triggerEx);
                }
            }
            if (t instanceof Exception e) {
                throw e;
            }
            throw new RuntimeException(t);
        }
    }

    @NonNull
    protected CONTEXT initializeContext() throws StateMachineException {
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
        return context;
    }

    @NonNull
    protected CONTEXT executeStateAction(
            @NonNull State<CONTEXT, STATE_KEY> state,
            @NonNull CONTEXT context
    ) throws Exception {
        return state.action().execute(context);
    }
}
