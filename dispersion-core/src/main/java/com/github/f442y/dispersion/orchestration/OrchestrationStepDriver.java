package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.StateMachineCallable;
import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.ActionException;
import com.github.f442y.dispersion.exception.MaxStateVisitsExceededException;
import com.github.f442y.dispersion.exception.MaxTransitionsExceededException;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.exception.TransitionException;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Turn-based execution driver for Orchestration State Machines.
 * Executes steps on virtual threads until reaching a terminal state or an external signal suspension point.
 * Supports de-hydration, checkpoint persistence, idempotent CommandEnvelope deduplication,
 * rehydration upon signal delivery, and automated LIFO Saga rollbacks.
 */
public final class OrchestrationStepDriver {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationStepDriver.class);

    private OrchestrationStepDriver() {}

    /**
     * Executes a new orchestration workflow from the beginning.
     */
    @NonNull
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> executeTurn(
            @NonNull UUID machineId,
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable INPUT input,
            @Nullable CONTEXT initialContext,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {
        STATE_KEY initialStateKey = configuration.getStateMap().getInitialStateKey();
        CONTEXT context = (initialContext != null)
                ? initialContext
                : configuration.stateMachineContextFactoryTrigger(null).newInstance();

        InputFunction<CONTEXT, INPUT> inputFunction =
                configuration.inputFunctionTrigger(null).orElse(null);
        if (inputFunction != null) {
            context = inputFunction.apply(context, input);
        }

        return runTurn(
                machineId,
                configuration,
                initialStateKey,
                context,
                new ArrayList<>(),
                new LinkedHashMap<>(),
                new HashSet<>(),
                new EnumMap<>(initialStateKey.getDeclaringClass()),
                0,
                null,
                virtualThreadExecutor
        );
    }

    /**
     * Resumes an existing suspended orchestration workflow with an incoming signal payload or command envelope.
     */
    @NonNull
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> resumeTurn(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint,
            @Nullable Object signalPayload,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {
        STATE_KEY resumeStateKey = checkpoint.currentStateKey();
        if (resumeStateKey == null) {
            throw new IllegalStateException("Cannot resume orchestration " + checkpoint.machineId() + ": currentStateKey is null");
        }

        Set<UUID> processedCommandIds = new HashSet<>(checkpoint.processedCommandIds());
        Object effectivePayload = signalPayload;

        // Idempotency check on CommandEnvelope
        if (signalPayload instanceof CommandEnvelope<?> envelope) {
            if (processedCommandIds.contains(envelope.commandId())) {
                log.info("Ignoring duplicate command [{}] for machine [{}] (already processed)",
                        envelope.commandId(), checkpoint.machineId());
                OutputFunction<CONTEXT, OUTPUT> outputFunction = configuration.outputFunctionTrigger(null).orElse(null);
                OUTPUT output = (outputFunction != null) ? outputFunction.apply(checkpoint.contextSnapshot()) : null;
                return new OrchestrationTurnResult<>(checkpoint.machineId(), checkpoint.status(), checkpoint.currentStateKey(), checkpoint.expectedSignal(), checkpoint.contextSnapshot(), output, null);
            }
            processedCommandIds.add(envelope.commandId());
            effectivePayload = envelope.command();
        }

        List<STATE_KEY> completedStates = new ArrayList<>(checkpoint.completedStates());
        Map<STATE_KEY, OrchestrationState<CONTEXT, STATE_KEY>> completedStateMap = new LinkedHashMap<>();
        for (STATE_KEY completedKey : completedStates) {
            State<CONTEXT, STATE_KEY> st = configuration.getStateMap().getState(completedKey);
            if (st instanceof OrchestrationState<CONTEXT, STATE_KEY> orchSt) {
                completedStateMap.put(completedKey, orchSt);
            }
        }

        return runTurn(
                checkpoint.machineId(),
                configuration,
                resumeStateKey,
                checkpoint.contextSnapshot(),
                completedStates,
                completedStateMap,
                processedCommandIds,
                new EnumMap<>(resumeStateKey.getDeclaringClass()),
                0,
                effectivePayload,
                virtualThreadExecutor
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> runTurn(
            @NonNull UUID machineId,
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull STATE_KEY startingStateKey,
            @NonNull CONTEXT initialContext,
            @NonNull List<STATE_KEY> completedStates,
            @NonNull Map<STATE_KEY, OrchestrationState<CONTEXT, STATE_KEY>> completedStateMap,
            @NonNull Set<UUID> processedCommandIds,
            @NonNull Map<STATE_KEY, Integer> stateVisits,
            int startingTransitionCount,
            @Nullable Object initialSignalPayload,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {

        STATE_KEY currentStateKey = startingStateKey;
        State<CONTEXT, STATE_KEY> currentState = configuration.getStateMap().getState(currentStateKey);
        CONTEXT context = initialContext;
        int transitionCount = startingTransitionCount;
        int maxTransitions = configuration.getMaxTransitions();
        Object currentSignalPayload = initialSignalPayload;

        Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> listener = configuration.checkpointListener();
        CheckpointStore<CONTEXT, STATE_KEY> store = configuration.checkpointStore();
        String machineName = configuration.machineName();

        notifyCheckpoint(listener, store, machineId, machineName, OrchestrationStatus.RUNNING, currentStateKey, completedStates, context, null, null, processedCommandIds, configuration);

        try {
            turnLoop:
            while (true) {
                if (currentState == null || currentState.isTerminal()) {
                    break turnLoop;
                }

                // 1. Visit limiter safeguard
                int visits = stateVisits.merge(currentStateKey, 1, Integer::sum);
                if (currentState.maxVisits() > 0 && visits > currentState.maxVisits()) {
                    if (currentState.maxVisitsFallback() != null) {
                        log.warn("Orchestration [{}] State '{}' reached maximum visits limit ({}). Diverting to fallback '{}'.",
                                machineName, currentStateKey, currentState.maxVisits(), currentState.maxVisitsFallback());
                        currentStateKey = currentState.maxVisitsFallback();
                        currentState = configuration.getStateMap().getState(currentStateKey);
                        continue turnLoop;
                    } else {
                        throw new MaxStateVisitsExceededException(currentStateKey, currentState.maxVisits());
                    }
                }

                // 2. Global transition circuit breaker
                if (++transitionCount > maxTransitions) {
                    throw new MaxTransitionsExceededException(currentStateKey, maxTransitions, transitionCount);
                }

                // 3. Signal wait state evaluation
                if (currentState instanceof OrchestrationState<CONTEXT, STATE_KEY> orchState && orchState.isSignalWait()) {
                    if (currentSignalPayload != null) {
                        // Rehydrated with signal! Process the payload:
                        SignalHandler signalHandler = orchState.signalHandler();
                        if (signalHandler != null) {
                            context = (CONTEXT) signalHandler.handleSignal(context, currentSignalPayload);
                        }
                        currentSignalPayload = null; // Signal consumed
                        completedStates.add(currentStateKey);
                        completedStateMap.put(currentStateKey, orchState);
                    } else {
                        // Entering signal wait state without a signal: SUSPEND THIS TURN
                        String expectedSignal = orchState.expectedSignal();
                        String correlationKey = extractCorrelationKey(configuration, context);

                        log.info("Orchestration [{}] suspending at state [{}] waiting for signal [{}]",
                                machineName, currentStateKey, expectedSignal);

                        notifyCheckpoint(listener, store, machineId, machineName, OrchestrationStatus.SUSPENDED, currentStateKey, completedStates, context, expectedSignal, correlationKey, processedCommandIds, configuration);

                        return OrchestrationTurnResult.suspended(machineId, currentStateKey, Objects.requireNonNull(expectedSignal), context);
                    }
                } else if (currentState instanceof OrchestrationState<CONTEXT, STATE_KEY> orchState) {
                    // 4. Orchestration Step Execution: Parallel, Child Machine, or Action
                    if (orchState.hasParallelBranches()) {
                        context = ParallelStateExecutor.executeParallelBranches(orchState.parallelBranches(), context, virtualThreadExecutor);
                    } else if (orchState.hasChildStateMachine()) {
                        context = executeChildStateMachineWithRecovery(orchState, currentStateKey, context, machineName, virtualThreadExecutor);
                    } else {
                        try {
                            context = currentState.action().execute(context);
                        } catch (Exception e) {
                            throw new ActionException(e);
                        }
                    }
                    completedStates.add(currentStateKey);
                    completedStateMap.put(currentStateKey, orchState);
                } else {
                    // Standard action state
                    try {
                        context = currentState.action().execute(context);
                    } catch (Exception e) {
                        throw new ActionException(e);
                    }
                    completedStates.add(currentStateKey);
                }

                // 5. Transition Resolution
                STATE_KEY nextStateKey;
                try {
                    nextStateKey = currentState.transition().nextState(context);
                } catch (Exception e) {
                    throw new TransitionException(e);
                }

                if (nextStateKey == null) {
                    break turnLoop;
                }

                // 6. Strict Runtime Adjacency Guard
                if (!currentState.permittedTargets().contains(nextStateKey)) {
                    throw new TransitionException(String.format(
                            "Illegal state transition: State '%s' attempted to transition to '%s', which is not in its permitted targets %s",
                            currentStateKey, nextStateKey, currentState.permittedTargets()
                    ));
                }

                currentStateKey = nextStateKey;
                currentState = configuration.getStateMap().getState(nextStateKey);
            }

            // Workflow Completed!
            notifyCheckpoint(listener, store, machineId, machineName, OrchestrationStatus.COMPLETED, null, completedStates, context, null, null, processedCommandIds, configuration);

            OutputFunction<CONTEXT, OUTPUT> outputFunction = configuration.outputFunctionTrigger(null).orElse(null);
            OUTPUT output = (outputFunction != null) ? outputFunction.apply(context) : null;

            return OrchestrationTurnResult.completed(machineId, context, output);

        } catch (Throwable failure) {
            log.error("Orchestration [{}] failed in state [{}]. Initiating Saga compensation rollback...",
                    machineName, currentStateKey, failure);

            notifyCheckpoint(listener, store, machineId, machineName, OrchestrationStatus.COMPENSATING, currentStateKey, completedStates, context, null, null, processedCommandIds, configuration, failure);
            CONTEXT compensatedContext = executeSagaCompensations(completedStates, completedStateMap, context);
            notifyCheckpoint(listener, store, machineId, machineName, OrchestrationStatus.COMPENSATED, currentStateKey, completedStates, compensatedContext, null, null, processedCommandIds, configuration, failure);

            if (failure instanceof StateMachineException sme) {
                configuration.stateMachineExceptionTrigger(null, sme);
                throw sme;
            } else if (failure instanceof Exception ex) {
                throw ex;
            } else {
                throw new RuntimeException("Orchestration failed in state " + currentStateKey, failure);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey>
    CONTEXT executeChildStateMachineWithRecovery(
            @NonNull OrchestrationState<CONTEXT, STATE_KEY> state,
            @NonNull STATE_KEY stateKey,
            @NonNull CONTEXT orchestrationContext,
            @NonNull String machineName,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {

        RetryPolicy retryPolicy = state.retryPolicy();
        int maxAttempts = retryPolicy.maxAttempts();
        StateMachineConfiguration childConfig = state.childStateMachine();
        ContextRecoverer recoverer = state.contextRecoverer();
        BiFunction outputMerger = state.outputMerger();

        Throwable lastFailure = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (attempt > 1) {
                Duration delay = retryPolicy.computeDelay(attempt);
                if (!delay.isZero() && !delay.isNegative()) {
                    Thread.sleep(delay.toMillis());
                }
                log.info("Orchestration [{}] State [{}] Retrying attempt {}/{} on fresh virtual thread...",
                        machineName, stateKey, attempt, maxAttempts);
            }

            try {
                Object childInput = (recoverer != null)
                        ? recoverer.recover(orchestrationContext, lastFailure, attempt)
                        : null;

                Callable<?> callable;
                if (childConfig instanceof OrchestrationStateMachineConfiguration nestedOrchConfig) {
                    callable = () -> executeTurn(UUID.randomUUID(), nestedOrchConfig, childInput, null, virtualThreadExecutor);
                } else {
                    callable = new StateMachineCallable.StateMachineCallableBuilder()
                            .input(childInput)
                            .stateMachine(childConfig);
                }

                Future<?> future = virtualThreadExecutor.submit(callable);
                Object childResult = future.get();

                Object childOutput = (childResult instanceof OrchestrationTurnResult turnResult)
                        ? turnResult.output()
                        : childResult;

                if (outputMerger != null) {
                    return (CONTEXT) outputMerger.apply(orchestrationContext, childOutput);
                }
                return orchestrationContext;

            } catch (Exception ex) {
                lastFailure = (ex instanceof ExecutionException) ? ex.getCause() : ex;
                log.warn("Orchestration [{}] State [{}] Attempt {}/{} failed: {}",
                        machineName, stateKey, attempt, maxAttempts, lastFailure != null ? lastFailure.getMessage() : "Unknown error");
            }
        }

        if (lastFailure instanceof Exception exception) {
            throw exception;
        } else {
            throw new RuntimeException("Child state machine failed in state " + stateKey, lastFailure);
        }
    }

    private static <CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey>
    CONTEXT executeSagaCompensations(
            @NonNull List<STATE_KEY> completedStates,
            @NonNull Map<STATE_KEY, OrchestrationState<CONTEXT, STATE_KEY>> completedStateMap,
            @NonNull CONTEXT context
    ) {
        ListIterator<STATE_KEY> iterator = completedStates.listIterator(completedStates.size());
        while (iterator.hasPrevious()) {
            STATE_KEY key = iterator.previous();
            OrchestrationState<CONTEXT, STATE_KEY> state = completedStateMap.get(key);
            if (state != null) {
                try {
                    log.info("Executing Saga Compensation for state [{}]...", key);
                    context = state.compensationAction().compensate(context);
                } catch (Exception ex) {
                    log.error("Error executing compensation for state [{}]: {}", key, ex.getMessage(), ex);
                }
            }
        }
        return context;
    }

    private static <CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey, I, O>
    String extractCorrelationKey(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, I, O> configuration,
            @NonNull CONTEXT context
    ) {
        if (configuration.correlationKeyExtractor() != null) {
            try {
                return configuration.correlationKeyExtractor().apply(context);
            } catch (Exception e) {
                log.warn("Error extracting correlation key from context: {}", e.getMessage(), e);
            }
        }
        return null;
    }

    private static <CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey, I, O>
    void notifyCheckpoint(
            @Nullable Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> listener,
            @Nullable CheckpointStore<CONTEXT, STATE_KEY> store,
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull OrchestrationStatus status,
            @Nullable STATE_KEY currentKey,
            @NonNull List<STATE_KEY> completed,
            @NonNull CONTEXT context,
            @Nullable String expectedSignal,
            @Nullable String correlationKey,
            @NonNull Set<UUID> processedCommandIds,
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, I, O> configuration
    ) {
        notifyCheckpoint(listener, store, machineId, machineName, status, currentKey, completed, context, expectedSignal, correlationKey, processedCommandIds, configuration, null);
    }

    private static <CONTEXT extends StateMachineContext, STATE_KEY extends Enum<STATE_KEY> & StateKey, I, O>
    void notifyCheckpoint(
            @Nullable Consumer<OrchestrationCheckpoint<CONTEXT, STATE_KEY>> listener,
            @Nullable CheckpointStore<CONTEXT, STATE_KEY> store,
            @NonNull UUID machineId,
            @NonNull String machineName,
            @NonNull OrchestrationStatus status,
            @Nullable STATE_KEY currentKey,
            @NonNull List<STATE_KEY> completed,
            @NonNull CONTEXT context,
            @Nullable String expectedSignal,
            @Nullable String correlationKey,
            @NonNull Set<UUID> processedCommandIds,
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, I, O> configuration,
            @Nullable Throwable error
    ) {
        String effectiveCorrelationKey = (correlationKey != null) ? correlationKey : extractCorrelationKey(configuration, context);
        OrchestrationCheckpoint<CONTEXT, STATE_KEY> cp = new OrchestrationCheckpoint<>(
                machineId,
                machineName,
                status,
                currentKey,
                List.copyOf(completed),
                context,
                expectedSignal,
                effectiveCorrelationKey,
                Set.copyOf(processedCommandIds),
                error,
                Instant.now()
        );

        if (store != null) {
            try {
                store.save(cp);
            } catch (Exception e) {
                log.warn("Error saving checkpoint to store: {}", e.getMessage(), e);
            }
        }

        if (listener != null) {
            try {
                listener.accept(cp);
            } catch (Exception e) {
                log.warn("Error notifying checkpoint listener: {}", e.getMessage(), e);
            }
        }
    }
}
