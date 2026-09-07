package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.AbstractStateMachineCallable;
import com.github.f442y.dispersion.config.InputFunction;
import com.github.f442y.dispersion.config.OutputFunction;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.context.StateMachineContextFactory;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.messaging.SignalMessage;
import com.github.f442y.dispersion.orchestration.messaging.SignalPublisher;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import com.github.f442y.dispersion.state.StateMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Execution driver for turn-based durable Orchestrations on Java 25 Virtual Threads,
 * coordinating signal wait suspension, child machine execution with retries/recovery,
 * parallel fork-join branches, broker publishing, and automated LIFO Saga compensations.
 */
public final class OrchestrationStepDriver {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationStepDriver.class);

    private OrchestrationStepDriver() {}

    /**
     * Executes the initial turn of an orchestration starting from the initial state.
     */
    @NonNull
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> executeTurn(
            @NonNull UUID machineId,
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> config,
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {

        CONTEXT context = initialContext;
        if (context == null) {
            StateMachineContextFactory<CONTEXT> factory = config.stateMachineContextFactory();
            if (factory == null) {
                throw new IllegalStateException("StateMachineContextFactory must be configured or initialContext provided");
            }
            context = factory.newInstance();
        }

        InputFunction<CONTEXT, INPUT> inputFn = config.inputFunction();
        if (inputFn != null) {
            context = inputFn.apply(context, input);
        }

        STATE_KEY initialState = config.getStateMap().getInitialState();

        return runTurn(
                machineId,
                config,
                initialState,
                context,
                new ArrayList<>(),
                new HashSet<>(),
                null,
                virtualThreadExecutor
        );
    }

    /**
     * Resumes an existing suspended orchestration from a checkpoint with an incoming signal or command.
     */
    @NonNull
    public static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> resumeTurn(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> config,
            @NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint,
            @Nullable Object signalPayload,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {

        Set<UUID> processedIds = new HashSet<>(checkpoint.processedCommandIds());
        Object effectiveSignal = signalPayload;

        // Idempotent Command Envelope Deduplication
        if (signalPayload instanceof CommandEnvelope<?> env) {
            if (processedIds.contains(env.commandId())) {
                log.info("[{}] Ignoring duplicate command envelope [{}]", checkpoint.machineId(), env.commandId());
                OutputFunction<CONTEXT, OUTPUT> outputFn = config.outputFunction();
                OUTPUT out = (outputFn != null) ? outputFn.apply(checkpoint.contextSnapshot()) : null;
                return new OrchestrationTurnResult<>(
                        checkpoint.machineId(),
                        checkpoint.status(),
                        checkpoint.currentStateKey(),
                        checkpoint.expectedSignal(),
                        checkpoint.contextSnapshot(),
                        out,
                        null
                );
            }
            processedIds.add(env.commandId());
            effectiveSignal = env.command();
        }

        return runTurn(
                checkpoint.machineId(),
                config,
                checkpoint.currentStateKey(),
                checkpoint.contextSnapshot(),
                new ArrayList<>(checkpoint.completedStates()),
                processedIds,
                effectiveSignal,
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
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> config,
            @Nullable STATE_KEY startingState,
            @NonNull CONTEXT initialContext,
            @NonNull List<STATE_KEY> completedStates,
            @NonNull Set<UUID> processedCommandIds,
            @Nullable Object initialSignalPayload,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {

        StateMap<CONTEXT, STATE_KEY> stateMap = config.getStateMap();
        CONTEXT context = initialContext;
        STATE_KEY currentStateKey = startingState;
        Object pendingSignal = initialSignalPayload;
        String correlationKey = (config.getCorrelationKeyExtractor() != null)
                ? config.getCorrelationKeyExtractor().apply(context) : null;

        try {
            while (currentStateKey != null) {
                // 1. Check if terminal
                if (stateMap.getEndStates().contains(currentStateKey)) {
                    log.debug("[{}] Reached terminal state [{}]", machineId, currentStateKey);
                    break;
                }

                State<CONTEXT, STATE_KEY> stateNode = stateMap.getState(currentStateKey);
                OrchestrationState<CONTEXT, STATE_KEY> orchState = (stateNode instanceof OrchestrationState<CONTEXT, STATE_KEY> os) ? os : null;

                // 2. Check Signal Wait
                if (orchState != null && orchState.isSignalWaitState()) {
                    if (pendingSignal != null) {
                        SignalHandler handler = orchState.signalHandler();
                        if (handler != null) {
                            context = (CONTEXT) handler.handleSignal(context, pendingSignal);
                        }
                        pendingSignal = null; // consumed
                    } else {
                        // Suspend execution at this state
                        OrchestrationCheckpoint<CONTEXT, STATE_KEY> cp = new OrchestrationCheckpoint<>(
                                machineId,
                                config.getMachineName(),
                                OrchestrationStatus.SUSPENDED,
                                currentStateKey,
                                completedStates,
                                context,
                                orchState.expectedSignal(),
                                correlationKey,
                                processedCommandIds,
                                null,
                                Instant.now()
                        );
                        persistCheckpoint(config, cp);

                        return new OrchestrationTurnResult<>(
                                machineId,
                                OrchestrationStatus.SUSPENDED,
                                currentStateKey,
                                orchState.expectedSignal(),
                                context,
                                null,
                                null
                        );
                    }
                }

                // 3. Child State Machine Execution with Retry and Recovery
                if (orchState != null && orchState.hasChildStateMachine()) {
                    context = executeChildMachineWithRetry(machineId, orchState, context);
                }
                // 4. Parallel Fork-Join Execution
                else if (orchState != null && orchState.isParallelState()) {
                    ParallelStateExecutor.executeParallel(orchState.parallelBranches(), context, virtualThreadExecutor);
                }
                // 5. Standard Action
                else {
                    context = stateNode.action().execute(context);
                }

                // 6. Outbound Broker Publishing
                if (orchState != null && orchState.hasOutboundPublish()) {
                    SignalPublisher publisher = orchState.signalPublisher();
                    String destination = orchState.publishDestination();
                    Function<CONTEXT, ?> extractor = orchState.publishPayloadExtractor();
                    if (publisher != null && destination != null && extractor != null) {
                        Object pubPayload = extractor.apply(context);
                        CompletableFuture<Void> pubFuture;
                        if (pubPayload instanceof SignalCommand cmd) {
                            pubFuture = publisher.publish(destination, cmd);
                        } else if (pubPayload instanceof CommandEnvelope<?> env) {
                            pubFuture = publisher.publish(destination, env);
                        } else if (pubPayload instanceof SignalMessage msg) {
                            pubFuture = publisher.publish(msg);
                        } else if (pubPayload != null) {
                            SignalMessage msg = new SignalMessage(
                                    destination,
                                    pubPayload.getClass().getSimpleName(),
                                    correlationKey,
                                    pubPayload
                            );
                            pubFuture = publisher.publish(msg);
                        } else {
                            pubFuture = CompletableFuture.completedFuture(null);
                        }
                        pubFuture.join();
                    }
                }

                // Record completed state for Saga compensation
                completedStates.add(currentStateKey);

                // Update correlation key if changed
                if (config.getCorrelationKeyExtractor() != null) {
                    correlationKey = config.getCorrelationKeyExtractor().apply(context);
                }

                // 7. Transition
                currentStateKey = stateNode.transition().nextState(context);
            }

            // Execution Completed
            OutputFunction<CONTEXT, OUTPUT> outputFn = config.outputFunction();
            OUTPUT output = (outputFn != null) ? outputFn.apply(context) : null;

            OrchestrationCheckpoint<CONTEXT, STATE_KEY> completedCheckpoint = new OrchestrationCheckpoint<>(
                    machineId,
                    config.getMachineName(),
                    OrchestrationStatus.COMPLETED,
                    currentStateKey,
                    completedStates,
                    context,
                    null,
                    correlationKey,
                    processedCommandIds,
                    null,
                    Instant.now()
            );
            persistCheckpoint(config, completedCheckpoint);

            return new OrchestrationTurnResult<>(
                    machineId,
                    OrchestrationStatus.COMPLETED,
                    currentStateKey,
                    null,
                    context,
                    output,
                    null
            );

        } catch (Throwable t) {
            log.error("[{}] Failure in state [{}]; initiating automated LIFO Saga compensation rollback: {}",
                    machineId, currentStateKey, t.getMessage(), t);

            // Execute Saga Compensation Rollback in Reverse Chronological Order (LIFO)
            for (int i = completedStates.size() - 1; i >= 0; i--) {
                STATE_KEY compStateKey = completedStates.get(i);
                try {
                    State<CONTEXT, STATE_KEY> s = stateMap.getState(compStateKey);
                    if (s instanceof OrchestrationState<CONTEXT, STATE_KEY> os) {
                        CompensationAction<CONTEXT> compAction = os.compensationAction();
                        if (compAction != null) {
                            log.debug("[{}] Compensating completed state [{}]", machineId, compStateKey);
                            context = compAction.compensate(context);
                        }
                    }
                } catch (Throwable compErr) {
                    log.error("[{}] Compensation error in state [{}]: {}", machineId, compStateKey, compErr.getMessage(), compErr);
                }
            }

            OrchestrationCheckpoint<CONTEXT, STATE_KEY> compensatedCheckpoint = new OrchestrationCheckpoint<>(
                    machineId,
                    config.getMachineName(),
                    OrchestrationStatus.COMPENSATED,
                    currentStateKey,
                    completedStates,
                    context,
                    null,
                    correlationKey,
                    processedCommandIds,
                    t,
                    Instant.now()
            );
            persistCheckpoint(config, compensatedCheckpoint);

            if (t instanceof Exception e) {
                throw e;
            }
            throw new RuntimeException(t);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey>
    CONTEXT executeChildMachineWithRetry(
            @NonNull UUID parentMachineId,
            @NonNull OrchestrationState<CONTEXT, STATE_KEY> orchState,
            @NonNull CONTEXT parentContext
    ) throws Exception {

        StateMachineConfiguration childConfig = Objects.requireNonNull(
                orchState.childStateMachine(),
                "childStateMachine must not be null"
        );
        RetryPolicy retryPolicy = orchState.retryPolicy();
        ContextRecoverer recoverer = orchState.contextRecoverer();

        int maxAttempts = retryPolicy.maxAttempts();
        Throwable lastError = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                if (attempt > 1) {
                    Duration delay = retryPolicy.computeDelay(attempt);
                    if (!delay.isZero()) {
                        Thread.sleep(delay);
                    }
                }

                Object childInput;
                Function<CONTEXT, ?> inputExtractor = orchState.childInputExtractor();
                if (attempt > 1 && recoverer != null) {
                    childInput = recoverer.recover(parentContext, lastError, attempt);
                } else if (inputExtractor != null) {
                    childInput = inputExtractor.apply(parentContext);
                } else {
                    childInput = null;
                }

                // If child is an OrchestrationStateMachineConfiguration, run via OrchestrationStepDriver
                Object childResult;
                if (childConfig instanceof OrchestrationStateMachineConfiguration orchChildConfig) {
                    UUID childMachineId = UUID.randomUUID();
                    OrchestrationTurnResult<?, ?, ?> turn = executeChildOrchestrationTurn(childMachineId, orchChildConfig, childInput);
                    if (turn.isFailed() || turn.isCompensated()) {
                        throw (turn.error() != null) ? new RuntimeException(turn.error()) : new IllegalStateException("Child orchestration failed");
                    }
                    if (turn.isSuspended()) {
                        throw new IllegalStateException("Child orchestration [" + childMachineId + "] unexpectedly suspended at state [" + turn.currentStateKey() + "]");
                    }
                    childResult = turn.output();
                } else {
                    childResult = AbstractStateMachineCallable.executeDirect(null, childConfig, null, childInput);
                }

                BiFunction<CONTEXT, Object, CONTEXT> outputMerger = orchState.childOutputMerger();
                if (outputMerger != null) {
                    return outputMerger.apply(parentContext, childResult);
                }
                return parentContext;

            } catch (Throwable t) {
                lastError = t;
                log.warn("[{}] Child machine attempt {}/{} failed: {}", parentMachineId, attempt, maxAttempts, t.getMessage());
                if (attempt >= maxAttempts) {
                    if (t instanceof Exception ex) throw ex;
                    throw new RuntimeException(t);
                }
            }
        }

        throw new RuntimeException(lastError);
    }

    @SuppressWarnings("unchecked")
    private static <CC extends StateMachineContext, CS extends Enum<CS> & StateKey, CI, CO>
    OrchestrationTurnResult<CC, CS, CO> executeChildOrchestrationTurn(
            @NonNull UUID childMachineId,
            @NonNull OrchestrationStateMachineConfiguration<CC, CS, CI, CO> childConfig,
            @Nullable Object childInput
    ) throws Exception {
        try (ExecutorService childExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("child-orch-", 0).factory()
        )) {
            return executeTurn(childMachineId, childConfig, null, (CI) childInput, childExecutor);
        }
    }

    private static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    void persistCheckpoint(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> config,
            @NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint
    ) {
        if (config.getCheckpointStore() != null) {
            config.getCheckpointStore().save(checkpoint);
        }
        if (config.getCheckpointListener() != null) {
            try {
                config.getCheckpointListener().accept(checkpoint);
            } catch (Throwable t) {
                log.error("Error invoking checkpoint listener", t);
            }
        }
    }
}
