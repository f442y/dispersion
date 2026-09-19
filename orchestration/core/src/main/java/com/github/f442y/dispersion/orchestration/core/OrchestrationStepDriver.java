package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.child.ChildMachineCompletedEvent;
import com.github.f442y.dispersion.event.child.ChildMachineSpawnedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepCompletedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepFailedEvent;
import com.github.f442y.dispersion.event.compensation.CompensationStepStartedEvent;
import com.github.f442y.dispersion.event.retry.RetryAttemptedEvent;
import com.github.f442y.dispersion.event.retry.RetryExhaustedEvent;
import com.github.f442y.dispersion.event.signal.SignalAwaitedEvent;
import com.github.f442y.dispersion.event.signal.SignalDeliveredEvent;
import com.github.f442y.dispersion.event.state.ActionExecutedEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.state.StateExitedEvent;
import com.github.f442y.dispersion.event.state.TransitionEvaluatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompensatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnFailedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.event.turn.TurnSuspendedEvent;
import com.github.f442y.dispersion.fsm.config.InputFunction;
import com.github.f442y.dispersion.fsm.config.OutputFunction;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.context.StateMachineContextFactory;
import com.github.f442y.dispersion.fsm.state.State;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import com.github.f442y.dispersion.orchestration.CompensationAction;
import com.github.f442y.dispersion.orchestration.CompensationRecord;
import com.github.f442y.dispersion.orchestration.ContextRecoverer;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.OrchestrationConfiguration;
import com.github.f442y.dispersion.orchestration.OrchestrationState;
import com.github.f442y.dispersion.orchestration.OrchestrationStatus;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.RetryPolicy;
import com.github.f442y.dispersion.orchestration.SignalHandler;
import com.github.f442y.dispersion.orchestration.WorkloadDispatcher;
import com.github.f442y.dispersion.orchestration.WorkloadExecutionMode;
import com.github.f442y.dispersion.orchestration.WorkloadInvocation;
import com.github.f442y.dispersion.orchestration.command.CommandDeduplicatedEvent;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.messaging.SignalMessage;
import com.github.f442y.dispersion.orchestration.messaging.SignalPublisher;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
            @NonNull OrchestrationConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> config,
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
            @NonNull OrchestrationConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> config,
            @NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint,
            @Nullable Object signalPayload,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {

        Set<UUID> processedIds = new HashSet<>(checkpoint.processedCommandIds());
        Object effectiveSignal = signalPayload;

        // Idempotent Command Envelope Deduplication
        if (signalPayload instanceof CommandEnvelope<?> env) {
            if (processedIds.contains(env.commandId())) {
                log.atInfo()
                        .addKeyValue("machine_id", checkpoint.machineId())
                        .addKeyValue("command_id", env.commandId())
                        .log("Ignoring duplicate command envelope");
                if (config.eventListener() != null) {
                    safeNotify(config.eventListener(), new CommandDeduplicatedEvent(
                            checkpoint.machineId(),
                            config.getMachineName(),
                            env.commandId(),
                            checkpoint.correlationKey(),
                            Instant.now()
                    ));
                }
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
            @NonNull OrchestrationConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> config,
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

        ExecutionEventListener eventListener = config.eventListener();
        long turnStartNanos = (eventListener != null) ? System.nanoTime() : 0L;

        if (eventListener != null) {
            safeNotify(eventListener, new TurnStartedEvent(
                    machineId,
                    config.getMachineName(),
                    correlationKey,
                    Instant.now()
            ));
        }

        List<CompensationRecord<CONTEXT>> compensationHistory = new ArrayList<>();

        try {
            while (currentStateKey != null) {
                // 1. Check if terminal
                if (stateMap.getEndStates().contains(currentStateKey)) {
                    log.atDebug()
                            .addKeyValue("machine_id", machineId)
                            .addKeyValue("terminal_state", currentStateKey.name())
                            .log("Reached terminal state");
                    break;
                }

                State<CONTEXT, STATE_KEY> stateNode = stateMap.getState(currentStateKey);
                OrchestrationState<CONTEXT, STATE_KEY> orchState = (stateNode instanceof OrchestrationState<CONTEXT, STATE_KEY> os) ? os : null;

                // 2. Check Signal Wait
                if (orchState != null && orchState.isSignalWaitState()) {
                    String expectedSignal = (orchState.expectedSignal() != null) ? orchState.expectedSignal() : "SIGNAL";
                    if (pendingSignal != null) {
                        if (eventListener != null) {
                            safeNotify(eventListener, new SignalDeliveredEvent(
                                    machineId,
                                    config.getMachineName(),
                                    currentStateKey.name(),
                                    expectedSignal,
                                    correlationKey,
                                    Instant.now()
                            ));
                        }
                        SignalHandler handler = orchState.signalHandler();
                        if (handler != null) {
                            context = (CONTEXT) handler.handleSignal(context, pendingSignal);
                        }
                        pendingSignal = null; // consumed
                    } else {
                        // Suspend execution at this state
                        Duration turnDuration = Duration.ofNanos(System.nanoTime() - turnStartNanos);
                        if (eventListener != null) {
                            safeNotify(eventListener, new SignalAwaitedEvent(
                                    machineId,
                                    config.getMachineName(),
                                    currentStateKey.name(),
                                    expectedSignal,
                                    correlationKey,
                                    Instant.now()
                            ));
                            safeNotify(eventListener, new TurnSuspendedEvent(
                                    machineId,
                                    config.getMachineName(),
                                    currentStateKey.name(),
                                    orchState.expectedSignal(),
                                    correlationKey,
                                    turnDuration,
                                    Instant.now()
                            ));
                        }

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

                if (eventListener != null) {
                    safeNotify(eventListener, new StateEnteredEvent(
                            machineId,
                            config.getMachineName(),
                            currentStateKey.name(),
                            Instant.now()
                    ));
                }

                long stateStartNanos = (eventListener != null) ? System.nanoTime() : 0L;

                // 3. Workload Invocation (Unifying child state machines and routed service workloads!)
                if (orchState != null && orchState.hasWorkloadInvocation()) {
                    context = executeWorkloadInvocationWithRetry(machineId, config, orchState, currentStateKey, eventListener, context, virtualThreadExecutor);
                }
                // 4. Parallel Fork-Join Execution with optional context isolation and reduction
                else if (orchState != null && orchState.isParallelState()) {
                    context = ParallelStateExecutor.executeParallel(
                            machineId,
                            config.getMachineName(),
                            currentStateKey.name(),
                            eventListener,
                            orchState.parallelBranches(),
                            context,
                            orchState.parallelContextCloner(),
                            orchState.parallelContextReducer(),
                            virtualThreadExecutor
                    );
                }
                // 5. Standard Action
                else {
                    context = stateNode.action().execute(context);
                }

                if (eventListener != null) {
                    Duration stateDuration = Duration.ofNanos(System.nanoTime() - stateStartNanos);
                    Instant now = Instant.now();
                    safeNotify(eventListener, new ActionExecutedEvent(
                            machineId,
                            config.getMachineName(),
                            currentStateKey.name(),
                            stateDuration,
                            now
                    ));
                    safeNotify(eventListener, new StateExitedEvent(
                            machineId,
                            config.getMachineName(),
                            currentStateKey.name(),
                            stateDuration,
                            now
                    ));
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
                if (orchState != null) {
                    if (orchState.compensationAction() != null) {
                        compensationHistory.add(new CompensationRecord.LocalCompensation<>(
                                currentStateKey.name(),
                                orchState.compensationAction()
                        ));
                    }
                    if (orchState.hasWorkloadInvocation() && orchState.workloadInvocation().hasRoutedCompensation()) {
                        Object compPayload = orchState.workloadInvocation().routedCompensationExtractor().apply(context);
                        compensationHistory.add(new CompensationRecord.RoutedCompensation<>(
                                currentStateKey.name(),
                                orchState.workloadInvocation().serviceName(),
                                orchState.workloadInvocation().workloadSelector(),
                                compPayload
                        ));
                    }
                }

                // Update correlation key if changed
                if (config.getCorrelationKeyExtractor() != null) {
                    correlationKey = config.getCorrelationKeyExtractor().apply(context);
                }

                // 7. Transition
                STATE_KEY nextStateKey = stateNode.transition().nextState(context);
                if (eventListener != null && nextStateKey != null) {
                    safeNotify(eventListener, new TransitionEvaluatedEvent(
                            machineId,
                            config.getMachineName(),
                            currentStateKey.name(),
                            nextStateKey.name(),
                            Instant.now()
                    ));
                }
                currentStateKey = nextStateKey;
            }

            // Execution Completed
            if (eventListener != null) {
                safeNotify(eventListener, new TurnCompletedEvent(
                        machineId,
                        config.getMachineName(),
                        (currentStateKey != null) ? currentStateKey.name() : null,
                        correlationKey,
                        Duration.ofNanos(System.nanoTime() - turnStartNanos),
                        Instant.now()
                ));
            }

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
            log.atError()
                    .setCause(t)
                    .addKeyValue("machine_id", machineId)
                    .addKeyValue("state", (currentStateKey != null) ? currentStateKey.name() : "UNKNOWN")
                    .log("Failure in state; initiating automated LIFO Saga compensation rollback");

            List<String> compStateNames = new ArrayList<>();
            // Execute Saga Compensation Rollback in Reverse Chronological Order (LIFO)
            if (!compensationHistory.isEmpty()) {
                for (int i = compensationHistory.size() - 1; i >= 0; i--) {
                    CompensationRecord<CONTEXT> compRecord = compensationHistory.get(i);
                    boolean isRouted = compRecord instanceof CompensationRecord.RoutedCompensation;
                    long compStartNanos = System.nanoTime();
                    if (eventListener != null) {
                        safeNotify(eventListener, new CompensationStepStartedEvent(
                                machineId,
                                config.getMachineName(),
                                compRecord.stateKey(),
                                isRouted,
                                Instant.now()
                        ));
                    }
                    try {
                        if (compRecord instanceof CompensationRecord.LocalCompensation<CONTEXT> localComp) {
                            log.atDebug()
                                    .addKeyValue("machine_id", machineId)
                                    .addKeyValue("compensating_state", localComp.stateKey())
                                    .log("Compensating local completed state");
                            context = localComp.action().compensate(context);
                            compStateNames.add(localComp.stateKey());
                        } else if (compRecord instanceof CompensationRecord.RoutedCompensation<CONTEXT, ?> routedComp) {
                            log.atInfo()
                                    .addKeyValue("machine_id", machineId)
                                    .addKeyValue("service_name", routedComp.serviceName())
                                    .addKeyValue("compensating_state", routedComp.stateKey())
                                    .log("Dispatching routed Saga compensation to worker service");
                            WorkloadDispatcher dispatcher = config.workloadDispatcher();
                            if (dispatcher != null) {
                                dispatcher.dispatchSync(
                                        routedComp.serviceName(),
                                        routedComp.payload(),
                                        routedComp.workloadSelector(),
                                        Duration.ofSeconds(30),
                                        Map.of("parentMachineId", machineId.toString(), "action", "compensate"),
                                        Object.class
                                );
                            }
                            compStateNames.add(routedComp.stateKey());
                        }
                        if (eventListener != null) {
                            safeNotify(eventListener, new CompensationStepCompletedEvent(
                                    machineId,
                                    config.getMachineName(),
                                    compRecord.stateKey(),
                                    Duration.ofNanos(System.nanoTime() - compStartNanos),
                                    Instant.now()
                            ));
                        }
                    } catch (Throwable compErr) {
                        log.atError()
                                .setCause(compErr)
                                .addKeyValue("machine_id", machineId)
                                .addKeyValue("state", compRecord.stateKey())
                                .log("Compensation error in state");
                        if (eventListener != null) {
                            safeNotify(eventListener, new CompensationStepFailedEvent(
                                    machineId,
                                    config.getMachineName(),
                                    compRecord.stateKey(),
                                    compErr,
                                    Instant.now()
                            ));
                        }
                    }
                }
            } else {
                for (int i = completedStates.size() - 1; i >= 0; i--) {
                    STATE_KEY compStateKey = completedStates.get(i);
                    State<CONTEXT, STATE_KEY> s = stateMap.getState(compStateKey);
                    if (s instanceof OrchestrationState<CONTEXT, STATE_KEY> os) {
                        CompensationAction<CONTEXT> compAction = os.compensationAction();
                        if (compAction != null) {
                            long compStartNanos = System.nanoTime();
                            if (eventListener != null) {
                                safeNotify(eventListener, new CompensationStepStartedEvent(
                                        machineId,
                                        config.getMachineName(),
                                        compStateKey.name(),
                                        false,
                                        Instant.now()
                                ));
                            }
                            try {
                                log.atDebug()
                                        .addKeyValue("machine_id", machineId)
                                        .addKeyValue("compensating_state", compStateKey.name())
                                        .log("Compensating completed state");
                                context = compAction.compensate(context);
                                compStateNames.add(compStateKey.name());
                                if (eventListener != null) {
                                    safeNotify(eventListener, new CompensationStepCompletedEvent(
                                            machineId,
                                            config.getMachineName(),
                                            compStateKey.name(),
                                            Duration.ofNanos(System.nanoTime() - compStartNanos),
                                            Instant.now()
                                    ));
                                }
                            } catch (Throwable compErr) {
                                log.atError()
                                        .setCause(compErr)
                                        .addKeyValue("machine_id", machineId)
                                        .addKeyValue("state", compStateKey.name())
                                        .log("Compensation error in state");
                                if (eventListener != null) {
                                    safeNotify(eventListener, new CompensationStepFailedEvent(
                                            machineId,
                                            config.getMachineName(),
                                            compStateKey.name(),
                                            compErr,
                                            Instant.now()
                                    ));
                                }
                            }
                        }
                    }
                }
            }

            boolean hasCompensations = !compStateNames.isEmpty();
            OrchestrationStatus finalStatus = hasCompensations ? OrchestrationStatus.COMPENSATED : OrchestrationStatus.FAILED;

            if (eventListener != null) {
                if (hasCompensations) {
                    safeNotify(eventListener, new TurnCompensatedEvent(
                            machineId,
                            config.getMachineName(),
                            (currentStateKey != null) ? currentStateKey.name() : "UNKNOWN",
                            compStateNames,
                            t,
                            correlationKey,
                            Duration.ofNanos(System.nanoTime() - turnStartNanos),
                            Instant.now()
                    ));
                } else {
                    safeNotify(eventListener, new TurnFailedEvent(
                            machineId,
                            config.getMachineName(),
                            (currentStateKey != null) ? currentStateKey.name() : "UNKNOWN",
                            t,
                            correlationKey,
                            Duration.ofNanos(System.nanoTime() - turnStartNanos),
                            Instant.now()
                    ));
                }
            }

            OrchestrationCheckpoint<CONTEXT, STATE_KEY> terminalErrorCheckpoint = new OrchestrationCheckpoint<>(
                    machineId,
                    config.getMachineName(),
                    finalStatus,
                    currentStateKey,
                    completedStates,
                    context,
                    null,
                    correlationKey,
                    processedCommandIds,
                    t,
                    Instant.now()
            );
            persistCheckpoint(config, terminalErrorCheckpoint);

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
    CONTEXT executeWorkloadInvocationWithRetry(
            @NonNull UUID parentMachineId,
            @NonNull OrchestrationConfiguration<CONTEXT, STATE_KEY, ?, ?> config,
            @NonNull OrchestrationState<CONTEXT, STATE_KEY> orchState,
            @NonNull STATE_KEY currentStateKey,
            @Nullable ExecutionEventListener eventListener,
            @NonNull CONTEXT parentContext,
            @NonNull ExecutorService virtualThreadExecutor
    ) throws Exception {

        WorkloadInvocation<CONTEXT, Object, Object> invocation = (WorkloadInvocation) orchState.workloadInvocation();
        if (invocation == null) {
            return parentContext;
        }

        RetryPolicy retryPolicy = invocation.retryPolicy();
        ContextRecoverer recoverer = invocation.recoverer();

        int maxAttempts = retryPolicy.maxAttempts();
        Throwable lastError = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                if (attempt > 1) {
                    Duration delay = retryPolicy.computeDelay(attempt);
                    if (eventListener != null) {
                        safeNotify(eventListener, new RetryAttemptedEvent(
                                parentMachineId,
                                config.getMachineName(),
                                currentStateKey.name(),
                                attempt,
                                maxAttempts,
                                delay,
                                lastError != null ? lastError : new RuntimeException("Workload retry attempt"),
                                Instant.now()
                        ));
                    }
                    if (!delay.isZero()) {
                        Thread.sleep(delay);
                    }
                }

                Object inputPayload;
                if (attempt > 1 && recoverer != null) {
                    inputPayload = recoverer.recover(parentContext, lastError, attempt);
                } else {
                    inputPayload = invocation.inputExtractor().apply(parentContext);
                }

                Object result;
                if (invocation.hasLocalStateMachine()) {
                    StateMachineConfiguration childConfig = invocation.localStateMachine();
                    if (childConfig instanceof OrchestrationConfiguration orchChildConfig) {
                        UUID childMachineId = UUID.randomUUID();
                        if (eventListener != null) {
                            safeNotify(eventListener, new ChildMachineSpawnedEvent(
                                    parentMachineId,
                                    config.getMachineName(),
                                    childMachineId,
                                    orchChildConfig.getMachineName(),
                                    currentStateKey.name(),
                                    Instant.now()
                            ));
                        }
                        OrchestrationTurnResult<?, ?, ?> turn = executeChildOrchestrationTurn(childMachineId, orchChildConfig, inputPayload);
                        if (turn.isFailed() || turn.isCompensated()) {
                            throw (turn.error() != null) ? new RuntimeException(turn.error()) : new IllegalStateException("Child orchestration failed");
                        }
                        if (turn.isSuspended()) {
                            throw new IllegalStateException("Child orchestration [" + childMachineId + "] unexpectedly suspended at state [" + turn.currentStateKey() + "]");
                        }
                        if (eventListener != null) {
                            safeNotify(eventListener, new ChildMachineCompletedEvent(
                                    parentMachineId,
                                    config.getMachineName(),
                                    childMachineId,
                                    orchChildConfig.getMachineName(),
                                    Instant.now()
                            ));
                        }
                        result = turn.output();
                    } else {
                        result = executeChildStateMachineDirect(childConfig, inputPayload);
                    }
                } else {
                    WorkloadDispatcher dispatcher = (config != null) ? config.workloadDispatcher() : null;
                    if (dispatcher == null) {
                        throw new IllegalStateException("WorkloadDispatcher must be configured on OrchestrationConfiguration to invoke service: " + invocation.serviceName());
                    }

                    Map<String, String> metadata = Map.of("parentMachineId", parentMachineId.toString());

                    if (invocation.executionMode() == WorkloadExecutionMode.SYNCHRONOUS_VIRTUAL_THREAD) {
                        result = dispatcher.dispatchSync(
                                invocation.serviceName(),
                                inputPayload,
                                invocation.workloadSelector(),
                                invocation.timeout(),
                                metadata,
                                invocation.responseType()
                        );
                    } else {
                        result = dispatcher.dispatchAsync(
                                invocation.serviceName(),
                                inputPayload,
                                invocation.workloadSelector(),
                                invocation.timeout(),
                                metadata,
                                invocation.responseType()
                        ).get(invocation.timeout().toMillis(), TimeUnit.MILLISECONDS);
                    }
                }

                BiFunction<CONTEXT, Object, CONTEXT> outputMerger = invocation.outputMerger();
                if (outputMerger != null) {
                    return outputMerger.apply(parentContext, result);
                }
                return parentContext;

            } catch (Throwable t) {
                lastError = t;
                log.atWarn()
                        .setCause(t)
                        .addKeyValue("parent_machine_id", parentMachineId)
                        .addKeyValue("service_name", invocation.serviceName())
                        .addKeyValue("attempt", attempt)
                        .addKeyValue("max_attempts", maxAttempts)
                        .log("Workload invocation attempt failed");
                if (attempt >= maxAttempts) {
                    if (eventListener != null) {
                        safeNotify(eventListener, new RetryExhaustedEvent(
                                parentMachineId,
                                config.getMachineName(),
                                currentStateKey.name(),
                                maxAttempts,
                                t,
                                Instant.now()
                        ));
                    }
                    if (t instanceof Exception ex) throw ex;
                    throw new RuntimeException(t);
                }
            }
        }

        throw new RuntimeException(lastError);
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

                // If child is an OrchestrationConfiguration, run via OrchestrationStepDriver
                Object childResult;
                if (childConfig instanceof OrchestrationConfiguration orchChildConfig) {
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
                    childResult = executeChildStateMachineDirect(childConfig, childInput);
                }

                BiFunction<CONTEXT, Object, CONTEXT> outputMerger = orchState.childOutputMerger();
                if (outputMerger != null) {
                    return outputMerger.apply(parentContext, childResult);
                }
                return parentContext;

            } catch (Throwable t) {
                lastError = t;
                log.atWarn()
                        .setCause(t)
                        .addKeyValue("parent_machine_id", parentMachineId)
                        .addKeyValue("attempt", attempt)
                        .addKeyValue("max_attempts", maxAttempts)
                        .log("Child machine attempt failed");
                if (attempt >= maxAttempts) {
                    if (t instanceof Exception ex) throw ex;
                    throw new RuntimeException(t);
                }
            }
        }

        throw new RuntimeException(lastError);
    }

    @SuppressWarnings("unchecked")
    private static <
            CHILD_CONTEXT extends StateMachineContext,
            CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
            CHILD_INPUT,
            CHILD_OUTPUT>
    OrchestrationTurnResult<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_OUTPUT> executeChildOrchestrationTurn(
            @NonNull UUID childMachineId,
            @NonNull OrchestrationConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childConfig,
            @Nullable Object childInput
    ) throws Exception {
        try (ExecutorService childExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("child-orch-", 0).factory()
        )) {
            return executeTurn(childMachineId, childConfig, null, (CHILD_INPUT) childInput, childExecutor);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <
            CHILD_CONTEXT extends StateMachineContext,
            CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
            CHILD_INPUT,
            CHILD_OUTPUT>
    CHILD_OUTPUT executeChildStateMachineDirect(
            @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, CHILD_INPUT, CHILD_OUTPUT> childConfig,
            @Nullable Object childInput
    ) throws Exception {
        StateMap<CHILD_CONTEXT, CHILD_STATE_KEY> stateMap = childConfig.getStateMap();
        CHILD_CONTEXT context = null;
        StateMachineContextFactory<CHILD_CONTEXT> factory = childConfig.stateMachineContextFactory();
        if (factory != null) {
            context = factory.newInstance();
        }
        if (context == null) {
            throw new IllegalStateException("StateMachine execution failed: Context is null and no contextFactory is configured");
        }

        InputFunction<CHILD_CONTEXT, CHILD_INPUT> inputFn = childConfig.inputFunction();
        if (inputFn != null) {
            context = inputFn.apply(context, (CHILD_INPUT) childInput);
        }

        ExecutionEventListener eventListener = childConfig.eventListener();
        UUID childExecId = (eventListener != null) ? UUID.randomUUID() : null;
        long turnStartNanos = (eventListener != null) ? System.nanoTime() : 0L;

        if (eventListener != null) {
            safeNotify(eventListener, new TurnStartedEvent(
                    childExecId,
                    childConfig.getMachineName(),
                    null,
                    Instant.now()
            ));
        }

        CHILD_STATE_KEY currentStateKey = stateMap.getInitialState();
        int totalTransitions = 0;
        int maxTransitions = childConfig.getMaxTransitions();

        try {
            while (currentStateKey != null) {
                int ordinal = currentStateKey.ordinal();
                if (maxTransitions > 0 && totalTransitions >= maxTransitions) {
                    throw new IllegalStateException("Max transitions exceeded: " + maxTransitions);
                }
                if (stateMap.isEndStateFast(ordinal)) {
                    break;
                }
                State<CHILD_CONTEXT, CHILD_STATE_KEY> state = stateMap.getStateFast(ordinal);

                if (eventListener != null) {
                    safeNotify(eventListener, new StateEnteredEvent(
                            childExecId,
                            childConfig.getMachineName(),
                            currentStateKey.name(),
                            Instant.now()
                    ));
                }
                long stateStartNanos = (eventListener != null) ? System.nanoTime() : 0L;

                context = state.action().execute(context);

                if (eventListener != null) {
                    safeNotify(eventListener, new StateExitedEvent(
                            childExecId,
                            childConfig.getMachineName(),
                            currentStateKey.name(),
                            Duration.ofNanos(System.nanoTime() - stateStartNanos),
                            Instant.now()
                    ));
                }

                CHILD_STATE_KEY nextStateKey = state.transition().nextState(context);

                if (eventListener != null && nextStateKey != null) {
                    safeNotify(eventListener, new TransitionEvaluatedEvent(
                            childExecId,
                            childConfig.getMachineName(),
                            currentStateKey.name(),
                            nextStateKey.name(),
                            Instant.now()
                    ));
                }

                totalTransitions++;
                currentStateKey = nextStateKey;
            }

            if (eventListener != null) {
                safeNotify(eventListener, new TurnCompletedEvent(
                        childExecId,
                        childConfig.getMachineName(),
                        (currentStateKey != null) ? currentStateKey.name() : null,
                        null,
                        Duration.ofNanos(System.nanoTime() - turnStartNanos),
                        Instant.now()
                ));
            }

            Consumer<CHILD_CONTEXT> finishTrigger = childConfig.stateMachineFinishTrigger();
            if (finishTrigger != null) {
                finishTrigger.accept(context);
            }

            OutputFunction<CHILD_CONTEXT, CHILD_OUTPUT> outputFn = childConfig.outputFunction();
            return (outputFn != null) ? outputFn.apply(context) : null;
        } catch (Throwable t) {
            if (eventListener != null) {
                safeNotify(eventListener, new TurnFailedEvent(
                        childExecId,
                        childConfig.getMachineName(),
                        (currentStateKey != null) ? currentStateKey.name() : "UNKNOWN",
                        t,
                        null,
                        Duration.ofNanos(System.nanoTime() - turnStartNanos),
                        Instant.now()
                ));
            }
            if (t instanceof Exception e) throw e;
            throw new RuntimeException(t);
        }
    }

    private static <
            CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            INPUT,
            OUTPUT>
    void persistCheckpoint(
            @NonNull OrchestrationConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> config,
            @NonNull OrchestrationCheckpoint<CONTEXT, STATE_KEY> checkpoint
    ) {
        if (config.getCheckpointStore() != null) {
            config.getCheckpointStore().save(checkpoint);
        }
        if (config.getCheckpointListener() != null) {
            try {
                config.getCheckpointListener().accept(checkpoint);
            } catch (Throwable t) {
                log.atError()
                        .setCause(t)
                        .log("Error invoking checkpoint listener");
            }
        }
    }

    private static void safeNotify(@NonNull ExecutionEventListener listener, @NonNull ExecutionEvent event) {
        try {
            listener.onEvent(event);
        } catch (Throwable t) {
            log.atError()
                    .setCause(t)
                    .addKeyValue("listener_class", listener.getClass().getName())
                    .log("ExecutionEventListener threw an exception");
        }
    }
}
