package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.AbstractStateMachineCallable;
import com.github.f442y.dispersion.StateMachineCallable;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.exception.ActionException;
import com.github.f442y.dispersion.exception.StateMachineException;
import com.github.f442y.dispersion.state.State;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Encapsulates the runtime execution loop of an Orchestration State Machine.
 * Manages atomic child state machines and nested orchestrations on fresh virtual threads,
 * coordinates concurrent parallel fork-join branches, captures checkpoint snapshots,
 * and coordinates automatic Saga compensation rollbacks upon permanent failure.
 *
 * @param <ORCHESTRATION_CONTEXT>   The orchestration context type
 * @param <ORCHESTRATION_STATE_KEY> The orchestration state key enum type
 * @param <INPUT>                   The input payload type
 * @param <OUTPUT>                  The output result type
 */
public final class OrchestrationStateMachineCallable<
        ORCHESTRATION_CONTEXT extends StateMachineContext,
        ORCHESTRATION_STATE_KEY extends Enum<ORCHESTRATION_STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        extends AbstractStateMachineCallable<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> {

    private static final Logger log = LoggerFactory.getLogger(OrchestrationStateMachineCallable.class);

    @NonNull
    private final String machineName;

    @Nullable
    private final Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> checkpointListener;

    @NonNull
    private final ExecutorService virtualThreadExecutor;

    private final boolean ownsExecutor;

    private final List<ORCHESTRATION_STATE_KEY> completedStates = new ArrayList<>();
    private final Map<ORCHESTRATION_STATE_KEY, OrchestrationState<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> completedStateMap = new LinkedHashMap<>();

    public OrchestrationStateMachineCallable(
            @NonNull String machineName,
            @NonNull StateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable INPUT input,
            @Nullable ORCHESTRATION_CONTEXT initialContext,
            @Nullable Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> checkpointListener
    ) {
        this(
                machineName,
                configuration,
                input,
                initialContext,
                checkpointListener,
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("orch-step-vt-", 0).factory()),
                true
        );
    }

    public OrchestrationStateMachineCallable(
            @NonNull String machineName,
            @NonNull StateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable INPUT input,
            @Nullable ORCHESTRATION_CONTEXT initialContext,
            @Nullable Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> checkpointListener,
            @NonNull ExecutorService virtualThreadExecutor,
            boolean ownsExecutor
    ) {
        super(UUID.randomUUID(), configuration, input, initialContext);
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
        this.checkpointListener = checkpointListener;
        this.virtualThreadExecutor = Objects.requireNonNull(virtualThreadExecutor, "virtualThreadExecutor must not be null");
        this.ownsExecutor = ownsExecutor;
    }

    @Override
    protected void onExecutionStarted(@NonNull ORCHESTRATION_STATE_KEY initialStateKey, @NonNull ORCHESTRATION_CONTEXT context) {
        notifyCheckpoint(OrchestrationStatus.RUNNING, initialStateKey, completedStates, context, null);
    }

    @Override
    protected void onBeforeStateExecution(@NonNull ORCHESTRATION_STATE_KEY stateKey, @NonNull State<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> state, @NonNull ORCHESTRATION_CONTEXT context) {
        notifyCheckpoint(OrchestrationStatus.RUNNING, stateKey, completedStates, context, null);
    }

    @NonNull
    @Override
    protected ORCHESTRATION_CONTEXT executeState(
            @NonNull ORCHESTRATION_STATE_KEY stateKey,
            @NonNull State<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> state,
            @NonNull ORCHESTRATION_CONTEXT context
    ) throws Exception {
        if (state instanceof OrchestrationState<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> orchState) {
            // 1. Parallel Fork-Join Execution across Virtual Threads
            if (orchState.hasParallelBranches()) {
                return ParallelStateExecutor.executeParallelBranches(orchState.parallelBranches(), context, virtualThreadExecutor);
            }

            // 2. Child State Machine Execution (Atomic or Nested Orchestration) with Virtual Thread Recovery
            if (orchState.hasChildStateMachine()) {
                return executeChildStateMachineWithRecovery(orchState, stateKey, context);
            }
        }

        // 3. Direct Action Execution
        try {
            return state.action().execute(context);
        } catch (Exception e) {
            throw new ActionException(e);
        }
    }

    @Override
    protected void onAfterStateExecution(@NonNull ORCHESTRATION_STATE_KEY stateKey, @NonNull State<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> state, @NonNull ORCHESTRATION_CONTEXT context) {
        completedStates.add(stateKey);
        if (state instanceof OrchestrationState<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> orchState) {
            completedStateMap.put(stateKey, orchState);
        }
    }

    @Override
    protected void onExecutionCompleted(@NonNull ORCHESTRATION_CONTEXT context) {
        notifyCheckpoint(OrchestrationStatus.COMPLETED, null, completedStates, context, null);
    }

    @Nullable
    @Override
    protected OUTPUT handleExecutionFailure(
            @NonNull ORCHESTRATION_STATE_KEY currentStateKey,
            @NonNull ORCHESTRATION_CONTEXT context,
            @NonNull Throwable failure
    ) throws Exception {
        log.error("Orchestration [{}] failed in state [{}]. Initiating Saga compensation rollback...",
                machineName, currentStateKey, failure);

        notifyCheckpoint(OrchestrationStatus.COMPENSATING, currentStateKey, completedStates, context, failure);
        ORCHESTRATION_CONTEXT compensatedContext = executeSagaCompensations(completedStates, completedStateMap, context);
        notifyCheckpoint(OrchestrationStatus.COMPENSATED, currentStateKey, completedStates, compensatedContext, failure);

        if (failure instanceof StateMachineException sme) {
            configuration.stateMachineExceptionTrigger(this, sme);
            throw sme;
        } else if (failure instanceof Exception ex) {
            throw ex;
        } else {
            throw new RuntimeException("Orchestration machine failed in state " + currentStateKey, failure);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ORCHESTRATION_CONTEXT executeChildStateMachineWithRecovery(
            @NonNull OrchestrationState<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> state,
            @NonNull ORCHESTRATION_STATE_KEY stateKey,
            @NonNull ORCHESTRATION_CONTEXT orchestrationContext
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
                    callable = new OrchestrationStateMachineCallable(
                            nestedOrchConfig.machineName(),
                            nestedOrchConfig,
                            childInput,
                            null,
                            nestedOrchConfig.checkpointListener(),
                            virtualThreadExecutor,
                            false
                    );
                } else {
                    callable = new StateMachineCallable.StateMachineCallableBuilder()
                            .input(childInput)
                            .stateMachine(childConfig);
                }

                Future<?> future = virtualThreadExecutor.submit(callable);
                Object childOutput = future.get();

                if (outputMerger != null) {
                    return (ORCHESTRATION_CONTEXT) outputMerger.apply(orchestrationContext, childOutput);
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

    private ORCHESTRATION_CONTEXT executeSagaCompensations(
            @NonNull List<ORCHESTRATION_STATE_KEY> completedStates,
            @NonNull Map<ORCHESTRATION_STATE_KEY, OrchestrationState<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> completedStateMap,
            @NonNull ORCHESTRATION_CONTEXT context
    ) {
        ListIterator<ORCHESTRATION_STATE_KEY> iterator = completedStates.listIterator(completedStates.size());
        while (iterator.hasPrevious()) {
            ORCHESTRATION_STATE_KEY key = iterator.previous();
            OrchestrationState<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> state = completedStateMap.get(key);
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

    private void notifyCheckpoint(
            @NonNull OrchestrationStatus status,
            @Nullable ORCHESTRATION_STATE_KEY currentKey,
            @NonNull List<ORCHESTRATION_STATE_KEY> completed,
            @NonNull ORCHESTRATION_CONTEXT context,
            @Nullable Throwable error
    ) {
        if (checkpointListener != null) {
            OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> cp = new OrchestrationCheckpoint<>(
                    uuid,
                    machineName,
                    status,
                    currentKey,
                    List.copyOf(completed),
                    context,
                    error,
                    Instant.now()
            );
            try {
                checkpointListener.accept(cp);
            } catch (Exception e) {
                log.warn("Error notifying orchestration checkpoint listener: {}", e.getMessage(), e);
            }
        }
    }

    @Override
    protected void cleanup() {
        if (ownsExecutor) {
            virtualThreadExecutor.shutdown();
        }
    }
}
