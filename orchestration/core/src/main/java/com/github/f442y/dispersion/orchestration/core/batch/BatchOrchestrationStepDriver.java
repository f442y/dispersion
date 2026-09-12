package com.github.f442y.dispersion.orchestration.core.batch;

import com.github.f442y.dispersion.orchestration.*;
import com.github.f442y.dispersion.orchestration.batch.*;
import com.github.f442y.dispersion.orchestration.command.*;
import com.github.f442y.dispersion.orchestration.messaging.*;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.orchestration.CompensationAction;
import com.github.f442y.dispersion.orchestration.OrchestrationStatus;
import com.github.f442y.dispersion.orchestration.SignalHandler;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.fsm.state.Action;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.Transition;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Execution driver for turn-based batch orchestrations with concurrent item execution,
 * barrier synchronization, and item-level signal rehydration on Java 25 Virtual Threads.
 */
public final class BatchOrchestrationStepDriver {

    private static final Logger log = LoggerFactory.getLogger(BatchOrchestrationStepDriver.class);

    private BatchOrchestrationStepDriver() {}

    public static final class ItemStateDefinition<
            ITEM_CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey> {

        public Action<ITEM_CONTEXT> action = Action.identity();
        public Transition<ITEM_CONTEXT, STATE_KEY> transition;
        public boolean isBarrier;
        public BarrierPolicy barrierPolicy;
        public String expectedSignal;
        public SignalHandler<ITEM_CONTEXT, ?> signalHandler;
        public CompensationAction<ITEM_CONTEXT> compensationAction = CompensationAction.noop();
    }

    public static final class BatchConfiguration<
            BATCH_CONTEXT extends StateMachineContext,
            ITEM_CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            OUTPUT> {

        public String batchName;
        public STATE_KEY initialStateKey;
        public Set<STATE_KEY> endStates = new HashSet<>();
        public Function<BATCH_CONTEXT, String> batchKeyExtractor;
        public Function<ITEM_CONTEXT, String> itemKeyExtractor;
        public Function<BATCH_CONTEXT, OUTPUT> outputFunction;
        public BatchFailurePolicy failurePolicy = BatchFailurePolicy.FAIL_FAST;
        public Map<STATE_KEY, ItemStateDefinition<ITEM_CONTEXT, STATE_KEY>> stateDefinitions = new LinkedHashMap<>();
    }

    /**
     * Executes the initial turn of a batch orchestration across all provided item contexts.
     */
    @NonNull
    public static <
            BATCH_CONTEXT extends StateMachineContext,
            ITEM_CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            OUTPUT>
    BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> executeBatchTurn(
            @NonNull UUID batchId,
            @NonNull BatchConfiguration<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> config,
            @NonNull BATCH_CONTEXT batchContext,
            @NonNull List<ITEM_CONTEXT> initialItemContexts,
            @NonNull ExecutorService virtualThreadExecutor
    ) {

        String extractedKey = (config.batchKeyExtractor != null) ? config.batchKeyExtractor.apply(batchContext) : null;
        String batchKey = (extractedKey != null) ? extractedKey : batchId.toString();
        Map<String, STATE_KEY> itemStates = new ConcurrentHashMap<>();
        Map<String, ITEM_CONTEXT> itemContexts = new ConcurrentHashMap<>();
        Set<String> arrivedBarrierItemKeys = ConcurrentHashMap.newKeySet();

        for (ITEM_CONTEXT itemCtx : initialItemContexts) {
            String itemKey = config.itemKeyExtractor.apply(itemCtx);
            itemStates.put(itemKey, config.initialStateKey);
            itemContexts.put(itemKey, itemCtx);
        }

        return runBatchTurn(
                batchId,
                batchKey,
                config,
                batchContext,
                itemStates,
                itemContexts,
                arrivedBarrierItemKeys,
                null,
                null,
                null,
                virtualThreadExecutor
        );
    }

    /**
     * Resumes an existing batch orchestration with an item-level or batch-level signal.
     */
    @NonNull
    public static <
            BATCH_CONTEXT extends StateMachineContext,
            ITEM_CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            OUTPUT>
    BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> resumeBatchTurn(
            @NonNull BatchConfiguration<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> config,
            @NonNull BatchOrchestrationCheckpoint<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY> checkpoint,
            @Nullable String targetItemKey,
            @Nullable Object itemSignal,
            @Nullable Object batchSignal,
            @NonNull ExecutorService virtualThreadExecutor
    ) {

        Map<String, STATE_KEY> itemStates = new ConcurrentHashMap<>(checkpoint.itemStates());
        Map<String, ITEM_CONTEXT> itemContexts = new ConcurrentHashMap<>(checkpoint.itemContexts());
        Set<String> arrivedBarrierItemKeys = ConcurrentHashMap.newKeySet();
        arrivedBarrierItemKeys.addAll(checkpoint.arrivedBarrierItemKeys());
        Set<UUID> processedCommandIds = ConcurrentHashMap.newKeySet();
        processedCommandIds.addAll(checkpoint.processedCommandIds());

        Object effectiveItemSignal = itemSignal;

        if (itemSignal instanceof CommandEnvelope<?> env) {
            if (processedCommandIds.contains(env.commandId())) {
                log.info("Ignoring duplicate command [{}] for batch [{}]", env.commandId(), checkpoint.batchKey());
                OUTPUT out = (config.outputFunction != null) ? config.outputFunction.apply(checkpoint.batchContext()) : null;
                return new BatchTurnResult<>(
                        checkpoint.batchId(),
                        checkpoint.batchKey(),
                        checkpoint.status(),
                        checkpoint.currentBatchStateKey(),
                        itemStates,
                        itemContexts,
                        checkpoint.batchContext(),
                        out,
                        null
                );
            }
            processedCommandIds.add(env.commandId());
            effectiveItemSignal = env.command();
        }

        return runBatchTurn(
                checkpoint.batchId(),
                checkpoint.batchKey(),
                config,
                checkpoint.batchContext(),
                itemStates,
                itemContexts,
                arrivedBarrierItemKeys,
                targetItemKey,
                effectiveItemSignal,
                batchSignal,
                virtualThreadExecutor
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <
            BATCH_CONTEXT extends StateMachineContext,
            ITEM_CONTEXT extends StateMachineContext,
            STATE_KEY extends Enum<STATE_KEY> & StateKey,
            OUTPUT>
    BatchTurnResult<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> runBatchTurn(
            @NonNull UUID batchId,
            @NonNull String batchKey,
            @NonNull BatchConfiguration<BATCH_CONTEXT, ITEM_CONTEXT, STATE_KEY, OUTPUT> config,
            @NonNull BATCH_CONTEXT batchContext,
            @NonNull Map<String, STATE_KEY> itemStates,
            @NonNull Map<String, ITEM_CONTEXT> itemContexts,
            @NonNull Set<String> arrivedBarrierItemKeys,
            @Nullable String targetItemKey,
            @Nullable Object itemSignalPayload,
            @Nullable Object batchSignalPayload,
            @NonNull ExecutorService virtualThreadExecutor
    ) {

        List<Throwable> failures = new CopyOnWriteArrayList<>();

        String currentTargetItemKey = targetItemKey;
        Object currentItemSignalPayload = itemSignalPayload;

        // Process loop: continues until no further items can make progress in this turn
        boolean progressMadeInTurn;
        do {
            progressMadeInTurn = false;
            Set<String> candidatesToRun = new HashSet<>();

            for (Map.Entry<String, STATE_KEY> entry : itemStates.entrySet()) {
                String itemKey = entry.getKey();
                STATE_KEY state = entry.getValue();

                if (!config.endStates.contains(state)) {
                    candidatesToRun.add(itemKey);
                }
            }

            if (candidatesToRun.isEmpty()) {
                break;
            }

            final String effectiveTargetItemKey = currentTargetItemKey;
            final Object effectiveItemSignal = currentItemSignalPayload;

            List<Future<Boolean>> futures = new ArrayList<>();

            for (String itemKey : candidatesToRun) {
                futures.add(virtualThreadExecutor.submit(() -> {
                    boolean moved = false;
                    try {
                        STATE_KEY currentState = itemStates.get(itemKey);
                        ITEM_CONTEXT context = itemContexts.get(itemKey);
                        Object pendingSignal = (itemKey.equals(effectiveTargetItemKey)) ? effectiveItemSignal : null;

                        while (currentState != null && !config.endStates.contains(currentState)) {
                            ItemStateDefinition<ITEM_CONTEXT, STATE_KEY> stateDef = config.stateDefinitions.get(currentState);
                            if (stateDef == null) break;

                            // 1. Barrier Check
                            if (stateDef.isBarrier) {
                                arrivedBarrierItemKeys.add(itemKey);
                                boolean barrierUnlocked = false;

                                if (stateDef.barrierPolicy == BarrierPolicy.ALL_ITEMS_ARRIVED) {
                                    if (arrivedBarrierItemKeys.size() >= itemStates.size()) {
                                        barrierUnlocked = true;
                                    }
                                } else if (stateDef.barrierPolicy == BarrierPolicy.SIGNAL_TRIGGERED) {
                                    if (batchSignalPayload != null) {
                                        barrierUnlocked = true;
                                    }
                                }

                                if (!barrierUnlocked) {
                                    // Item holds at barrier
                                    break;
                                }
                            }

                            // 2. Signal Wait Check
                            if (stateDef.expectedSignal != null) {
                                if (pendingSignal != null) {
                                    SignalHandler handler = stateDef.signalHandler;
                                    if (handler != null) {
                                        context = (ITEM_CONTEXT) handler.handleSignal(context, pendingSignal);
                                    }
                                    pendingSignal = null; // Signal consumed
                                    moved = true;
                                } else {
                                    // Suspend this item at wait state
                                    break;
                                }
                            } else {
                                // Standard Item Action
                                if (stateDef.action != null) {
                                    context = stateDef.action.execute(context);
                                }
                                moved = true;
                            }

                            itemContexts.put(itemKey, context);

                            // 3. Transition to next state
                            if (stateDef.transition != null) {
                                STATE_KEY nextState = stateDef.transition.nextState(context);
                                currentState = nextState;
                                itemStates.put(itemKey, nextState);
                            } else {
                                break;
                            }
                        }
                    } catch (Throwable t) {
                        failures.add(t);
                        log.error("Batch [{}] Item [{}] failed: {}", batchKey, itemKey, t.getMessage(), t);
                    }
                    return moved;
                }));
            }

            for (Future<Boolean> f : futures) {
                try {
                    if (Boolean.TRUE.equals(f.get())) {
                        progressMadeInTurn = true;
                    }
                } catch (Exception ex) {
                    failures.add(ex);
                }
            }

            // Only consume the targetItemKey signal once
            currentTargetItemKey = null;
            currentItemSignalPayload = null;

        } while (progressMadeInTurn && failures.isEmpty());

        if (!failures.isEmpty()) {
            Throwable firstFailure = failures.getFirst();
            if (config.failurePolicy == BatchFailurePolicy.FAIL_FAST) {
                OUTPUT out = (config.outputFunction != null) ? config.outputFunction.apply(batchContext) : null;
                return new BatchTurnResult<>(
                        batchId,
                        batchKey,
                        OrchestrationStatus.COMPENSATED,
                        null,
                        itemStates,
                        itemContexts,
                        batchContext,
                        out,
                        firstFailure
                );
            }
        }

        // Check if all items reached terminal states
        boolean allCompleted = true;
        for (STATE_KEY st : itemStates.values()) {
            if (!config.endStates.contains(st)) {
                allCompleted = false;
                break;
            }
        }

        OrchestrationStatus status = allCompleted ? OrchestrationStatus.COMPLETED : OrchestrationStatus.SUSPENDED;
        OUTPUT output = (allCompleted && config.outputFunction != null) ? config.outputFunction.apply(batchContext) : null;

        return new BatchTurnResult<>(
                batchId,
                batchKey,
                status,
                null,
                itemStates,
                itemContexts,
                batchContext,
                output,
                null
        );
    }
}
