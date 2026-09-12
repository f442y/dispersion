package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.fsm.StateMachineFuture;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.executor.StateMachineExecutor;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import com.github.f442y.dispersion.orchestration.CheckpointStore;
import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineConfiguration;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * High-performance turn-based Orchestration State Machine Executor coordinating durable workflow
 * execution, signal suspension/rehydration, idempotent command routing, and Saga compensation on Java 25 Virtual Threads.
 *
 * @param <CONTEXT>   The context type
 * @param <STATE_KEY> The state key enum type
 * @param <INPUT>     The input type
 * @param <OUTPUT>    The output type
 */
public class OrchestrationStateMachineExecutor<
        CONTEXT extends StateMachineContext,
        STATE_KEY extends Enum<STATE_KEY> & StateKey,
        INPUT,
        OUTPUT> implements StateMachineExecutor<CONTEXT, INPUT, OUTPUT> {

    private final OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration;
    private final ExecutorService virtualThreadExecutor;
    private final OrchestrationSignalWatcher<CONTEXT, STATE_KEY, INPUT, OUTPUT> signalWatcher;

    public OrchestrationStateMachineExecutor(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration
    ) {
        this(
                configuration,
                Executors.newThreadPerTaskExecutor(
                        Thread.ofVirtual().name("orch-" + configuration.getMachineName() + "-", 0).factory()
                )
        );
    }

    public OrchestrationStateMachineExecutor(
            @NonNull OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull ExecutorService virtualThreadExecutor
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.virtualThreadExecutor = Objects.requireNonNull(virtualThreadExecutor, "virtualThreadExecutor must not be null");
        this.signalWatcher = new OrchestrationSignalWatcher<>(configuration, virtualThreadExecutor);
    }

    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable INPUT input) throws Exception {
        return dispatchSync(null, input);
    }

    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable CONTEXT initialContext, @Nullable INPUT input) throws Exception {
        UUID machineId = UUID.randomUUID();
        OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> result =
                OrchestrationStepDriver.executeTurn(machineId, configuration, initialContext, input, virtualThreadExecutor);

        if (result.isFailed() || result.isCompensated()) {
            if (result.error() instanceof Exception ex) throw ex;
            if (result.error() != null) throw new RuntimeException(result.error());
        }

        if (result.isSuspended()) {
            String stateName = result.currentStateKey() != null ? result.currentStateKey().name() : "UNKNOWN";
            String signal = result.expectedSignal() != null ? result.expectedSignal() : "UNKNOWN";
            throw new IllegalStateException("Orchestration machine [" + configuration.getMachineName()
                    + "] suspended in state [" + stateName
                    + "] awaiting signal [" + signal
                    + "]. For workflows with suspension points, use dispatchTurnSync() to inspect the OrchestrationTurnResult.");
        }

        return result.output();
    }

    @Override
    @NonNull
    public StateMachineFuture<OUTPUT> dispatchAsync(@Nullable INPUT input) {
        return dispatchAsync(null, input);
    }

    @Override
    @NonNull
    public StateMachineFuture<OUTPUT> dispatchAsync(@Nullable CONTEXT initialContext, @Nullable INPUT input) {
        UUID machineId = UUID.randomUUID();
        CompletableFuture<OUTPUT> future = new CompletableFuture<>();
        try {
            virtualThreadExecutor.submit(() -> {
                try {
                    future.complete(dispatchSync(initialContext, input));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return new StateMachineFuture<>(machineId, future);
    }

    /**
     * Executes the initial turn synchronously and returns the full {@link OrchestrationTurnResult}.
     */
    @NonNull
    public OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT> dispatchTurnSync(
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) throws Exception {
        UUID machineId = UUID.randomUUID();
        return OrchestrationStepDriver.executeTurn(machineId, configuration, initialContext, input, virtualThreadExecutor);
    }

    /**
     * Executes the initial turn asynchronously and returns a {@link CompletableFuture} with the {@link OrchestrationTurnResult}.
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> dispatchTurnAsync(
            @Nullable CONTEXT initialContext,
            @Nullable INPUT input
    ) {
        CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> future = new CompletableFuture<>();
        try {
            virtualThreadExecutor.submit(() -> {
                try {
                    future.complete(dispatchTurnSync(initialContext, input));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * Delivers an external signal to a suspended orchestration by its machine UUID.
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> sendSignal(
            @NonNull UUID machineId,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        return signalWatcher.deliverSignal(machineId, signalName, signalPayload);
    }

    /**
     * Delivers an external signal to a suspended orchestration by its domain correlation key.
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> sendSignalByCorrelationKey(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        return signalWatcher.deliverSignalByCorrelationKey(correlationKey, signalName, signalPayload);
    }

    /**
     * Routes a typed domain {@link SignalCommand} directly to the matching suspended orchestration.
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull SignalCommand command
    ) {
        return signalWatcher.handleCommand(command);
    }

    /**
     * Routes an idempotent {@link CommandEnvelope} ensuring deduplication against network retries.
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<CONTEXT, STATE_KEY, OUTPUT>> handleCommand(
            @NonNull CommandEnvelope<? extends SignalCommand> envelope
    ) {
        return signalWatcher.handleCommand(envelope);
    }

    @NonNull
    public OrchestrationStateMachineConfiguration<CONTEXT, STATE_KEY, INPUT, OUTPUT> getConfiguration() {
        return configuration;
    }

    /**
     * Adapts this orchestration state machine into an {@link InspectableMachine} SPI instance
     * for monitoring, topology discovery, checkpoint inspection, and signal routing in the Control Plane.
     */
    @NonNull
    public InspectableMachine asInspectableMachine() {
        String machineName = configuration.getMachineName();
        StateMap<CONTEXT, STATE_KEY> stateMap = configuration.getStateMap();
        String initialState = stateMap.getInitialState().name();
        Set<String> endStates = stateMap.getEndStates().stream()
                .map(Enum::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> allStateNames = new LinkedHashSet<>();
        stateMap.getAllStates().keySet().forEach(k -> allStateNames.add(k.name()));
        stateMap.getEndStates().forEach(k -> allStateNames.add(k.name()));
        List<String> allStates = List.copyOf(allStateNames);

        String mermaid = stateMap.toMermaid();

        MachineDescriptor descriptor = new MachineDescriptor(
                machineName,
                MachineType.ORCHESTRATION,
                initialState,
                endStates,
                allStates,
                mermaid
        );

        return new InspectableMachine() {
            @Override
            @NonNull
            public MachineDescriptor descriptor() {
                return descriptor;
            }

            @Override
            @NonNull
            public CompletableFuture<SignalDeliveryResult> sendSignal(
                    @NonNull String correlationKey,
                    @NonNull String signalName,
                    @Nullable Object payload
            ) {
                return sendSignalByCorrelationKey(correlationKey, signalName, payload != null ? payload : new Object())
                        .handle((turnResult, throwable) -> {
                            if (throwable != null) {
                                return SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                                        "Error executing turn upon signal delivery: " + throwable.getMessage());
                            }
                            if (turnResult == null) {
                                return SignalDeliveryResult.failure(machineName, correlationKey, signalName,
                                        "No execution turn result returned");
                            }
                            String resultingState = turnResult.currentStateKey() != null
                                    ? turnResult.currentStateKey().name()
                                    : null;
                            String errorMessage = turnResult.error() != null
                                    ? turnResult.error().getMessage()
                                    : null;
                            return new SignalDeliveryResult(
                                    true,
                                    "Signal delivered successfully",
                                    machineName,
                                    correlationKey,
                                    signalName,
                                    turnResult.isCompleted(),
                                    turnResult.isSuspended(),
                                    resultingState,
                                    errorMessage
                            );
                        });
            }

            @Override
            @NonNull
            public Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                CheckpointStore<CONTEXT, STATE_KEY> store = configuration.getCheckpointStore();
                if (store == null) {
                    return Optional.empty();
                }
                return store.findByCorrelationKey(correlationKey).map(cp -> (Object) cp);
            }
        };
    }

    @Override
    public void close() {
        virtualThreadExecutor.close();
    }
}

