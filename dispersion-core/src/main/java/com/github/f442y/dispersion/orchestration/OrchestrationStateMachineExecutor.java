package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.StateMachineFuture;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.executor.AdmissionController;
import com.github.f442y.dispersion.executor.StateMachineExecutor;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Execution coordinator for Orchestration State Machines.
 * Leverages the turn-based {@link OrchestrationStepDriver} and {@link OrchestrationSignalWatcher}
 * to execute steps on Java 25 Virtual Threads, persist checkpoints to {@link CheckpointStore},
 * handle external signal resumption, execute child atomic state machines, and coordinate Saga rollbacks.
 *
 * @param <ORCHESTRATION_CONTEXT>   The orchestration context type
 * @param <ORCHESTRATION_STATE_KEY> The orchestration state key enum type
 * @param <INPUT>                   The input payload type
 * @param <OUTPUT>                  The output result type
 */
public class OrchestrationStateMachineExecutor<
        ORCHESTRATION_CONTEXT extends StateMachineContext,
        ORCHESTRATION_STATE_KEY extends Enum<ORCHESTRATION_STATE_KEY> & StateKey,
        INPUT,
        OUTPUT>
        implements StateMachineExecutor<ORCHESTRATION_CONTEXT, INPUT, OUTPUT> {

    @NonNull
    private final String machineName;

    @NonNull
    private final OrchestrationStateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration;

    @NonNull
    private final CheckpointStore<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> checkpointStore;

    @NonNull
    private final OrchestrationSignalWatcher<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> signalWatcher;

    @NonNull
    private final ExecutorService executorService;

    @NonNull
    private final AdmissionController admissionController;

    @SuppressWarnings("unchecked")
    public OrchestrationStateMachineExecutor(
            @NonNull OrchestrationStateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration
    ) {
        this(configuration, new AdmissionController(10_000));
    }

    public OrchestrationStateMachineExecutor(
            @NonNull OrchestrationStateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull AdmissionController admissionController
    ) {
        this.machineName = configuration.machineName();
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.admissionController = Objects.requireNonNull(admissionController, "admissionController must not be null");
        this.checkpointStore = (configuration.checkpointStore() != null)
                ? configuration.checkpointStore()
                : new InMemoryCheckpointStore<>();
        this.executorService = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("orch-exec-" + machineName + "-", 0).factory()
        );
        this.signalWatcher = new OrchestrationSignalWatcher<>(this.configuration, this.checkpointStore, this.executorService);
    }

    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable INPUT input) throws Exception {
        return dispatchSync(null, input);
    }

    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable ORCHESTRATION_CONTEXT initialContext, @Nullable INPUT input) throws Exception {
        OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT> turnResult =
                dispatchTurnSync(initialContext, input);

        if (turnResult.isFailed()) {
            if (turnResult.error() instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException(turnResult.error());
        }

        return turnResult.output();
    }

    /**
     * Executes the first turn synchronously, returning the complete turn result (Completed, Suspended, or Failed).
     *
     * @param initialContext Optional pre-populated initial context
     * @param input          Optional input payload
     * @return The result of this execution turn
     * @throws Exception If an unhandled error occurs
     */
    @NonNull
    public OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT> dispatchTurnSync(
            @Nullable ORCHESTRATION_CONTEXT initialContext,
            @Nullable INPUT input
    ) throws Exception {
        admissionController.acquirePermit();
        try {
            return OrchestrationStepDriver.executeTurn(
                    UUID.randomUUID(),
                    configuration,
                    input,
                    initialContext,
                    executorService
            );
        } finally {
            admissionController.releasePermit();
        }
    }

    @Override
    @NonNull
    public StateMachineFuture<OUTPUT> dispatchAsync(@Nullable INPUT input) throws Exception {
        return dispatchAsync(null, input);
    }

    @Override
    @NonNull
    public StateMachineFuture<OUTPUT> dispatchAsync(@Nullable ORCHESTRATION_CONTEXT initialContext, @Nullable INPUT input) throws Exception {
        UUID machineId = UUID.randomUUID();
        CompletableFuture<OUTPUT> completableFuture = new CompletableFuture<>();

        admissionController.acquirePermit();
        executorService.submit(() -> {
            try {
                OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT> turnResult =
                        OrchestrationStepDriver.executeTurn(
                                machineId,
                                configuration,
                                input,
                                initialContext,
                                executorService
                        );

                if (turnResult.isFailed()) {
                    completableFuture.completeExceptionally(turnResult.error());
                } else {
                    completableFuture.complete(turnResult.output());
                }
            } catch (Throwable t) {
                completableFuture.completeExceptionally(t);
            } finally {
                admissionController.releasePermit();
            }
        });

        return new StateMachineFuture<>(machineId, completableFuture, () -> {});
    }

    /**
     * Executes the initial turn asynchronously, returning the detailed {@link OrchestrationTurnResult}.
     *
     * @param initialContext Optional pre-populated initial context
     * @param input          Optional input payload
     * @return CompletableFuture resolving to the turn result
     * @throws Exception If permit acquisition fails
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT>> dispatchTurnAsync(
            @Nullable ORCHESTRATION_CONTEXT initialContext,
            @Nullable INPUT input
    ) throws Exception {
        UUID machineId = UUID.randomUUID();
        CompletableFuture<OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT>> future = new CompletableFuture<>();

        admissionController.acquirePermit();
        executorService.submit(() -> {
            try {
                OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT> turnResult =
                        OrchestrationStepDriver.executeTurn(
                                machineId,
                                configuration,
                                input,
                                initialContext,
                                executorService
                        );
                future.complete(turnResult);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                admissionController.releasePermit();
            }
        });

        return future;
    }

    /**
     * Delivers an external signal to a suspended orchestration instance by its machine UUID.
     *
     * @param machineId     The unique workflow machine ID
     * @param signalName    The name of the incoming signal
     * @param signalPayload The signal event data payload
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT>> sendSignal(
            @NonNull UUID machineId,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        return signalWatcher.sendSignal(machineId, signalName, signalPayload);
    }

    /**
     * Delivers an external signal to a suspended orchestration instance by its domain correlation key.
     *
     * @param correlationKey The business correlation identifier
     * @param signalName     The name of the incoming signal
     * @param signalPayload  The signal event data payload
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT>> sendSignalByCorrelationKey(
            @NonNull String correlationKey,
            @NonNull String signalName,
            @Nullable Object signalPayload
    ) {
        return signalWatcher.sendSignalByCorrelationKey(correlationKey, signalName, signalPayload);
    }

    /**
     * Dispatches a strongly typed {@link SignalCommand} using the correlation key declared on the command.
     *
     * @param command The typed signal command
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT>> handleCommand(
            @NonNull SignalCommand command
    ) {
        return signalWatcher.handleCommand(command);
    }

    /**
     * Dispatches a strongly typed {@link SignalCommand} directly to a specific workflow machine UUID.
     *
     * @param machineId The workflow machine ID
     * @param command   The typed signal command
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT>> handleCommand(
            @NonNull UUID machineId,
            @NonNull SignalCommand command
    ) {
        return signalWatcher.handleCommand(machineId, command);
    }

    /**
     * Dispatches an idempotent {@link CommandEnvelope} using the correlation key declared on the command.
     *
     * @param envelope The command envelope containing deduplication commandId and payload
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT>> handleCommand(
            @NonNull CommandEnvelope<? extends SignalCommand> envelope
    ) {
        return signalWatcher.handleCommand(envelope);
    }

    /**
     * Dispatches an idempotent {@link CommandEnvelope} directly to a specific workflow machine UUID.
     *
     * @param machineId The workflow machine ID
     * @param envelope  The command envelope containing deduplication commandId and payload
     * @return CompletableFuture containing the outcome of the resumed execution turn
     */
    @NonNull
    public CompletableFuture<OrchestrationTurnResult<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, OUTPUT>> handleCommand(
            @NonNull UUID machineId,
            @NonNull CommandEnvelope<? extends SignalCommand> envelope
    ) {
        return signalWatcher.handleCommand(machineId, envelope);
    }

    /**
     * Looks up an active or suspended checkpoint by machine ID.
     *
     * @param machineId The workflow machine ID
     * @return Optional containing the checkpoint snapshot if found
     */
    @NonNull
    public Optional<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> getCheckpoint(@NonNull UUID machineId) {
        return checkpointStore.findById(machineId);
    }

    /**
     * Looks up an active or suspended checkpoint by domain correlation key.
     *
     * @param correlationKey The correlation identifier
     * @return Optional containing the checkpoint snapshot if found
     */
    @NonNull
    public Optional<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> getCheckpointByCorrelationKey(@NonNull String correlationKey) {
        return checkpointStore.findByCorrelationKey(correlationKey);
    }

    /**
     * Returns the signal watcher instance for external signal registrations.
     *
     * @return The {@link OrchestrationSignalWatcher}
     */
    @NonNull
    public OrchestrationSignalWatcher<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> signalWatcher() {
        return signalWatcher;
    }

    /**
     * Returns the configured checkpoint store.
     *
     * @return The {@link CheckpointStore}
     */
    @NonNull
    public CheckpointStore<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY> checkpointStore() {
        return checkpointStore;
    }

    @NonNull
    public StateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration() {
        return configuration;
    }

    @Override
    public void close() {
        executorService.close();
    }
}
