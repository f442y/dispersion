package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.StateMachineFuture;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.executor.StateMachineExecutor;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        virtualThreadExecutor.submit(() -> {
            try {
                future.complete(dispatchSync(initialContext, input));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
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
        virtualThreadExecutor.submit(() -> {
            try {
                future.complete(dispatchTurnSync(initialContext, input));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
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

    @Override
    public void close() {
        virtualThreadExecutor.close();
    }
}
