package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.StateMachineFuture;
import com.github.f442y.dispersion.config.StateMachineConfiguration;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.executor.AdmissionController;
import com.github.f442y.dispersion.executor.StateMachineExecutor;
import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Execution coordinator for Orchestration State Machines.
 * Submits macro state machines to Java 25 Virtual Threads, capturing checkpoints,
 * executing child atomic state machines on fresh virtual threads, and coordinating Saga rollbacks.
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
    private final StateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration;

    @Nullable
    private final Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> checkpointListener;

    @NonNull
    private final ExecutorService executorService;

    @NonNull
    private final AdmissionController admissionController;

    public OrchestrationStateMachineExecutor(
            @NonNull OrchestrationStateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration
    ) {
        this(configuration.machineName(), configuration, configuration.checkpointListener(), new AdmissionController(10_000));
    }

    public OrchestrationStateMachineExecutor(
            @NonNull OrchestrationStateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration,
            @NonNull AdmissionController admissionController
    ) {
        this(configuration.machineName(), configuration, configuration.checkpointListener(), admissionController);
    }

    public OrchestrationStateMachineExecutor(
            @NonNull String machineName,
            @NonNull StateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> checkpointListener
    ) {
        this(machineName, configuration, checkpointListener, new AdmissionController(10_000));
    }

    public OrchestrationStateMachineExecutor(
            @NonNull String machineName,
            @NonNull StateMachineConfiguration<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> configuration,
            @Nullable Consumer<OrchestrationCheckpoint<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY>> checkpointListener,
            @NonNull AdmissionController admissionController
    ) {
        this.machineName = Objects.requireNonNull(machineName, "machineName must not be null");
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.checkpointListener = checkpointListener;
        this.admissionController = Objects.requireNonNull(admissionController, "admissionController must not be null");
        this.executorService = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("orch-exec-" + machineName + "-", 0).factory()
        );
    }

    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable INPUT input) throws Exception {
        return dispatchSync(null, input);
    }

    @Override
    @Nullable
    public OUTPUT dispatchSync(@Nullable ORCHESTRATION_CONTEXT initialContext, @Nullable INPUT input) throws Exception {
        admissionController.acquirePermit();
        try {
            OrchestrationStateMachineCallable<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> callable =
                    new OrchestrationStateMachineCallable<>(machineName, configuration, input, initialContext, checkpointListener, executorService, false);
            return callable.call();
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
        admissionController.acquirePermit();

        OrchestrationStateMachineCallable<ORCHESTRATION_CONTEXT, ORCHESTRATION_STATE_KEY, INPUT, OUTPUT> callable =
                new OrchestrationStateMachineCallable<>(machineName, configuration, input, initialContext, checkpointListener, executorService, false);

        CompletableFuture<OUTPUT> completableFuture = new CompletableFuture<>();

        executorService.submit(() -> {
            try {
                OUTPUT result = callable.call();
                completableFuture.complete(result);
            } catch (Throwable t) {
                completableFuture.completeExceptionally(t);
            } finally {
                admissionController.releasePermit();
            }
        });

        return new StateMachineFuture<>(
                callable.uuid(),
                completableFuture,
                callable::cancel
        );
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
