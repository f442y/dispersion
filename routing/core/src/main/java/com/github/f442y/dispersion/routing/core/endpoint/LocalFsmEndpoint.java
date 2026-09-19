package com.github.f442y.dispersion.routing.core.endpoint;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.ExecutionEventListener;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.state.StateExitedEvent;
import com.github.f442y.dispersion.event.state.TransitionEvaluatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.fsm.config.InputFunction;
import com.github.f442y.dispersion.fsm.config.OutputFunction;
import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.context.StateMachineContextFactory;
import com.github.f442y.dispersion.fsm.state.State;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.fsm.state.StateMap;
import com.github.f442y.dispersion.routing.endpoint.EndpointStatus;
import com.github.f442y.dispersion.routing.endpoint.EndpointType;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public final class LocalFsmEndpoint<
        CHILD_CONTEXT extends StateMachineContext,
        CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
        WORKLOAD_INPUT,
        WORKLOAD_OUTPUT> implements WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> {

    private static final Logger log = LoggerFactory.getLogger(LocalFsmEndpoint.class);
    private static final Executor VIRTUAL_THREAD_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final String endpointId;
    private final StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, WORKLOAD_INPUT, WORKLOAD_OUTPUT> configuration;
    private final EndpointTags tags;
    private final int maxConcurrency;
    private final AtomicInteger activeWorkloads = new AtomicInteger(0);
    private volatile EndpointStatus status = EndpointStatus.HEALTHY;

    public LocalFsmEndpoint(
            @NonNull String endpointId,
            @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, WORKLOAD_INPUT, WORKLOAD_OUTPUT> configuration,
            @NonNull EndpointTags tags,
            int maxConcurrency
    ) {
        this.endpointId = Objects.requireNonNull(endpointId, "endpointId must not be null");
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.tags = Objects.requireNonNull(tags, "tags must not be null");
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive: " + maxConcurrency);
        }
        this.maxConcurrency = maxConcurrency;
    }

    @NonNull
    public static <
            CHILD_CONTEXT extends StateMachineContext,
            CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
            WORKLOAD_INPUT,
            WORKLOAD_OUTPUT>
    LocalFsmEndpoint<CHILD_CONTEXT, CHILD_STATE_KEY, WORKLOAD_INPUT, WORKLOAD_OUTPUT> of(
            @NonNull String endpointId,
            @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, WORKLOAD_INPUT, WORKLOAD_OUTPUT> configuration,
            @NonNull EndpointTags tags,
            int maxConcurrency
    ) {
        return new LocalFsmEndpoint<>(endpointId, configuration, tags, maxConcurrency);
    }

    @NonNull
    public static <
            CHILD_CONTEXT extends StateMachineContext,
            CHILD_STATE_KEY extends Enum<CHILD_STATE_KEY> & StateKey,
            WORKLOAD_INPUT,
            WORKLOAD_OUTPUT>
    LocalFsmEndpoint<CHILD_CONTEXT, CHILD_STATE_KEY, WORKLOAD_INPUT, WORKLOAD_OUTPUT> of(
            @NonNull String endpointId,
            @NonNull StateMachineConfiguration<CHILD_CONTEXT, CHILD_STATE_KEY, WORKLOAD_INPUT, WORKLOAD_OUTPUT> configuration
    ) {
        return new LocalFsmEndpoint<>(endpointId, configuration, EndpointTags.empty(), 500);
    }

    @Override
    @NonNull
    public String endpointId() {
        return endpointId;
    }

    @Override
    @NonNull
    public EndpointType endpointType() {
        return EndpointType.LOCAL;
    }

    @Override
    @NonNull
    public EndpointStatus status() {
        return status;
    }

    public void setStatus(@NonNull EndpointStatus status) {
        this.status = Objects.requireNonNull(status, "status must not be null");
    }

    @Override
    @NonNull
    public EndpointTags tags() {
        return tags;
    }

    @Override
    public int activeWorkloads() {
        return activeWorkloads.get();
    }

    @Override
    public int maxConcurrency() {
        return maxConcurrency;
    }

    @Override
    public boolean canAccept() {
        return status == EndpointStatus.HEALTHY && activeWorkloads.get() < maxConcurrency;
    }

    @Override
    @NonNull
    public WORKLOAD_OUTPUT executeSync(
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata,
            @NonNull Duration timeout
    ) throws Exception {
        Objects.requireNonNull(input, "input must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");

        activeWorkloads.incrementAndGet();
        try {
            return executeDirect(input, metadata);
        } finally {
            activeWorkloads.decrementAndGet();
        }
    }

    @Override
    @NonNull
    public CompletableFuture<WORKLOAD_OUTPUT> executeAsync(
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata
    ) {
        Objects.requireNonNull(input, "input must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");

        return CompletableFuture.supplyAsync(() -> {
            activeWorkloads.incrementAndGet();
            try {
                return executeDirect(input, metadata);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                activeWorkloads.decrementAndGet();
            }
        }, VIRTUAL_THREAD_EXECUTOR);
    }

    private WORKLOAD_OUTPUT executeDirect(
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata
    ) throws Exception {
        StateMap<CHILD_CONTEXT, CHILD_STATE_KEY> stateMap = configuration.getStateMap();
        StateMachineContextFactory<CHILD_CONTEXT> factory = configuration.stateMachineContextFactory();
        CHILD_CONTEXT context = (factory != null) ? factory.newInstance() : null;

        if (context == null) {
            throw new IllegalStateException("Local execution failed: Context is null and no factory configured");
        }

        InputFunction<CHILD_CONTEXT, WORKLOAD_INPUT> inputFn = configuration.inputFunction();
        if (inputFn != null) {
            context = inputFn.apply(context, input);
        }

        ExecutionEventListener eventListener = configuration.eventListener();
        UUID childExecId = (eventListener != null) ? UUID.randomUUID() : null;
        long turnStartNanos = (eventListener != null) ? System.nanoTime() : 0L;

        if (eventListener != null) {
            safeNotify(eventListener, new TurnStartedEvent(
                    childExecId,
                    configuration.getMachineName(),
                    metadata.correlationId(),
                    Instant.now()
            ));
        }

        CHILD_STATE_KEY currentStateKey = stateMap.getInitialState();
        int totalTransitions = 0;
        int maxTransitions = configuration.getMaxTransitions();

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
                            configuration.getMachineName(),
                            currentStateKey.name(),
                            Instant.now()
                    ));
                }
                long stateStartNanos = (eventListener != null) ? System.nanoTime() : 0L;

                context = state.action().execute(context);

                if (eventListener != null) {
                    safeNotify(eventListener, new StateExitedEvent(
                            childExecId,
                            configuration.getMachineName(),
                            currentStateKey.name(),
                            Duration.ofNanos(System.nanoTime() - stateStartNanos),
                            Instant.now()
                    ));
                }

                CHILD_STATE_KEY nextStateKey = state.transition().nextState(context);
                if (nextStateKey == null) {
                    break;
                }

                if (eventListener != null) {
                    safeNotify(eventListener, new TransitionEvaluatedEvent(
                            childExecId,
                            configuration.getMachineName(),
                            currentStateKey.name(),
                            nextStateKey.name(),
                            Instant.now()
                    ));
                }

                currentStateKey = nextStateKey;
                totalTransitions++;
            }

            OutputFunction<CHILD_CONTEXT, WORKLOAD_OUTPUT> outputFn = configuration.outputFunction();
            WORKLOAD_OUTPUT result = (outputFn != null) ? outputFn.apply(context) : null;

            if (eventListener != null) {
                safeNotify(eventListener, new TurnCompletedEvent(
                        childExecId,
                        configuration.getMachineName(),
                        (currentStateKey != null) ? currentStateKey.name() : "COMPLETED",
                        metadata.correlationId(),
                        Duration.ofNanos(System.nanoTime() - turnStartNanos),
                        Instant.now()
                ));
            }

            return result;

        } catch (Throwable t) {
            log.atWarn()
                    .setCause(t)
                    .addKeyValue("endpoint_id", endpointId)
                    .addKeyValue("service_name", metadata.serviceName())
                    .addKeyValue("correlation_id", metadata.correlationId())
                    .log("Local FSM execution encountered error");

            if (t instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException(t);
        }
    }

    private static void safeNotify(ExecutionEventListener listener, ExecutionEvent event) {
        try {
            listener.onEvent(event);
        } catch (Throwable t) {
            log.atWarn().setCause(t).log("Failed to deliver execution event to listener");
        }
    }
}
