package com.github.f442y.dispersion.routing.test;

import com.github.f442y.dispersion.routing.endpoint.EndpointStatus;
import com.github.f442y.dispersion.routing.endpoint.EndpointType;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public final class FakeWorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> implements WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> {

    private final String endpointId;
    private final EndpointType endpointType;
    private final EndpointTags tags;
    private final int maxConcurrency;
    private final AtomicInteger activeWorkloads = new AtomicInteger(0);
    private final List<WORKLOAD_INPUT> receivedInputs = new CopyOnWriteArrayList<>();
    private final List<WorkloadMetadata> receivedMetadata = new CopyOnWriteArrayList<>();
    private volatile EndpointStatus status = EndpointStatus.HEALTHY;
    private volatile Function<WORKLOAD_INPUT, WORKLOAD_OUTPUT> handler;
    private volatile Duration artificialDelay = Duration.ZERO;
    private volatile Throwable simulatedFailure = null;

    public FakeWorkloadEndpoint(
            @NonNull String endpointId,
            @NonNull EndpointType endpointType,
            @NonNull EndpointTags tags,
            int maxConcurrency,
            @NonNull Function<WORKLOAD_INPUT, WORKLOAD_OUTPUT> handler
    ) {
        this.endpointId = Objects.requireNonNull(endpointId, "endpointId must not be null");
        this.endpointType = Objects.requireNonNull(endpointType, "endpointType must not be null");
        this.tags = Objects.requireNonNull(tags, "tags must not be null");
        this.maxConcurrency = maxConcurrency;
        this.handler = Objects.requireNonNull(handler, "handler must not be null");
    }

    @NonNull
    public static <WORKLOAD_INPUT, WORKLOAD_OUTPUT> FakeWorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> of(
            @NonNull String endpointId,
            @NonNull Function<WORKLOAD_INPUT, WORKLOAD_OUTPUT> handler
    ) {
        return new FakeWorkloadEndpoint<>(endpointId, EndpointType.LOCAL, EndpointTags.empty(), 100, handler);
    }

    @NonNull
    public static <WORKLOAD_INPUT, WORKLOAD_OUTPUT> FakeWorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> of(
            @NonNull String endpointId,
            @NonNull EndpointTags tags,
            @NonNull Function<WORKLOAD_INPUT, WORKLOAD_OUTPUT> handler
    ) {
        return new FakeWorkloadEndpoint<>(endpointId, EndpointType.LOCAL, tags, 100, handler);
    }

    public void setHandler(@NonNull Function<WORKLOAD_INPUT, WORKLOAD_OUTPUT> handler) {
        this.handler = Objects.requireNonNull(handler, "handler must not be null");
    }

    public void setStatus(@NonNull EndpointStatus status) {
        this.status = Objects.requireNonNull(status, "status must not be null");
    }

    public void setArtificialDelay(@NonNull Duration delay) {
        this.artificialDelay = Objects.requireNonNull(delay, "delay must not be null");
    }

    public void setSimulatedFailure(@Nullable Throwable failure) {
        this.simulatedFailure = failure;
    }

    @NonNull
    public List<WORKLOAD_INPUT> receivedInputs() {
        return Collections.unmodifiableList(new ArrayList<>(receivedInputs));
    }

    @NonNull
    public List<WorkloadMetadata> receivedMetadata() {
        return Collections.unmodifiableList(new ArrayList<>(receivedMetadata));
    }

    @Override
    @NonNull
    public String endpointId() {
        return endpointId;
    }

    @Override
    @NonNull
    public EndpointType endpointType() {
        return endpointType;
    }

    @Override
    @NonNull
    public EndpointStatus status() {
        return status;
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
        receivedInputs.add(input);
        receivedMetadata.add(metadata);

        activeWorkloads.incrementAndGet();
        try {
            if (!artificialDelay.isZero()) {
                Thread.sleep(artificialDelay);
            }

            if (simulatedFailure != null) {
                if (simulatedFailure instanceof Exception ex) {
                    throw ex;
                }
                throw new RuntimeException(simulatedFailure);
            }

            return handler.apply(input);
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
        receivedInputs.add(input);
        receivedMetadata.add(metadata);

        activeWorkloads.incrementAndGet();
        CompletableFuture<WORKLOAD_OUTPUT> future = new CompletableFuture<>();

        Thread.ofVirtual().start(() -> {
            try {
                if (!artificialDelay.isZero()) {
                    Thread.sleep(artificialDelay);
                }
                if (simulatedFailure != null) {
                    future.completeExceptionally(simulatedFailure);
                } else {
                    future.complete(handler.apply(input));
                }
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                activeWorkloads.decrementAndGet();
            }
        });

        return future;
    }
}
