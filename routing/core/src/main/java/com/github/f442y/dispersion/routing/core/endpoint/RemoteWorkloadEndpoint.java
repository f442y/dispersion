package com.github.f442y.dispersion.routing.core.endpoint;

import com.github.f442y.dispersion.routing.transport.ChannelTransport;
import com.github.f442y.dispersion.routing.endpoint.EndpointStatus;
import com.github.f442y.dispersion.routing.endpoint.EndpointType;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public final class RemoteWorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> implements WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> {

    private final String endpointId;
    private final EndpointType endpointType;
    private final String requestDestination;
    private final String replyDestination;
    private final ChannelTransport transport;
    private final EndpointTags tags;
    private final int maxConcurrency;
    private final AtomicInteger activeWorkloads = new AtomicInteger(0);
    private volatile EndpointStatus status = EndpointStatus.HEALTHY;

    public RemoteWorkloadEndpoint(
            @NonNull String endpointId,
            @NonNull EndpointType endpointType,
            @NonNull String requestDestination,
            @NonNull String replyDestination,
            @NonNull ChannelTransport transport,
            @NonNull EndpointTags tags,
            int maxConcurrency
    ) {
        this.endpointId = Objects.requireNonNull(endpointId, "endpointId must not be null");
        this.endpointType = Objects.requireNonNull(endpointType, "endpointType must not be null");
        this.requestDestination = Objects.requireNonNull(requestDestination, "requestDestination must not be null");
        this.replyDestination = Objects.requireNonNull(replyDestination, "replyDestination must not be null");
        this.transport = Objects.requireNonNull(transport, "transport must not be null");
        this.tags = Objects.requireNonNull(tags, "tags must not be null");
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive: " + maxConcurrency);
        }
        this.maxConcurrency = maxConcurrency;
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
        CompletableFuture<WORKLOAD_OUTPUT> future = executeAsync(input, metadata);
        try {
            return future.get();
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof Exception exc) {
                throw exc;
            }
            throw new RuntimeException(cause);
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

        activeWorkloads.incrementAndGet();

        WorkloadMetadata enrichedMetadata = WorkloadMetadata.builder(metadata.serviceName(), metadata.correlationId())
                .createdAt(metadata.createdAt())
                .replyDestination(replyDestination)
                .headers(metadata.headers())
                .selector(metadata.selector())
                .build();

        WorkloadEnvelope<WORKLOAD_INPUT> envelope = WorkloadEnvelope.request(enrichedMetadata, input);

        CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> replyFuture = transport.requestReply(
                requestDestination,
                replyDestination,
                envelope,
                Duration.ofSeconds(30)
        );

        return replyFuture
                .whenComplete((res, err) -> activeWorkloads.decrementAndGet())
                .thenApply((WorkloadEnvelope<WORKLOAD_OUTPUT> responseEnv) -> {
                    if (responseEnv.isFailure()) {
                        throw new IllegalStateException("Remote execution failed: " + responseEnv.error());
                    }
                    return responseEnv.payload();
                });
    }
}
