package com.github.f442y.dispersion.routing.endpoint;

import com.github.f442y.dispersion.routing.policy.EndpointTags;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> extends AutoCloseable {

    @NonNull
    String endpointId();

    @NonNull
    EndpointType endpointType();

    @NonNull
    EndpointStatus status();

    @NonNull
    EndpointTags tags();

    int activeWorkloads();

    int maxConcurrency();

    boolean canAccept();

    @NonNull
    WORKLOAD_OUTPUT executeSync(
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata,
            @NonNull Duration timeout
    ) throws Exception;

    @NonNull
    CompletableFuture<WORKLOAD_OUTPUT> executeAsync(
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata
    );

    @Override
    default void close() throws Exception {}
}
