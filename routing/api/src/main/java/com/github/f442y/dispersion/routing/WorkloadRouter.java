package com.github.f442y.dispersion.routing;

import com.github.f442y.dispersion.routing.backpressure.BackpressureStrategy;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import com.github.f442y.dispersion.routing.policy.RoutingPolicy;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface WorkloadRouter extends AutoCloseable {

    <WORKLOAD_INPUT, WORKLOAD_OUTPUT> void registerEndpoint(
            @NonNull String serviceName,
            @NonNull WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint
    );

    <WORKLOAD_INPUT, WORKLOAD_OUTPUT> void registerEndpoint(
            @NonNull String serviceName,
            @NonNull WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint,
            @NonNull EndpointTags tags
    );

    void unregisterEndpoint(@NonNull String serviceName, @NonNull String endpointId);

    void setDefaultPolicy(@NonNull RoutingPolicy policy);

    void setServicePolicy(@NonNull String serviceName, @NonNull RoutingPolicy policy);

    void setBackpressureStrategy(@NonNull BackpressureStrategy strategy);

    <WORKLOAD_INPUT, WORKLOAD_OUTPUT> WORKLOAD_OUTPUT routeSync(
            @NonNull String serviceName,
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata,
            @NonNull Duration timeout
    ) throws Exception;

    <WORKLOAD_INPUT, WORKLOAD_OUTPUT> CompletableFuture<WORKLOAD_OUTPUT> routeAsync(
            @NonNull String serviceName,
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata
    );

    @NonNull
    List<WorkloadEndpoint<?, ?>> listEndpoints(@NonNull String serviceName);

    @Override
    default void close() throws Exception {}
}
