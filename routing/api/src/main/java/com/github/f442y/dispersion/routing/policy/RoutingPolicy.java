package com.github.f442y.dispersion.routing.policy;

import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Optional;

@FunctionalInterface
public interface RoutingPolicy {

    <WORKLOAD_INPUT, WORKLOAD_OUTPUT> Optional<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> selectEndpoint(
            @NonNull String serviceName,
            @NonNull List<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> candidateEndpoints,
            @NonNull WorkloadMetadata metadata
    );
}
