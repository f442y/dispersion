package com.github.f442y.dispersion.routing.core.policy;

import com.github.f442y.dispersion.routing.endpoint.EndpointStatus;
import com.github.f442y.dispersion.routing.endpoint.EndpointType;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.RoutingPolicy;
import org.jspecify.annotations.NonNull;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class LocalFirstRoutingPolicy implements RoutingPolicy {

    private static final LocalFirstRoutingPolicy INSTANCE = new LocalFirstRoutingPolicy();

    public static LocalFirstRoutingPolicy getInstance() {
        return INSTANCE;
    }

    @Override
    @NonNull
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> Optional<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> selectEndpoint(
            @NonNull String serviceName,
            @NonNull List<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> candidateEndpoints,
            @NonNull WorkloadMetadata metadata
    ) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(candidateEndpoints, "candidateEndpoints must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");

        for (WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint : candidateEndpoints) {
            if (endpoint.endpointType() == EndpointType.LOCAL
                    && endpoint.status() == EndpointStatus.HEALTHY
                    && endpoint.canAccept()) {
                return Optional.of(endpoint);
            }
        }

        return candidateEndpoints.stream()
                .filter((WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint) ->
                        endpoint.status() == EndpointStatus.HEALTHY && endpoint.canAccept())
                .min(Comparator.comparingInt(WorkloadEndpoint::activeWorkloads));
    }
}
