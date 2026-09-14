package com.github.f442y.dispersion.routing.core.policy;

import com.github.f442y.dispersion.routing.endpoint.EndpointStatus;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.RoutingPolicy;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public final class RoundRobinRoutingPolicy implements RoutingPolicy {

    private final AtomicInteger counter = new AtomicInteger(0);

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

        List<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> available = candidateEndpoints.stream()
                .filter((WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint) ->
                        endpoint.status() == EndpointStatus.HEALTHY && endpoint.canAccept())
                .toList();

        if (available.isEmpty()) {
            return Optional.empty();
        }

        int index = Math.floorMod(counter.getAndIncrement(), available.size());
        return Optional.of(available.get(index));
    }
}
