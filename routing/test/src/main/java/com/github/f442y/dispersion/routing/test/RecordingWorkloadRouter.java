package com.github.f442y.dispersion.routing.test;

import com.github.f442y.dispersion.routing.WorkloadRouter;
import com.github.f442y.dispersion.routing.backpressure.BackpressureStrategy;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import com.github.f442y.dispersion.routing.policy.RoutingPolicy;
import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public final class RecordingWorkloadRouter implements WorkloadRouter {

    public record RoutedCall(
            @NonNull String serviceName,
            @NonNull Object input,
            @NonNull WorkloadMetadata metadata
    ) {
        public RoutedCall {
            Objects.requireNonNull(serviceName, "serviceName must not be null");
            Objects.requireNonNull(input, "input must not be null");
            Objects.requireNonNull(metadata, "metadata must not be null");
        }
    }

    private final Map<String, List<WorkloadEndpoint<?, ?>>> endpointsByService = new ConcurrentHashMap<>();
    private final List<RoutedCall> routedCalls = new CopyOnWriteArrayList<>();
    private volatile RoutingPolicy defaultPolicy;
    private volatile BackpressureStrategy backpressureStrategy;

    @NonNull
    public List<RoutedCall> routedCalls() {
        return Collections.unmodifiableList(new ArrayList<>(routedCalls));
    }

    @Override
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> void registerEndpoint(
            @NonNull String serviceName,
            @NonNull WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint
    ) {
        registerEndpoint(serviceName, endpoint, EndpointTags.empty());
    }

    @Override
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> void registerEndpoint(
            @NonNull String serviceName,
            @NonNull WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint,
            @NonNull EndpointTags tags
    ) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        endpointsByService.computeIfAbsent(serviceName, k -> new CopyOnWriteArrayList<>()).add(endpoint);
    }

    @Override
    public void unregisterEndpoint(@NonNull String serviceName, @NonNull String endpointId) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(endpointId, "endpointId must not be null");
        List<WorkloadEndpoint<?, ?>> list = endpointsByService.get(serviceName);
        if (list != null) {
            list.removeIf(ep -> ep.endpointId().equals(endpointId));
        }
    }

    @Override
    public void setDefaultPolicy(@NonNull RoutingPolicy policy) {
        this.defaultPolicy = Objects.requireNonNull(policy, "policy must not be null");
    }

    @Override
    public void setServicePolicy(@NonNull String serviceName, @NonNull RoutingPolicy policy) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(policy, "policy must not be null");
    }

    @Override
    public void setBackpressureStrategy(@NonNull BackpressureStrategy strategy) {
        this.backpressureStrategy = Objects.requireNonNull(strategy, "strategy must not be null");
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> WORKLOAD_OUTPUT routeSync(
            @NonNull String serviceName,
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata,
            @NonNull Duration timeout
    ) throws Exception {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(input, "input must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");

        routedCalls.add(new RoutedCall(serviceName, input, metadata));

        List<WorkloadEndpoint<?, ?>> list = endpointsByService.get(serviceName);
        if (list == null || list.isEmpty()) {
            throw new NoSuchElementException("No endpoints registered for: " + serviceName);
        }

        WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint =
                (WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>) list.getFirst();

        return endpoint.executeSync(input, metadata, timeout);
    }

    @Override
    @NonNull
    @SuppressWarnings("unchecked")
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> CompletableFuture<WORKLOAD_OUTPUT> routeAsync(
            @NonNull String serviceName,
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata
    ) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(input, "input must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");

        routedCalls.add(new RoutedCall(serviceName, input, metadata));

        List<WorkloadEndpoint<?, ?>> list = endpointsByService.get(serviceName);
        if (list == null || list.isEmpty()) {
            return CompletableFuture.failedFuture(new NoSuchElementException("No endpoints registered for: " + serviceName));
        }

        WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint =
                (WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>) list.getFirst();

        return endpoint.executeAsync(input, metadata);
    }

    @Override
    @NonNull
    public List<WorkloadEndpoint<?, ?>> listEndpoints(@NonNull String serviceName) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        List<WorkloadEndpoint<?, ?>> list = endpointsByService.get(serviceName);
        if (list == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(list));
    }

    @Override
    public void close() throws Exception {
        for (List<WorkloadEndpoint<?, ?>> endpoints : endpointsByService.values()) {
            for (WorkloadEndpoint<?, ?> ep : endpoints) {
                ep.close();
            }
        }
        endpointsByService.clear();
        routedCalls.clear();
    }
}
