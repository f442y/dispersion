package com.github.f442y.dispersion.routing.core;

import com.github.f442y.dispersion.routing.InspectableRouter;
import com.github.f442y.dispersion.routing.WorkloadRouter;
import com.github.f442y.dispersion.routing.backpressure.BackpressureStrategy;
import com.github.f442y.dispersion.routing.backpressure.CapacityExceededException;
import com.github.f442y.dispersion.routing.backpressure.NoMatchingEndpointException;
import com.github.f442y.dispersion.routing.core.policy.LocalFirstRoutingPolicy;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import com.github.f442y.dispersion.routing.policy.FallbackPolicy;
import com.github.f442y.dispersion.routing.policy.RoutingPolicy;
import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DefaultWorkloadRouter implements WorkloadRouter, InspectableRouter {

    private static final Logger log = LoggerFactory.getLogger(DefaultWorkloadRouter.class);

    private final Map<String, List<WorkloadEndpoint<?, ?>>> endpointsByService = new ConcurrentHashMap<>();
    private final Map<String, RoutingPolicy> servicePolicies = new ConcurrentHashMap<>();
    private volatile RoutingPolicy defaultPolicy = LocalFirstRoutingPolicy.getInstance();
    private volatile BackpressureStrategy backpressureStrategy = BackpressureStrategy.FAIL_FAST;

    @Override
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> void registerEndpoint(
            @NonNull String serviceName,
            @NonNull WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint
    ) {
        registerEndpoint(serviceName, endpoint, endpoint.tags());
    }

    @Override
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> void registerEndpoint(
            @NonNull String serviceName,
            @NonNull WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint,
            @NonNull EndpointTags tags
    ) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(tags, "tags must not be null");

        List<WorkloadEndpoint<?, ?>> list = endpointsByService.computeIfAbsent(serviceName, k -> new CopyOnWriteArrayList<>());
        list.removeIf((WorkloadEndpoint<?, ?> existing) -> existing.endpointId().equals(endpoint.endpointId()));
        list.add(endpoint);

        log.atInfo()
                .addKeyValue("service_name", serviceName)
                .addKeyValue("endpoint_id", endpoint.endpointId())
                .addKeyValue("endpoint_type", endpoint.endpointType())
                .addKeyValue("tags_count", tags.tags().size())
                .log("Registered workload endpoint");
    }

    @Override
    public void unregisterEndpoint(@NonNull String serviceName, @NonNull String endpointId) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(endpointId, "endpointId must not be null");

        List<WorkloadEndpoint<?, ?>> list = endpointsByService.get(serviceName);
        if (list != null) {
            list.removeIf((WorkloadEndpoint<?, ?> existing) -> existing.endpointId().equals(endpointId));
            log.atInfo()
                    .addKeyValue("service_name", serviceName)
                    .addKeyValue("endpoint_id", endpointId)
                    .log("Unregistered workload endpoint");
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
        servicePolicies.put(serviceName, policy);
    }

    @Override
    public void setBackpressureStrategy(@NonNull BackpressureStrategy strategy) {
        this.backpressureStrategy = Objects.requireNonNull(strategy, "strategy must not be null");
    }

    @Override
    @NonNull
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

        Instant deadline = Instant.now().plus(timeout);

        while (true) {
            WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint = selectCandidate(serviceName, metadata);
            if (endpoint != null) {
                log.atDebug()
                        .addKeyValue("service_name", serviceName)
                        .addKeyValue("endpoint_id", endpoint.endpointId())
                        .addKeyValue("correlation_id", metadata.correlationId())
                        .log("Routing workload to selected endpoint");
                return endpoint.executeSync(input, metadata, timeout);
            }

            if (backpressureStrategy == BackpressureStrategy.FAIL_FAST) {
                int inFlight = currentInFlightForService(serviceName);
                int maxCap = maxCapacityForService(serviceName);
                log.atWarn()
                        .addKeyValue("service_name", serviceName)
                        .addKeyValue("in_flight", inFlight)
                        .addKeyValue("max_capacity", maxCap)
                        .log("Backpressure limit reached; rejecting workload");
                throw new CapacityExceededException(
                        serviceName,
                        inFlight,
                        maxCap,
                        "Capacity exceeded for service [" + serviceName + "]: All endpoints saturated"
                );
            }

            if (Instant.now().isAfter(deadline)) {
                throw new CapacityExceededException(
                        serviceName,
                        currentInFlightForService(serviceName),
                        maxCapacityForService(serviceName),
                        "Timed out waiting for available capacity on service [" + serviceName + "]"
                );
            }

            Thread.sleep(Duration.ofMillis(10));
        }
    }

    @Override
    @NonNull
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> CompletableFuture<WORKLOAD_OUTPUT> routeAsync(
            @NonNull String serviceName,
            @NonNull WORKLOAD_INPUT input,
            @NonNull WorkloadMetadata metadata
    ) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(input, "input must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");

        WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint;
        try {
            endpoint = selectCandidate(serviceName, metadata);
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }

        if (endpoint == null) {
            int inFlight = currentInFlightForService(serviceName);
            int maxCap = maxCapacityForService(serviceName);
            return CompletableFuture.failedFuture(new CapacityExceededException(
                    serviceName,
                    inFlight,
                    maxCap,
                    "Capacity exceeded for service [" + serviceName + "]: No endpoint available"
            ));
        }

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
        return List.copyOf(list);
    }

    @SuppressWarnings("unchecked")
    private <WORKLOAD_INPUT, WORKLOAD_OUTPUT> WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> selectCandidate(
            String serviceName,
            WorkloadMetadata metadata
    ) {
        List<WorkloadEndpoint<?, ?>> registered = endpointsByService.get(serviceName);
        if (registered == null || registered.isEmpty()) {
            throw new NoMatchingEndpointException(
                    serviceName,
                    metadata.selector(),
                    "No endpoints registered for service [" + serviceName + "]"
            );
        }

        List<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> typedCandidates = new ArrayList<>(registered.size());
        for (WorkloadEndpoint<?, ?> raw : registered) {
            typedCandidates.add((WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>) raw);
        }

        RoutingSelector selector = metadata.selector();
        List<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> matching = typedCandidates.stream()
                .filter((WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> ep) -> selector.matches(ep.tags()))
                .toList();

        if (matching.isEmpty()) {
            if (selector.fallbackPolicy() == FallbackPolicy.STRICT && selector.hasTagRequirements()) {
                throw new NoMatchingEndpointException(
                        serviceName,
                        selector,
                        "No endpoints matching required tags for service [" + serviceName + "]"
                );
            }
            matching = typedCandidates;
        }

        RoutingPolicy policy = servicePolicies.getOrDefault(serviceName, defaultPolicy);
        Optional<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> selected = policy.selectEndpoint(serviceName, matching, metadata);
        return selected.orElse(null);
    }

    private int currentInFlightForService(String serviceName) {
        List<WorkloadEndpoint<?, ?>> list = endpointsByService.get(serviceName);
        if (list == null) {
            return 0;
        }
        int total = 0;
        for (WorkloadEndpoint<?, ?> ep : list) {
            total += ep.activeWorkloads();
        }
        return total;
    }

    private int maxCapacityForService(String serviceName) {
        List<WorkloadEndpoint<?, ?>> list = endpointsByService.get(serviceName);
        if (list == null) {
            return 0;
        }
        int total = 0;
        for (WorkloadEndpoint<?, ?> ep : list) {
            total += ep.maxConcurrency();
        }
        return total;
    }

    @Override
    @NonNull
    public Set<String> registeredServices() {
        return Collections.unmodifiableSet(endpointsByService.keySet());
    }

    @Override
    public int totalActiveWorkloads() {
        int total = 0;
        for (List<WorkloadEndpoint<?, ?>> list : endpointsByService.values()) {
            for (WorkloadEndpoint<?, ?> ep : list) {
                total += ep.activeWorkloads();
            }
        }
        return total;
    }

    @Override
    public int totalRegisteredEndpoints() {
        int total = 0;
        for (List<WorkloadEndpoint<?, ?>> list : endpointsByService.values()) {
            total += list.size();
        }
        return total;
    }

    @Override
    @NonNull
    public List<EndpointDescriptor> listEndpointDescriptors(@NonNull String serviceName) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        List<WorkloadEndpoint<?, ?>> list = endpointsByService.get(serviceName);
        if (list == null) {
            return Collections.emptyList();
        }
        List<EndpointDescriptor> descriptors = new ArrayList<>(list.size());
        for (WorkloadEndpoint<?, ?> ep : list) {
            descriptors.add(new EndpointDescriptor(
                    ep.endpointId(),
                    serviceName,
                    ep.endpointType(),
                    ep.status(),
                    ep.tags(),
                    ep.activeWorkloads(),
                    ep.maxConcurrency()
            ));
        }
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    @NonNull
    public Map<String, List<EndpointDescriptor>> allEndpoints() {
        Map<String, List<EndpointDescriptor>> map = new HashMap<>();
        for (String serviceName : endpointsByService.keySet()) {
            map.put(serviceName, listEndpointDescriptors(serviceName));
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public void close() throws Exception {
        for (List<WorkloadEndpoint<?, ?>> list : endpointsByService.values()) {
            for (WorkloadEndpoint<?, ?> ep : list) {
                ep.close();
            }
        }
        endpointsByService.clear();
        servicePolicies.clear();
    }
}
