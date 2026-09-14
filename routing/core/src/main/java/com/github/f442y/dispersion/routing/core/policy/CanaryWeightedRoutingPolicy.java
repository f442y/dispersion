package com.github.f442y.dispersion.routing.core.policy;

import com.github.f442y.dispersion.routing.endpoint.EndpointStatus;
import com.github.f442y.dispersion.routing.endpoint.WorkloadEndpoint;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import com.github.f442y.dispersion.routing.policy.RoutingPolicy;
import org.jspecify.annotations.NonNull;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public final class CanaryWeightedRoutingPolicy implements RoutingPolicy {

    public record CanaryRule(
            @NonNull EndpointTags requiredTags,
            int weight
    ) {
        public CanaryRule {
            Objects.requireNonNull(requiredTags, "requiredTags must not be null");
            if (weight <= 0) {
                throw new IllegalArgumentException("Canary weight must be positive: " + weight);
            }
        }
    }

    private final List<CanaryRule> rules;
    private final int totalWeight;

    public CanaryWeightedRoutingPolicy(@NonNull List<CanaryRule> rules) {
        Objects.requireNonNull(rules, "rules must not be null");
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("CanaryWeightedRoutingPolicy requires at least one rule");
        }
        this.rules = List.copyOf(rules);
        int sum = 0;
        for (CanaryRule rule : this.rules) {
            sum += rule.weight();
        }
        this.totalWeight = sum;
    }

    @NonNull
    public static CanaryWeightedRoutingPolicy of(
            @NonNull EndpointTags canaryTags, int canaryWeight,
            @NonNull EndpointTags stableTags, int stableWeight
    ) {
        return new CanaryWeightedRoutingPolicy(List.of(
                new CanaryRule(canaryTags, canaryWeight),
                new CanaryRule(stableTags, stableWeight)
        ));
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

        List<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> available = candidateEndpoints.stream()
                .filter((WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint) ->
                        endpoint.status() == EndpointStatus.HEALTHY && endpoint.canAccept())
                .toList();

        if (available.isEmpty()) {
            return Optional.empty();
        }

        int roll = ThreadLocalRandom.current().nextInt(totalWeight);
        int current = 0;
        CanaryRule chosenRule = rules.getFirst();
        for (CanaryRule rule : rules) {
            current += rule.weight();
            if (roll < current) {
                chosenRule = rule;
                break;
            }
        }

        final CanaryRule targetRule = chosenRule;
        List<WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT>> matched = available.stream()
                .filter((WorkloadEndpoint<WORKLOAD_INPUT, WORKLOAD_OUTPUT> endpoint) ->
                        endpoint.tags().containsAll(targetRule.requiredTags().tags()))
                .toList();

        if (!matched.isEmpty()) {
            return matched.stream().min(Comparator.comparingInt(WorkloadEndpoint::activeWorkloads));
        }

        return available.stream().min(Comparator.comparingInt(WorkloadEndpoint::activeWorkloads));
    }
}
