package com.github.f442y.dispersion.orchestration;

import org.jspecify.annotations.NonNull;

import java.util.Map;
import java.util.Objects;

/**
 * Criteria for matching workload execution targets across worker pools.
 * Decoupled from concrete routing engine implementations.
 */
public record WorkloadSelector(
        @NonNull Map<String, String> requiredTags,
        @NonNull Map<String, String> preferredTags,
        @NonNull Map<String, String> requiredAttributes
) {
    private static final WorkloadSelector ANY = new WorkloadSelector(Map.of(), Map.of(), Map.of());

    public WorkloadSelector {
        requiredTags = Map.copyOf(Objects.requireNonNull(requiredTags, "requiredTags must not be null"));
        preferredTags = Map.copyOf(Objects.requireNonNull(preferredTags, "preferredTags must not be null"));
        requiredAttributes = Map.copyOf(Objects.requireNonNull(requiredAttributes, "requiredAttributes must not be null"));
    }

    public static WorkloadSelector any() {
        return ANY;
    }

    public static WorkloadSelector requireTag(String key, String value) {
        return new WorkloadSelector(Map.of(key, value), Map.of(), Map.of());
    }

    public static WorkloadSelector requireTags(Map<String, String> tags) {
        return new WorkloadSelector(tags, Map.of(), Map.of());
    }

    public static WorkloadSelector of(
            @NonNull Map<String, String> requiredTags,
            @NonNull Map<String, String> preferredTags,
            @NonNull Map<String, String> requiredAttributes
    ) {
        return new WorkloadSelector(requiredTags, preferredTags, requiredAttributes);
    }
}
