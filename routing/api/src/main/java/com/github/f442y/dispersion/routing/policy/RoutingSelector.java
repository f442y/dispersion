package com.github.f442y.dispersion.routing.policy;

import org.jspecify.annotations.NonNull;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public record RoutingSelector(
        @NonNull Map<String, String> requiredTags,
        @NonNull FallbackPolicy fallbackPolicy,
        @NonNull Predicate<EndpointTags> customPredicate
) {

    private static final RoutingSelector ANY = new RoutingSelector(
            Collections.emptyMap(),
            FallbackPolicy.STRICT,
            (EndpointTags tags) -> true
    );

    public RoutingSelector {
        Objects.requireNonNull(requiredTags, "requiredTags must not be null");
        Objects.requireNonNull(fallbackPolicy, "fallbackPolicy must not be null");
        Objects.requireNonNull(customPredicate, "customPredicate must not be null");
        requiredTags = Map.copyOf(requiredTags);
    }

    @NonNull
    public static RoutingSelector any() {
        return ANY;
    }

    @NonNull
    public static RoutingSelector requireTag(@NonNull String key, @NonNull String value) {
        return requireTag(key, value, FallbackPolicy.STRICT);
    }

    @NonNull
    public static RoutingSelector requireTag(
            @NonNull String key,
            @NonNull String value,
            @NonNull FallbackPolicy fallbackPolicy
    ) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        Objects.requireNonNull(fallbackPolicy, "fallbackPolicy must not be null");
        return new RoutingSelector(Map.of(key, value), fallbackPolicy, (EndpointTags tags) -> true);
    }

    @NonNull
    public static RoutingSelector requireTags(@NonNull Map<String, String> requiredTags) {
        return requireTags(requiredTags, FallbackPolicy.STRICT);
    }

    @NonNull
    public static RoutingSelector requireTags(
            @NonNull Map<String, String> requiredTags,
            @NonNull FallbackPolicy fallbackPolicy
    ) {
        Objects.requireNonNull(requiredTags, "requiredTags must not be null");
        Objects.requireNonNull(fallbackPolicy, "fallbackPolicy must not be null");
        return new RoutingSelector(requiredTags, fallbackPolicy, (EndpointTags tags) -> true);
    }

    public boolean matches(@NonNull EndpointTags tags) {
        Objects.requireNonNull(tags, "tags must not be null");
        return tags.containsAll(requiredTags) && customPredicate.test(tags);
    }

    public boolean hasTagRequirements() {
        return !requiredTags.isEmpty();
    }
}
