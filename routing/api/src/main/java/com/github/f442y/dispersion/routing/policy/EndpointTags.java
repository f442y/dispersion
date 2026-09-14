package com.github.f442y.dispersion.routing.policy;

import org.jspecify.annotations.NonNull;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record EndpointTags(@NonNull Map<String, String> tags) {

    private static final EndpointTags EMPTY = new EndpointTags(Collections.emptyMap());

    public EndpointTags {
        Objects.requireNonNull(tags, "tags must not be null");
        tags = Map.copyOf(tags);
    }

    @NonNull
    public static EndpointTags empty() {
        return EMPTY;
    }

    @NonNull
    public static EndpointTags of(@NonNull String key, @NonNull String value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        return new EndpointTags(Map.of(key, value));
    }

    @NonNull
    public static EndpointTags of(
            @NonNull String key1, @NonNull String value1,
            @NonNull String key2, @NonNull String value2
    ) {
        return new EndpointTags(Map.of(key1, value1, key2, value2));
    }

    @NonNull
    public static EndpointTags of(@NonNull Map<String, String> tags) {
        return new EndpointTags(tags);
    }

    @NonNull
    public Optional<String> get(@NonNull String key) {
        Objects.requireNonNull(key, "key must not be null");
        return Optional.ofNullable(tags.get(key));
    }

    public boolean hasTag(@NonNull String key, @NonNull String value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        return value.equals(tags.get(key));
    }

    public boolean containsAll(@NonNull Map<String, String> requiredTags) {
        Objects.requireNonNull(requiredTags, "requiredTags must not be null");
        for (Map.Entry<String, String> entry : requiredTags.entrySet()) {
            if (!entry.getValue().equals(tags.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
