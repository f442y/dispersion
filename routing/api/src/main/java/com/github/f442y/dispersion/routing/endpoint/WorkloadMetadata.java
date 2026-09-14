package com.github.f442y.dispersion.routing.endpoint;

import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public record WorkloadMetadata(
        @NonNull String correlationId,
        @NonNull String serviceName,
        @NonNull Instant createdAt,
        @Nullable String replyDestination,
        @NonNull Map<String, String> headers,
        @NonNull RoutingSelector selector
) {
    public WorkloadMetadata {
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(headers, "headers must not be null");
        Objects.requireNonNull(selector, "selector must not be null");
        headers = Map.copyOf(headers);
    }

    @NonNull
    public static Builder builder(@NonNull String serviceName, @NonNull String correlationId) {
        return new Builder(serviceName, correlationId);
    }

    public static final class Builder {
        private final String serviceName;
        private final String correlationId;
        private Instant createdAt = Instant.now();
        private String replyDestination;
        private final Map<String, String> headers = new HashMap<>();
        private RoutingSelector selector = RoutingSelector.any();

        private Builder(@NonNull String serviceName, @NonNull String correlationId) {
            this.serviceName = Objects.requireNonNull(serviceName, "serviceName must not be null");
            this.correlationId = Objects.requireNonNull(correlationId, "correlationId must not be null");
        }

        @NonNull
        public Builder createdAt(@NonNull Instant createdAt) {
            this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
            return this;
        }

        @NonNull
        public Builder replyDestination(@Nullable String replyDestination) {
            this.replyDestination = replyDestination;
            return this;
        }

        @NonNull
        public Builder header(@NonNull String key, @NonNull String value) {
            this.headers.put(
                    Objects.requireNonNull(key, "key must not be null"),
                    Objects.requireNonNull(value, "value must not be null")
            );
            return this;
        }

        @NonNull
        public Builder headers(@NonNull Map<String, String> headers) {
            this.headers.putAll(Objects.requireNonNull(headers, "headers must not be null"));
            return this;
        }

        @NonNull
        public Builder selector(@NonNull RoutingSelector selector) {
            this.selector = Objects.requireNonNull(selector, "selector must not be null");
            return this;
        }

        @NonNull
        public WorkloadMetadata build() {
            return new WorkloadMetadata(
                    correlationId,
                    serviceName,
                    createdAt,
                    replyDestination,
                    Collections.unmodifiableMap(new HashMap<>(headers)),
                    selector
            );
        }
    }
}
