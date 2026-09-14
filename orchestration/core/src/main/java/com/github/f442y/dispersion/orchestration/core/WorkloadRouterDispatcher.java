package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.orchestration.WorkloadDispatcher;
import com.github.f442y.dispersion.orchestration.WorkloadSelector;
import com.github.f442y.dispersion.routing.WorkloadRouter;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Default adapter bridging {@link WorkloadDispatcher} to a Dispersion {@link WorkloadRouter}.
 */
public final class WorkloadRouterDispatcher implements WorkloadDispatcher {

    private final WorkloadRouter router;

    public WorkloadRouterDispatcher(@NonNull WorkloadRouter router) {
        this.router = Objects.requireNonNull(router, "router must not be null");
    }

    @NonNull
    public WorkloadRouter router() {
        return router;
    }

    @Override
    public <REQUEST, RESPONSE> RESPONSE dispatchSync(
            @NonNull String serviceName,
            @Nullable REQUEST payload,
            @NonNull WorkloadSelector selector,
            @NonNull Duration timeout,
            @NonNull Map<String, String> metadata,
            @NonNull Class<RESPONSE> expectedType
    ) throws Exception {
        RoutingSelector routingSelector = toRoutingSelector(selector);
        WorkloadMetadata.Builder metaBuilder = WorkloadMetadata.builder(serviceName, UUID.randomUUID().toString())
                .selector(routingSelector);
        metadata.forEach(metaBuilder::header);
        return router.routeSync(serviceName, payload, metaBuilder.build(), timeout);
    }

    @Override
    public <REQUEST, RESPONSE> CompletableFuture<RESPONSE> dispatchAsync(
            @NonNull String serviceName,
            @Nullable REQUEST payload,
            @NonNull WorkloadSelector selector,
            @NonNull Duration timeout,
            @NonNull Map<String, String> metadata,
            @NonNull Class<RESPONSE> expectedType
    ) {
        RoutingSelector routingSelector = toRoutingSelector(selector);
        WorkloadMetadata.Builder metaBuilder = WorkloadMetadata.builder(serviceName, UUID.randomUUID().toString())
                .selector(routingSelector);
        metadata.forEach(metaBuilder::header);
        return router.routeAsync(serviceName, payload, metaBuilder.build());
    }

    @NonNull
    public static RoutingSelector toRoutingSelector(@NonNull WorkloadSelector selector) {
        Objects.requireNonNull(selector, "selector must not be null");
        if (selector.requiredTags().isEmpty()) {
            return RoutingSelector.any();
        }
        return RoutingSelector.requireTags(selector.requiredTags());
    }
}
