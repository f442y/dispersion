package com.github.f442y.dispersion.routing.backpressure;

import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import org.jspecify.annotations.NonNull;

import java.util.Objects;

public final class NoMatchingEndpointException extends RuntimeException {

    private final String serviceName;
    private final RoutingSelector selector;

    public NoMatchingEndpointException(
            @NonNull String serviceName,
            @NonNull RoutingSelector selector,
            @NonNull String message
    ) {
        super(Objects.requireNonNull(message, "message must not be null"));
        this.serviceName = Objects.requireNonNull(serviceName, "serviceName must not be null");
        this.selector = Objects.requireNonNull(selector, "selector must not be null");
    }

    @NonNull
    public String serviceName() {
        return serviceName;
    }

    @NonNull
    public RoutingSelector selector() {
        return selector;
    }
}
