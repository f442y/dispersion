package com.github.f442y.dispersion.routing.backpressure;

import org.jspecify.annotations.NonNull;

import java.util.Objects;

public final class CapacityExceededException extends RuntimeException {

    private final String serviceName;
    private final int totalInFlight;
    private final int maxPermits;

    public CapacityExceededException(
            @NonNull String serviceName,
            int totalInFlight,
            int maxPermits,
            @NonNull String message
    ) {
        super(Objects.requireNonNull(message, "message must not be null"));
        this.serviceName = Objects.requireNonNull(serviceName, "serviceName must not be null");
        this.totalInFlight = totalInFlight;
        this.maxPermits = maxPermits;
    }

    @NonNull
    public String serviceName() {
        return serviceName;
    }

    public int totalInFlight() {
        return totalInFlight;
    }

    public int maxPermits() {
        return maxPermits;
    }
}
