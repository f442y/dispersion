package com.github.f442y.dispersion.orchestration;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable retry policy specification for handling transient errors.
 */
public record RetryPolicy(
        int maxAttempts,
        @NonNull Duration initialDelay,
        double backoffMultiplier,
        @NonNull Duration maxDelay
) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, but was: " + maxAttempts);
        }
        Objects.requireNonNull(initialDelay, "initialDelay must not be null");
        Objects.requireNonNull(maxDelay, "maxDelay must not be null");
        if (backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be >= 1.0, but was: " + backoffMultiplier);
        }
    }

    /**
     * Computes the delay duration for a specific retry attempt index (1-based).
     *
     * @param attempt Current attempt index (1-based)
     * @return The duration to wait before this attempt
     */
    @NonNull
    public Duration computeDelay(int attempt) {
        if (attempt <= 1 || initialDelay.isZero()) {
            return Duration.ZERO;
        }
        double factor = Math.pow(backoffMultiplier, attempt - 2);
        long delayMillis = (long) (initialDelay.toMillis() * factor);
        long maxMillis = maxDelay.toMillis();
        return Duration.ofMillis(Math.min(delayMillis, maxMillis));
    }

    @NonNull
    public static RetryPolicy noRetries() {
        return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
    }

    @NonNull
    public static RetryPolicy none() {
        return noRetries();
    }

    @NonNull
    public static RetryPolicy fixed(int maxAttempts, @NonNull Duration delay) {
        Objects.requireNonNull(delay, "delay must not be null");
        return new RetryPolicy(maxAttempts, delay, 1.0, delay);
    }

    @NonNull
    public static RetryPolicy exponential(
            int maxAttempts,
            @NonNull Duration initialDelay,
            double multiplier,
            @NonNull Duration maxDelay
    ) {
        return new RetryPolicy(maxAttempts, initialDelay, multiplier, maxDelay);
    }

    @NonNull
    public static RetryPolicy exponentialBackoff(
            int maxAttempts,
            @NonNull Duration initialDelay,
            double multiplier
    ) {
        return exponential(maxAttempts, initialDelay, multiplier, initialDelay.multipliedBy(100));
    }
}
