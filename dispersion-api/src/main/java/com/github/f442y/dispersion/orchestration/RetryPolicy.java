package com.github.f442y.dispersion.orchestration;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration defining retry attempts, backoff durations, and recovery limits
 * for an atomic state machine execution within an orchestration state machine.
 */
public record RetryPolicy(
        int maxAttempts,
        @NonNull Duration initialDelay,
        double backoffMultiplier,
        @NonNull Duration maxDelay
) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        Objects.requireNonNull(initialDelay, "initialDelay must not be null");
        Objects.requireNonNull(maxDelay, "maxDelay must not be null");
        if (backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be at least 1.0");
        }
    }

    /**
     * Creates a single-attempt retry policy (no retries).
     *
     * @return Single-attempt {@link RetryPolicy}
     */
    @NonNull
    public static RetryPolicy noRetries() {
        return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
    }

    /**
     * Creates a simple fixed-delay retry policy.
     *
     * @param maxAttempts Maximum attempts before declaring failure
     * @param delay       Fixed delay between attempts
     * @return A fixed delay {@link RetryPolicy}
     */
    @NonNull
    public static RetryPolicy fixed(int maxAttempts, @NonNull Duration delay) {
        return new RetryPolicy(maxAttempts, delay, 1.0, delay);
    }

    /**
     * Creates an exponential backoff retry policy.
     *
     * @param maxAttempts  Maximum attempts before declaring failure
     * @param initialDelay Initial delay before first retry
     * @param multiplier   Multiplier applied to delay on each subsequent retry
     * @param maxDelay     Upper bound on delay duration
     * @return An exponential backoff {@link RetryPolicy}
     */
    @NonNull
    public static RetryPolicy exponential(
            int maxAttempts,
            @NonNull Duration initialDelay,
            double multiplier,
            @NonNull Duration maxDelay
    ) {
        return new RetryPolicy(maxAttempts, initialDelay, multiplier, maxDelay);
    }

    /**
     * Calculates the delay duration before the given attempt number.
     *
     * @param attemptNumber 1-based attempt index (e.g. 2 for first retry)
     * @return The duration to wait before executing the attempt
     */
    @NonNull
    public Duration computeDelay(int attemptNumber) {
        if (attemptNumber <= 1 || maxAttempts <= 1) {
            return Duration.ZERO;
        }
        long delayMillis = (long) (initialDelay.toMillis() * Math.pow(backoffMultiplier, attemptNumber - 2));
        long clampedMillis = Math.min(delayMillis, maxDelay.toMillis());
        return Duration.ofMillis(clampedMillis);
    }
}
