package com.github.f442y.dispersion.executor;

import com.github.f442y.dispersion.exception.BackpressureException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Manages concurrent task admission and applies backpressure control to prevent
 * overwhelming system resources under high burst loads.
 */
public class AdmissionController {

    @NonNull
    private final Semaphore semaphore;

    @NonNull
    private final BackpressureStrategy strategy;

    @NonNull
    private final Duration defaultTimeout;

    /**
     * Creates an {@link AdmissionController} with the default {@link BackpressureStrategy#BLOCK} strategy.
     *
     * @param maxConcurrentTasks Maximum number of concurrent tasks allowed
     */
    public AdmissionController(int maxConcurrentTasks) {
        this(maxConcurrentTasks, BackpressureStrategy.BLOCK, Duration.ofSeconds(30));
    }

    /**
     * Creates an {@link AdmissionController} with a custom backpressure strategy.
     *
     * @param maxConcurrentTasks Maximum number of concurrent tasks allowed
     * @param strategy           Backpressure handling strategy
     */
    public AdmissionController(int maxConcurrentTasks, @NonNull BackpressureStrategy strategy) {
        this(maxConcurrentTasks, strategy, Duration.ofSeconds(30));
    }

    /**
     * Creates an {@link AdmissionController} with a custom backpressure strategy and timeout.
     *
     * @param maxConcurrentTasks Maximum number of concurrent tasks allowed
     * @param strategy           Backpressure handling strategy
     * @param defaultTimeout     Default timeout when using {@link BackpressureStrategy#WAIT_WITH_TIMEOUT}
     */
    public AdmissionController(
            int maxConcurrentTasks,
            @NonNull BackpressureStrategy strategy,
            @NonNull Duration defaultTimeout
    ) {
        if (maxConcurrentTasks <= 0) {
            throw new IllegalArgumentException("maxConcurrentTasks must be greater than 0, got: " + maxConcurrentTasks);
        }
        this.semaphore = new Semaphore(maxConcurrentTasks);
        this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
        this.defaultTimeout = Objects.requireNonNull(defaultTimeout, "defaultTimeout must not be null");
    }

    /**
     * Acquires an admission permit according to the configured {@link BackpressureStrategy}.
     *
     * @throws InterruptedException If the calling thread is interrupted while waiting
     * @throws BackpressureException If the request is rejected due to saturated capacity
     */
    public void acquirePermit() throws InterruptedException, BackpressureException {
        acquirePermit(null);
    }

    /**
     * Acquires an admission permit with an optional custom timeout.
     *
     * @param overrideTimeout Custom timeout duration (used with {@link BackpressureStrategy#WAIT_WITH_TIMEOUT})
     * @throws InterruptedException If the calling thread is interrupted while waiting
     * @throws BackpressureException If the request is rejected due to saturated capacity or timeout
     */
    public void acquirePermit(@Nullable Duration overrideTimeout) throws InterruptedException, BackpressureException {
        switch (strategy) {
            case BLOCK -> semaphore.acquire();
            case REJECT_IMMEDIATELY -> {
                if (!semaphore.tryAcquire()) {
                    throw new BackpressureException("State machine admission rejected: executor capacity is fully saturated (REJECT_IMMEDIATELY)");
                }
            }
            case WAIT_WITH_TIMEOUT -> {
                Duration timeout = (overrideTimeout != null) ? overrideTimeout : defaultTimeout;
                boolean acquired = semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (!acquired) {
                    throw new BackpressureException(String.format(
                            "State machine admission rejected: timed out waiting %d ms for an available permit",
                            timeout.toMillis()
                    ));
                }
            }
        }
    }

    /**
     * Releases an admission permit back to the semaphore pool.
     */
    public void releasePermit() {
        semaphore.release();
    }

    /**
     * Returns the current number of available permits.
     *
     * @return Available permits
     */
    public int availablePermits() {
        return semaphore.availablePermits();
    }

    /**
     * Returns the configured {@link BackpressureStrategy}.
     *
     * @return The active strategy
     */
    @NonNull
    public BackpressureStrategy getStrategy() {
        return strategy;
    }

    /**
     * Returns the configured default timeout duration.
     *
     * @return The default timeout
     */
    @NonNull
    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }
}
