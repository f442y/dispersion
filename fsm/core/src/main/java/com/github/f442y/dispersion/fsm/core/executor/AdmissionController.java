package com.github.f442y.dispersion.fsm.core.executor;

import com.github.f442y.dispersion.fsm.exception.BackpressureException;
import com.github.f442y.dispersion.fsm.executor.BackpressureStrategy;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Concurrency admission coordinator managing permit allocation and backpressure strategies
 * for state machine executors on Java Virtual Threads.
 */
public class AdmissionController {

    private final int maxConcurrent;
    private final BackpressureStrategy strategy;
    private final Duration defaultTimeout;
    private final Semaphore semaphore;

    public AdmissionController(int maxConcurrent) {
        this(maxConcurrent, BackpressureStrategy.BLOCK, Duration.ofSeconds(30));
    }

    public AdmissionController(int maxConcurrent, @NonNull BackpressureStrategy strategy) {
        this(maxConcurrent, strategy, Duration.ofSeconds(30));
    }

    public AdmissionController(
            int maxConcurrent,
            @NonNull BackpressureStrategy strategy,
            @Nullable Duration defaultTimeout
    ) {
        if (maxConcurrent < 1) {
            throw new IllegalArgumentException("maxConcurrent must be >= 1, but was: " + maxConcurrent);
        }
        this.maxConcurrent = maxConcurrent;
        this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
        this.defaultTimeout = (defaultTimeout != null) ? defaultTimeout : Duration.ofSeconds(30);
        this.semaphore = new Semaphore(maxConcurrent, true);
    }

    public static AdmissionController rejectImmediately(int maxConcurrent) {
        return new AdmissionController(maxConcurrent, BackpressureStrategy.REJECT_IMMEDIATELY, Duration.ZERO);
    }

    public static AdmissionController waitWithTimeout(int maxConcurrent, @NonNull Duration timeout) {
        return new AdmissionController(maxConcurrent, BackpressureStrategy.WAIT_WITH_TIMEOUT, timeout);
    }

    public static AdmissionController block(int maxConcurrent) {
        return new AdmissionController(maxConcurrent, BackpressureStrategy.BLOCK, Duration.ZERO);
    }

    /**
     * Attempts to acquire an execution permit according to the configured {@link BackpressureStrategy}.
     *
     * @throws InterruptedException If the thread is interrupted while waiting
     * @throws BackpressureException If permit acquisition fails or times out
     */
    public void acquire() throws InterruptedException, BackpressureException {
        switch (strategy) {
            case BLOCK -> semaphore.acquire();
            case REJECT_IMMEDIATELY -> {
                if (!semaphore.tryAcquire()) {
                    throw new BackpressureException(
                            "Admission rejected: executor capacity saturated (" + maxConcurrent + " active tasks)"
                    );
                }
            }
            case WAIT_WITH_TIMEOUT -> acquire(defaultTimeout);
        }
    }

    /**
     * Attempts to acquire an execution permit with a specific timeout duration.
     *
     * @param timeout The maximum duration to wait for a permit
     * @throws InterruptedException If interrupted while waiting
     * @throws BackpressureException If permit could not be acquired within the timeout
     */
    public void acquire(@NonNull Duration timeout) throws InterruptedException, BackpressureException {
        Objects.requireNonNull(timeout, "timeout must not be null");
        boolean acquired = semaphore.tryAcquire(timeout.toNanos(), TimeUnit.NANOSECONDS);
        if (!acquired) {
            throw new BackpressureException(
                    "Admission timed out waiting for permit after " + timeout.toMillis() + "ms"
            );
        }
    }

    /**
     * Releases a previously acquired execution permit back to the admission pool.
     */
    public void release() {
        semaphore.release();
    }

    public int maxConcurrent() {
        return maxConcurrent;
    }

    @NonNull
    public BackpressureStrategy strategy() {
        return strategy;
    }

    @NonNull
    public Duration defaultTimeout() {
        return defaultTimeout;
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }
}
