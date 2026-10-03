package com.github.f442y.dispersion.event.bus;

import com.github.f442y.dispersion.event.DynamicTapManager;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe default implementation of {@link DynamicTapManager} with reference-counted
 * active tap leases and background sweeper.
 *
 * <p>Multiple concurrent subscribers (e.g. multiple browser tabs or operators) can watch the
 * same execution without interfering with each other. Each {@link #registerTap} increments
 * the active subscriber reference count and extends the lease expiration. {@link #renewTap}
 * extends the lease without modifying the count. {@link #unregisterTap} decrements the count
 * and only evicts when no active subscribers remain. If subscribers disconnect ungracefully
 * without unregistering, the background Virtual Thread sweeper prunes expired leases automatically.</p>
 */
public final class DefaultDynamicTapManager implements DynamicTapManager {

    private final Duration defaultTtl;
    private final Duration sweepInterval;
    private final ConcurrentHashMap<UUID, TapState> activeTaps = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread sweeperThread;

    public DefaultDynamicTapManager() {
        this(DEFAULT_TAP_TTL, DEFAULT_SWEEP_INTERVAL);
    }

    public DefaultDynamicTapManager(@NonNull Duration defaultTtl) {
        this(defaultTtl, DEFAULT_SWEEP_INTERVAL);
    }

    public DefaultDynamicTapManager(@NonNull Duration defaultTtl, @Nullable Duration sweepInterval) {
        this.defaultTtl = Objects.requireNonNull(defaultTtl, "defaultTtl must not be null");
        this.sweepInterval = sweepInterval;
        if (sweepInterval != null && !sweepInterval.isZero() && !sweepInterval.isNegative()) {
            this.sweeperThread = Thread.ofVirtual()
                    .name("dispersion-dynamic-tap-sweeper")
                    .start(this::sweepLoop);
        } else {
            this.sweeperThread = null;
        }
    }

    private void sweepLoop() {
        while (!closed.get()) {
            try {
                Thread.sleep(sweepInterval);
                pruneExpired();
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    @Override
    public void registerTap(@NonNull UUID machineId) {
        registerTap(machineId, defaultTtl);
    }

    @Override
    public void registerTap(@NonNull UUID machineId, @NonNull Duration ttl) {
        if (closed.get()) {
            return;
        }
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        Instant expiry = Instant.now().plus(ttl);

        activeTaps.compute(machineId, (_, state) -> {
            if (state == null) {
                return new TapState(expiry);
            }
            state.increment();
            state.extend(expiry);
            return state;
        });
    }

    @Override
    public void renewTap(@NonNull UUID machineId, @NonNull Duration ttl) {
        if (closed.get()) {
            return;
        }
        Objects.requireNonNull(machineId, "machineId must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        Instant expiry = Instant.now().plus(ttl);

        TapState state = activeTaps.get(machineId);
        if (state != null) {
            state.extend(expiry);
        }
    }

    @Override
    public void unregisterTap(@NonNull UUID machineId) {
        Objects.requireNonNull(machineId, "machineId must not be null");
        activeTaps.computeIfPresent(machineId, (_, state) -> {
            int remaining = state.decrement();
            return remaining > 0 ? state : null;
        });
    }

    @Override
    public boolean hasActiveTap(@NonNull UUID machineId) {
        if (closed.get()) {
            return false;
        }
        Objects.requireNonNull(machineId, "machineId must not be null");
        TapState state = activeTaps.get(machineId);
        if (state == null) {
            return false;
        }

        Instant now = Instant.now();
        if (state.isExpired(now)) {
            activeTaps.remove(machineId, state);
            return false;
        }
        return state.refCount() > 0;
    }

    @Override
    public int activeTapCount() {
        pruneExpired();
        return activeTaps.size();
    }

    @Override
    public void pruneExpired() {
        Instant now = Instant.now();
        activeTaps.entrySet().removeIf(entry -> entry.getValue().isExpired(now));
    }

    @Override
    public void clear() {
        activeTaps.clear();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (sweeperThread != null) {
                sweeperThread.interrupt();
            }
            activeTaps.clear();
        }
    }

    public @NonNull Duration defaultTtl() {
        return defaultTtl;
    }

    public @Nullable Duration sweepInterval() {
        return sweepInterval;
    }

    public boolean isClosed() {
        return closed.get();
    }

    private static final class TapState {
        private final AtomicInteger refCount = new AtomicInteger(1);
        private volatile Instant expiry;

        private TapState(Instant expiry) {
            this.expiry = expiry;
        }

        int increment() {
            return refCount.incrementAndGet();
        }

        int decrement() {
            return refCount.decrementAndGet();
        }

        int refCount() {
            return refCount.get();
        }

        void extend(Instant newExpiry) {
            if (newExpiry.isAfter(this.expiry)) {
                this.expiry = newExpiry;
            }
        }

        boolean isExpired(Instant now) {
            return now.isAfter(expiry);
        }
    }
}
