package com.github.f442y.dispersion.event;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Manages dynamic, short-lived live-watch leases for target execution IDs.
 *
 * <p>When an operator or UI client "watches live" a specific execution, a dynamic tap
 * is registered with a bounded lease (default 30 seconds). While the tap is active,
 * both {@link EventTier#LIFECYCLE} and {@link EventTier#GRANULAR} events for that execution
 * are routed to telemetry subscribers.</p>
 */
public interface DynamicTapManager extends AutoCloseable {

    /**
     * Default lease duration for an active dynamic tap.
     */
    Duration DEFAULT_TAP_TTL = Duration.ofSeconds(30);

    /**
     * Default interval between background active lease sweeps.
     */
    Duration DEFAULT_SWEEP_INTERVAL = Duration.ofSeconds(15);

    /**
     * Registers an active tap lease for the given execution ID using {@link #DEFAULT_TAP_TTL}.
     * If already registered, increments the subscriber reference count.
     *
     * @param machineId The execution UUID to tap
     */
    void registerTap(@NonNull UUID machineId);

    /**
     * Registers an active tap lease for the given execution ID with a custom TTL.
     * If already registered, increments the subscriber reference count.
     *
     * @param machineId The execution UUID to tap
     * @param ttl       The duration of the tap lease
     */
    void registerTap(@NonNull UUID machineId, @NonNull Duration ttl);

    /**
     * Renews the active tap lease expiration for the given execution ID without modifying
     * subscriber reference counts using {@link #DEFAULT_TAP_TTL}.
     *
     * @param machineId The execution UUID whose lease to renew
     */
    default void renewTap(@NonNull UUID machineId) {
        renewTap(machineId, DEFAULT_TAP_TTL);
    }

    /**
     * Renews the active tap lease expiration for the given execution ID with a custom TTL
     * without modifying subscriber reference counts.
     *
     * @param machineId The execution UUID whose lease to renew
     * @param ttl       The duration to extend the lease
     */
    default void renewTap(@NonNull UUID machineId, @NonNull Duration ttl) {
        registerTap(machineId, ttl);
    }

    /**
     * Decrements the active subscriber count for the given execution ID, revoking the tap
     * when no active subscribers remain.
     *
     * @param machineId The execution UUID to untap
     */
    void unregisterTap(@NonNull UUID machineId);

    /**
     * Checks if the given execution ID has an active, unexpired tap lease.
     *
     * @param machineId The execution UUID to check
     * @return {@code true} if active tap lease exists and has not expired
     */
    boolean hasActiveTap(@NonNull UUID machineId);

    /**
     * Returns the count of currently active (unexpired) tap leases.
     *
     * @return Active tap lease count
     */
    int activeTapCount();

    /**
     * Prunes all expired tap leases.
     */
    void pruneExpired();

    /**
     * Clears all registered tap leases.
     */
    void clear();

    /**
     * Closes the dynamic tap manager, terminating any background sweeper threads.
     */
    @Override
    default void close() {
    }

    /**
     * Evaluates whether an event should be routed for telemetry publication based on
     * event tier and active dynamic taps.
     *
     * <ul>
     *   <li>{@link EventTier#LIFECYCLE} events always return {@code true}.</li>
     *   <li>{@link EventTier#GRANULAR} events return {@code true} if and only if an active tap exists for the machine ID.</li>
     * </ul>
     *
     * @param event The event to evaluate
     * @return {@code true} if the event should be routed/published
     */
    default boolean shouldRoute(@NonNull ExecutionEvent event) {
        return event.isLifecycle() || hasActiveTap(event.machineId());
    }

    /**
     * Returns a {@link Predicate} representing {@link #shouldRoute(ExecutionEvent)}.
     *
     * @return Event routing filter predicate
     */
    default @NonNull Predicate<ExecutionEvent> asFilter() {
        return this::shouldRoute;
    }
}
