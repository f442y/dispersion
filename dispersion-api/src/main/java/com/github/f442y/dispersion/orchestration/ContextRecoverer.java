package com.github.f442y.dispersion.orchestration;

import com.github.f442y.dispersion.context.StateMachineContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Functional contract for reconstructing or resetting pristine input payloads when an atomic
 * state machine step within an orchestration state machine fails and needs to be retried on a fresh virtual thread.
 * <p>
 * This prevents polluted or half-mutated context state from corrupting subsequent execution attempts.
 *
 * @param <ORCHESTRATION_CONTEXT> The encompassing orchestration state machine context type
 * @param <ATOMIC_INPUT>          The input payload required by the atomic state machine
 */
@FunctionalInterface
public interface ContextRecoverer<ORCHESTRATION_CONTEXT extends StateMachineContext, ATOMIC_INPUT> {

    /**
     * Reconstructs or supplies a pristine {@code ATOMIC_INPUT} payload for an atomic state machine execution attempt.
     *
     * @param orchestrationContext The current orchestration context snapshot
     * @param failureCause         The exception from the previous failed attempt, or {@code null} on the initial run
     * @param attemptNumber        The current attempt index (1-based: 1 for initial, 2+ for retries)
     * @return A clean, uncorrupted input payload for the atomic state machine
     * @throws Exception If context reconstruction fails
     */
    @Nullable
    ATOMIC_INPUT recover(
            @NonNull ORCHESTRATION_CONTEXT orchestrationContext,
            @Nullable Throwable failureCause,
            int attemptNumber
    ) throws Exception;
}
