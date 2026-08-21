package com.github.f442y.dispersion.exception;

import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;

import java.util.Objects;

/**
 * Exception thrown when a state machine execution exceeds the configured global maximum transition threshold.
 * Prevents runaway infinite loops during graph traversal.
 */
public final class MaxTransitionsExceededException extends StateMachineException {

    @NonNull
    private final StateKey lastStateKey;
    private final int maxTransitions;
    private final int executedTransitions;

    public MaxTransitionsExceededException(
            @NonNull StateKey lastStateKey,
            int maxTransitions,
            int executedTransitions
    ) {
        super(String.format(
                "State machine exceeded maximum transition limit of %d (executed %d transitions, halted in state '%s')",
                maxTransitions, executedTransitions, Objects.requireNonNull(lastStateKey, "lastStateKey must not be null").name()
        ));
        this.lastStateKey = lastStateKey;
        this.maxTransitions = maxTransitions;
        this.executedTransitions = executedTransitions;
    }

    /**
     * Returns the state identifier where execution was halted.
     *
     * @return The {@link StateKey} instance
     */
    @NonNull
    public StateKey getLastStateKey() {
        return lastStateKey;
    }

    /**
     * Returns the configured maximum transition threshold.
     *
     * @return The maximum allowed transitions
     */
    public int getMaxTransitions() {
        return maxTransitions;
    }

    /**
     * Returns the total number of transitions executed before halting.
     *
     * @return The executed transition count
     */
    public int getExecutedTransitions() {
        return executedTransitions;
    }
}
