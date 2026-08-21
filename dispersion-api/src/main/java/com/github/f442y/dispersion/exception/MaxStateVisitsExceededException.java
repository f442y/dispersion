package com.github.f442y.dispersion.exception;

import com.github.f442y.dispersion.state.StateKey;
import org.jspecify.annotations.NonNull;

import java.util.Objects;

/**
 * Exception thrown when a specific state within a state machine execution is entered more times
 * than allowed by its configured maximum visit threshold and no fallback state is configured.
 */
public final class MaxStateVisitsExceededException extends StateMachineException {

    @NonNull
    private final StateKey stateKey;
    private final int maxVisits;

    public MaxStateVisitsExceededException(
            @NonNull StateKey stateKey,
            int maxVisits
    ) {
        super(String.format(
                "State '%s' exceeded its maximum visit threshold of %d",
                Objects.requireNonNull(stateKey, "stateKey must not be null").name(), maxVisits
        ));
        this.stateKey = stateKey;
        this.maxVisits = maxVisits;
    }

    /**
     * Returns the state identifier that exceeded its visit threshold.
     *
     * @return The {@link StateKey} instance
     */
    @NonNull
    public StateKey getStateKey() {
        return stateKey;
    }

    /**
     * Returns the configured maximum visit limit for this state.
     *
     * @return The maximum allowed visits
     */
    public int getMaxVisits() {
        return maxVisits;
    }
}
