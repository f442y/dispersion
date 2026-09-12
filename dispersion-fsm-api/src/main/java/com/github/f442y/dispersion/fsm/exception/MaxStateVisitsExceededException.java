package com.github.f442y.dispersion.fsm.exception;

import org.jspecify.annotations.NonNull;

/**
 * Thrown when a state exceeds its configured maximum visit count without a fallback destination state.
 */
public final class MaxStateVisitsExceededException extends StateMachineException {

    private final String stateName;
    private final int maxVisits;

    public MaxStateVisitsExceededException(@NonNull String stateName, int maxVisits) {
        super("State [" + stateName + "] exceeded maximum permitted visit count of " + maxVisits + " without a fallback state");
        this.stateName = stateName;
        this.maxVisits = maxVisits;
    }

    @NonNull
    public String getStateName() {
        return stateName;
    }

    public int getMaxVisits() {
        return maxVisits;
    }
}
