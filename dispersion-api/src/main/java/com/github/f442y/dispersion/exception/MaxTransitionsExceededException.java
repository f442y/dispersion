package com.github.f442y.dispersion.exception;

/**
 * Thrown when a state machine execution exceeds the global maximum permitted transition count,
 * acting as a circuit breaker against infinite loops or runaway cycles.
 */
public final class MaxTransitionsExceededException extends StateMachineException {

    private final int maxTransitions;

    public MaxTransitionsExceededException(int maxTransitions) {
        super("State machine exceeded the global maximum transition limit of " + maxTransitions);
        this.maxTransitions = maxTransitions;
    }

    public int getMaxTransitions() {
        return maxTransitions;
    }
}
