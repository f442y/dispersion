package com.github.f442y.dispersion.fsm.test;

import com.github.f442y.dispersion.fsm.state.StateKey;

/**
 * Standard enum implementing {@link StateKey} for testing state transitions.
 */
public enum TestStateKey implements StateKey {
    START,
    STEP_1,
    STEP_2,
    STEP_3,
    SUSPENDED,
    FAILED,
    COMPLETED
}
