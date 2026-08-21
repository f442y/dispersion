package com.github.f442y.dispersion.orchestration;

/**
 * Represents the lifecycle execution status of an Orchestration State Machine.
 */
public enum OrchestrationStatus {
    /** The orchestration state machine is initialized and pending execution. */
    PENDING,
    /** The orchestration state machine is currently traversing states on virtual threads. */
    RUNNING,
    /** The orchestration state machine reached a terminal end state successfully. */
    COMPLETED,
    /** A state failed permanently and saga compensation is currently executing. */
    COMPENSATING,
    /** Saga compensation finished and the orchestration state machine concluded in a rolled-back state. */
    COMPENSATED,
    /** The orchestration state machine failed permanently. */
    FAILED
}
