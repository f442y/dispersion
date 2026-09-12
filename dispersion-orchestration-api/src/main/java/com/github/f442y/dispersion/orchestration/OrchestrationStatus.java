package com.github.f442y.dispersion.orchestration;

/**
 * Lifecycle execution status of a turn-based durable Orchestration State Machine.
 */
public enum OrchestrationStatus {

    /**
     * Initial status before the first turn begins.
     */
    PENDING,

    /**
     * Currently executing a step or turn on a virtual thread.
     */
    RUNNING,

    /**
     * Successfully transitioned to a terminal end state.
     */
    COMPLETED,

    /**
     * Execution suspended at a signal/command wait state, releasing thread and persisting a checkpoint.
     */
    SUSPENDED,

    /**
     * Unwinding actions via LIFO Saga compensations due to a downstream failure.
     */
    COMPENSATING,

    /**
     * Saga compensation rollback has completed.
     */
    COMPENSATED,

    /**
     * Execution terminated with a fatal unhandled failure or uncompensated error.
     */
    FAILED
}
