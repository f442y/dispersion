package com.github.f442y.dispersion.control;

/**
 * Lifecycle execution status of a state machine execution or turn.
 */
public enum ExecutionStatus {

    /**
     * Workflow execution or turn is actively progressing on a virtual thread.
     */
    RUNNING,

    /**
     * Workflow is suspended at a signal wait step or barrier, awaiting an external event or rehydration.
     */
    SUSPENDED,

    /**
     * Workflow has reached a declared terminal end-state and successfully completed.
     */
    COMPLETED,

    /**
     * Workflow encountered a failure and has successfully executed all compensating actions in LIFO order.
     */
    COMPENSATED,

    /**
     * Workflow failed due to an unhandled exception or non-recoverable error without compensation.
     */
    FAILED
}
