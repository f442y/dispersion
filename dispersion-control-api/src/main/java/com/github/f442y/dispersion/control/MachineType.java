package com.github.f442y.dispersion.control;

/**
 * Categorization of state machine architectures supported by Dispersion.
 */
public enum MachineType {

    /**
     * Thread-confined, high-throughput micro-state machine executing on a single virtual thread.
     */
    ATOMIC,

    /**
     * Turn-based, durable macro-orchestration workflow supporting checkpoint persistence,
     * external signal suspension/rehydration, parallel branches, and automated LIFO Saga rollbacks.
     */
    ORCHESTRATION,

    /**
     * Batch collection workflow coordinating concurrent item pipelines and dynamic synchronization barriers.
     */
    BATCH
}
