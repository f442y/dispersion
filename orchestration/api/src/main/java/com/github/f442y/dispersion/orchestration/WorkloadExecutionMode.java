package com.github.f442y.dispersion.orchestration;

/**
 * Declares the execution and suspension mode for a workload invocation in an orchestration step.
 */
public enum WorkloadExecutionMode {

    /**
     * Executes synchronously on the caller's Java 25 virtual thread.
     * Carrier threads are released during I/O wait. Recommended for in-process monolith
     * workloads (< 1 μs) and low-latency microservice RPC.
     */
    SYNCHRONOUS_VIRTUAL_THREAD,

    /**
     * Emits an outbound workload request, snapshots state to CheckpointStore,
     * releases the virtual thread, and suspends the turn. Resumes when the worker
     * reply signal is delivered.
     */
    DURABLE_TURN_SUSPENSION
}
