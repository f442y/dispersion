package com.github.f442y.dispersion.routing.worker;

import org.jspecify.annotations.NonNull;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface WorkloadHandler<WORKLOAD_INPUT, WORKLOAD_OUTPUT> {

    @NonNull
    CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> handle(
            @NonNull WorkloadEnvelope<WORKLOAD_INPUT> envelope
    );
}
