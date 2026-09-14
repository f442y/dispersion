package com.github.f442y.dispersion.orchestration;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Service Provider Interface for dispatching distributed workloads to external workers.
 * Bridges orchestration steps with external routing and transport mechanisms.
 */
@FunctionalInterface
public interface WorkloadDispatcher {

    /**
     * Synchronously dispatches a workload request to an external worker service.
     *
     * @param serviceName  Target service identifier
     * @param payload      Workload request payload
     * @param selector     Selection criteria for worker hosts
     * @param timeout      Execution deadline
     * @param metadata     Trace and correlation headers
     * @param expectedType Expected response class
     * @param <REQUEST>    Request type
     * @param <RESPONSE>   Response type
     * @return Result produced by worker service
     * @throws Exception If dispatch fails or times out
     */
    <REQUEST, RESPONSE> RESPONSE dispatchSync(
            @NonNull String serviceName,
            @Nullable REQUEST payload,
            @NonNull WorkloadSelector selector,
            @NonNull Duration timeout,
            @NonNull Map<String, String> metadata,
            @NonNull Class<RESPONSE> expectedType
    ) throws Exception;

    /**
     * Asynchronously dispatches a workload request to an external worker service.
     */
    default <REQUEST, RESPONSE> CompletableFuture<RESPONSE> dispatchAsync(
            @NonNull String serviceName,
            @Nullable REQUEST payload,
            @NonNull WorkloadSelector selector,
            @NonNull Duration timeout,
            @NonNull Map<String, String> metadata,
            @NonNull Class<RESPONSE> expectedType
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return dispatchSync(serviceName, payload, selector, timeout, metadata, expectedType);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
