package com.github.f442y.dispersion.routing.transport;

import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Transport contract for bi-directional asynchronous workload envelope delivery.
 */
public interface ChannelTransport extends AutoCloseable {

    <PAYLOAD> void send(
            @NonNull String destination,
            @NonNull WorkloadEnvelope<PAYLOAD> envelope
    );

    <WORKLOAD_INPUT, WORKLOAD_OUTPUT> CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> requestReply(
            @NonNull String requestDestination,
            @NonNull String replyDestination,
            @NonNull WorkloadEnvelope<WORKLOAD_INPUT> envelope,
            @NonNull Duration timeout
    );

    <PAYLOAD> AutoCloseable subscribe(
            @NonNull String destination,
            @NonNull Consumer<WorkloadEnvelope<PAYLOAD>> listener
    );

    @Override
    default void close() throws Exception {}
}
