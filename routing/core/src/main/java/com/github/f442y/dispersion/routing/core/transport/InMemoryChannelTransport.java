package com.github.f442y.dispersion.routing.core.transport;

import com.github.f442y.dispersion.routing.transport.ChannelTransport;
import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public final class InMemoryChannelTransport implements ChannelTransport {

    private static final Logger log = LoggerFactory.getLogger(InMemoryChannelTransport.class);
    private static final Executor VIRTUAL_THREAD_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final Map<String, List<Consumer<WorkloadEnvelope<?>>>> subscribers = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    @Override
    public <PAYLOAD> void send(
            @NonNull String destination,
            @NonNull WorkloadEnvelope<PAYLOAD> envelope
    ) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(envelope, "envelope must not be null");

        if (closed) {
            throw new IllegalStateException("InMemoryChannelTransport is closed");
        }

        List<Consumer<WorkloadEnvelope<?>>> listeners = subscribers.get(destination);
        if (listeners != null && !listeners.isEmpty()) {
            for (Consumer<WorkloadEnvelope<?>> listener : listeners) {
                VIRTUAL_THREAD_EXECUTOR.execute(() -> {
                    try {
                        listener.accept(envelope);
                    } catch (Throwable t) {
                        log.atWarn()
                                .setCause(t)
                                .addKeyValue("destination", destination)
                                .addKeyValue("correlation_id", envelope.correlationId())
                                .log("Error delivering message to subscriber");
                    }
                });
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    @NonNull
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> requestReply(
            @NonNull String requestDestination,
            @NonNull String replyDestination,
            @NonNull WorkloadEnvelope<WORKLOAD_INPUT> envelope,
            @NonNull Duration timeout
    ) {
        Objects.requireNonNull(requestDestination, "requestDestination must not be null");
        Objects.requireNonNull(replyDestination, "replyDestination must not be null");
        Objects.requireNonNull(envelope, "envelope must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");

        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("InMemoryChannelTransport is closed"));
        }

        CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> future = new CompletableFuture<>();
        AutoCloseable subscription = this.<WORKLOAD_OUTPUT>subscribe(replyDestination, (WorkloadEnvelope<WORKLOAD_OUTPUT> replyEnv) -> {
            if (envelope.correlationId().equals(replyEnv.correlationId())) {
                future.complete(replyEnv);
            }
        });

        future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((res, err) -> {
                    try {
                        subscription.close();
                    } catch (Exception ignored) {}

                    if (err != null && err.getCause() instanceof TimeoutException) {
                        log.atWarn()
                                .addKeyValue("correlation_id", envelope.correlationId())
                                .addKeyValue("request_destination", requestDestination)
                                .log("Workload request timed out waiting for reply");
                    }
                });

        send(requestDestination, envelope);

        return future;
    }

    @Override
    @SuppressWarnings("unchecked")
    @NonNull
    public <PAYLOAD> AutoCloseable subscribe(
            @NonNull String destination,
            @NonNull Consumer<WorkloadEnvelope<PAYLOAD>> listener
    ) {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(listener, "listener must not be null");

        Consumer<WorkloadEnvelope<?>> erased = (WorkloadEnvelope<?> env) -> listener.accept((WorkloadEnvelope<PAYLOAD>) env);
        List<Consumer<WorkloadEnvelope<?>>> list = subscribers.computeIfAbsent(destination, k -> new CopyOnWriteArrayList<>());
        list.add(erased);

        return () -> list.remove(erased);
    }

    @Override
    public void close() {
        closed = true;
        subscribers.clear();
    }
}
