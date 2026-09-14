package com.github.f442y.dispersion.routing.test;

import com.github.f442y.dispersion.routing.transport.ChannelTransport;
import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import org.jspecify.annotations.NonNull;

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

public final class SimulatedNetworkTransport implements ChannelTransport {

    private static final class DefaultInMemoryTransport implements ChannelTransport {
        private static final Executor VIRTUAL_THREAD_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
        private final Map<String, List<Consumer<WorkloadEnvelope<?>>>> subscribers = new ConcurrentHashMap<>();
        private volatile boolean closed = false;

        @Override
        public <PAYLOAD> void send(@NonNull String destination, @NonNull WorkloadEnvelope<PAYLOAD> envelope) {
            if (closed) return;
            List<Consumer<WorkloadEnvelope<?>>> list = subscribers.get(destination);
            if (list != null) {
                for (Consumer<WorkloadEnvelope<?>> consumer : list) {
                    VIRTUAL_THREAD_EXECUTOR.execute(() -> consumer.accept(envelope));
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
            CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> future = new CompletableFuture<>();
            AutoCloseable sub = subscribe(replyDestination, (WorkloadEnvelope<WORKLOAD_OUTPUT> replyEnv) -> {
                if (envelope.correlationId().equals(replyEnv.correlationId())) {
                    future.complete(replyEnv);
                }
            });

            future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                    .whenComplete((res, err) -> {
                        try {
                            sub.close();
                        } catch (Exception ignored) {}
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

    private final ChannelTransport delegate;
    private volatile Duration artificialLatency = Duration.ZERO;
    private volatile double dropRate = 0.0;
    private volatile boolean partitioned = false;

    public SimulatedNetworkTransport() {
        this(new DefaultInMemoryTransport());
    }

    public SimulatedNetworkTransport(@NonNull ChannelTransport delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    public void setArtificialLatency(@NonNull Duration latency) {
        this.artificialLatency = Objects.requireNonNull(latency, "latency must not be null");
    }

    public void setDropRate(double dropRate) {
        if (dropRate < 0.0 || dropRate > 1.0) {
            throw new IllegalArgumentException("Drop rate must be between 0.0 and 1.0: " + dropRate);
        }
        this.dropRate = dropRate;
    }

    public void setPartitioned(boolean partitioned) {
        this.partitioned = partitioned;
    }

    @Override
    public <PAYLOAD> void send(@NonNull String destination, @NonNull WorkloadEnvelope<PAYLOAD> envelope) {
        if (partitioned || (dropRate > 0.0 && Math.random() < dropRate)) {
            return;
        }

        if (!artificialLatency.isZero()) {
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(artificialLatency);
                    delegate.send(destination, envelope);
                } catch (InterruptedException ignored) {}
            });
        } else {
            delegate.send(destination, envelope);
        }
    }

    @Override
    @NonNull
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> requestReply(
            @NonNull String requestDestination,
            @NonNull String replyDestination,
            @NonNull WorkloadEnvelope<WORKLOAD_INPUT> envelope,
            @NonNull Duration timeout
    ) {
        if (partitioned || (dropRate > 0.0 && Math.random() < dropRate)) {
            CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> dropped = new CompletableFuture<>();
            CompletableFuture.delayedExecutor(timeout.toMillis(), TimeUnit.MILLISECONDS)
                    .execute(() -> dropped.completeExceptionally(new TimeoutException("Simulated packet drop / partition timeout")));
            return dropped;
        }

        if (!artificialLatency.isZero()) {
            CompletableFuture<WorkloadEnvelope<WORKLOAD_OUTPUT>> delayed = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(artificialLatency);
                    delegate.<WORKLOAD_INPUT, WORKLOAD_OUTPUT>requestReply(requestDestination, replyDestination, envelope, timeout)
                            .whenComplete((res, err) -> {
                                if (err != null) {
                                    delayed.completeExceptionally(err);
                                } else {
                                    delayed.complete(res);
                                }
                            });
                } catch (InterruptedException ex) {
                    delayed.completeExceptionally(ex);
                }
            });
            return delayed;
        }

        return delegate.requestReply(requestDestination, replyDestination, envelope, timeout);
    }

    @Override
    @NonNull
    public <PAYLOAD> AutoCloseable subscribe(
            @NonNull String destination,
            @NonNull Consumer<WorkloadEnvelope<PAYLOAD>> listener
    ) {
        return delegate.subscribe(destination, listener);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
