package com.github.f442y.dispersion.routing.core.worker;

import com.github.f442y.dispersion.routing.transport.ChannelTransport;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import com.github.f442y.dispersion.routing.worker.WorkerHost;
import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import com.github.f442y.dispersion.routing.worker.WorkloadHandler;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public final class DefaultWorkerHost implements WorkerHost {

    private static final Logger log = LoggerFactory.getLogger(DefaultWorkerHost.class);
    private static final Executor VIRTUAL_THREAD_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final String workerId;
    private final EndpointTags tags;
    private final ChannelTransport transport;
    private final Map<String, WorkloadHandler<?, ?>> handlers = new ConcurrentHashMap<>();
    private final List<AutoCloseable> subscriptions = new ArrayList<>();
    private volatile boolean running = false;

    public DefaultWorkerHost(
            @NonNull String workerId,
            @NonNull EndpointTags tags,
            @NonNull ChannelTransport transport
    ) {
        this.workerId = Objects.requireNonNull(workerId, "workerId must not be null");
        this.tags = Objects.requireNonNull(tags, "tags must not be null");
        this.transport = Objects.requireNonNull(transport, "transport must not be null");
    }

    @Override
    @NonNull
    public String workerId() {
        return workerId;
    }

    @Override
    @NonNull
    public EndpointTags tags() {
        return tags;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public <WORKLOAD_INPUT, WORKLOAD_OUTPUT> void registerHandler(
            @NonNull String serviceName,
            @NonNull WorkloadHandler<WORKLOAD_INPUT, WORKLOAD_OUTPUT> handler
    ) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        handlers.put(serviceName, handler);

        if (running) {
            bindChannel(serviceName);
        }
    }

    @Override
    public void unregisterHandler(@NonNull String serviceName) {
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        handlers.remove(serviceName);
    }

    @Override
    @NonNull
    public Set<String> registeredServices() {
        return handlers.keySet();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;

        for (String serviceName : handlers.keySet()) {
            bindChannel(serviceName);
        }

        log.atInfo()
                .addKeyValue("worker_id", workerId)
                .addKeyValue("services_count", handlers.size())
                .log("WorkerHost started successfully");
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;

        for (AutoCloseable sub : subscriptions) {
            try {
                sub.close();
            } catch (Exception ex) {
                log.atWarn().setCause(ex).log("Error closing channel subscription");
            }
        }
        subscriptions.clear();

        log.atInfo()
                .addKeyValue("worker_id", workerId)
                .log("WorkerHost stopped");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void bindChannel(String serviceName) {
        AutoCloseable subscription = transport.<Object>subscribe(serviceName, (WorkloadEnvelope<Object> rawEnvelope) -> {
            VIRTUAL_THREAD_EXECUTOR.execute(() -> {
                WorkloadHandler handler = handlers.get(serviceName);
                if (handler == null) {
                    log.atWarn()
                            .addKeyValue("worker_id", workerId)
                            .addKeyValue("service_name", serviceName)
                            .addKeyValue("correlation_id", rawEnvelope.correlationId())
                            .log("No handler registered for received workload envelope");
                    return;
                }

                try {
                    CompletableFuture<WorkloadEnvelope<?>> future = handler.handle(rawEnvelope);
                    future.whenComplete((reply, err) -> {
                        String replyDest = rawEnvelope.replyDestination();
                        if (replyDest == null) {
                            return;
                        }

                        if (err != null) {
                            WorkloadEnvelope<?> failure = WorkloadEnvelope.failure(
                                    rawEnvelope.correlationId(),
                                    serviceName,
                                    rawEnvelope.metadata(),
                                    err.getMessage() != null ? err.getMessage() : "Unknown error"
                            );
                            transport.send(replyDest, failure);
                        } else if (reply != null) {
                            transport.send(replyDest, reply);
                        }
                    });
                } catch (Throwable t) {
                    log.atError()
                            .setCause(t)
                            .addKeyValue("worker_id", workerId)
                            .addKeyValue("service_name", serviceName)
                            .addKeyValue("correlation_id", rawEnvelope.correlationId())
                            .log("Uncaught error in worker handler execution");

                    String replyDest = rawEnvelope.replyDestination();
                    if (replyDest != null) {
                        WorkloadEnvelope<?> failure = WorkloadEnvelope.failure(
                                rawEnvelope.correlationId(),
                                serviceName,
                                rawEnvelope.metadata(),
                                t.getMessage() != null ? t.getMessage() : "Handler execution exception"
                        );
                        transport.send(replyDest, failure);
                    }
                }
            });
        });

        subscriptions.add(subscription);
    }
}
