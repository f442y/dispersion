package com.github.f442y.dispersion.routing.worker;

import com.github.f442y.dispersion.routing.policy.EndpointTags;
import org.jspecify.annotations.NonNull;

import java.util.Set;

public interface WorkerHost extends AutoCloseable {

    @NonNull
    String workerId();

    @NonNull
    EndpointTags tags();

    boolean isRunning();

    <WORKLOAD_INPUT, WORKLOAD_OUTPUT> void registerHandler(
            @NonNull String serviceName,
            @NonNull WorkloadHandler<WORKLOAD_INPUT, WORKLOAD_OUTPUT> handler
    );

    void unregisterHandler(@NonNull String serviceName);

    @NonNull
    Set<String> registeredServices();

    void start();

    void stop();

    @Override
    default void close() {
        stop();
    }
}
