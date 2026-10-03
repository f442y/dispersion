package com.github.f442y.dispersion.server.api;

import com.github.f442y.dispersion.control.ControlPlane;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Standard telemetry and operational health descriptor for a Dispersion Control Plane node.
 */
public record ControlPlaneNodeInfo(
        @NonNull String nodeId,
        @NonNull String host,
        int port,
        @NonNull String basePath,
        @NonNull String environment,
        @NonNull String clusterId,
        @NonNull String role,
        @NonNull String runtime,
        boolean virtualThreadsEnabled,
        long uptimeSeconds,
        int availableProcessors,
        int activeMachines,
        @NonNull String status
) {

    public ControlPlaneNodeInfo {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(host, "host must not be null");
        Objects.requireNonNull(basePath, "basePath must not be null");
        Objects.requireNonNull(environment, "environment must not be null");
        Objects.requireNonNull(clusterId, "clusterId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        Objects.requireNonNull(runtime, "runtime must not be null");
        Objects.requireNonNull(status, "status must not be null");
    }

    @NonNull
    public static ControlPlaneNodeInfo create(
            @NonNull ServerConfig config,
            @NonNull ControlPlane controlPlane,
            int boundPort,
            @Nullable Instant startedAt,
            @NonNull String runtimeDescription
    ) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(controlPlane, "controlPlane must not be null");
        Objects.requireNonNull(runtimeDescription, "runtimeDescription must not be null");

        long uptime = startedAt != null ? Duration.between(startedAt, Instant.now()).toSeconds() : 0L;
        int activeMachines = controlPlane.listMachines().size();
        int port = boundPort > 0 ? boundPort : config.port();

        return new ControlPlaneNodeInfo(
                "dispersion-node-" + port,
                config.host(),
                port,
                config.basePath(),
                config.environment(),
                config.clusterId(),
                "CONTROL_PLANE",
                runtimeDescription,
                true,
                uptime,
                Runtime.getRuntime().availableProcessors(),
                activeMachines,
                "HEALTHY"
        );
    }

    /**
     * Formats this descriptor as a standard JSON string.
     */
    @NonNull
    public String toJson() {
        return """
                {
                  "nodeId": "%s",
                  "host": "%s",
                  "port": %d,
                  "basePath": "%s",
                  "environment": "%s",
                  "clusterId": "%s",
                  "role": "%s",
                  "runtime": "%s",
                  "virtualThreadsEnabled": %b,
                  "uptimeSeconds": %d,
                  "availableProcessors": %d,
                  "activeMachines": %d,
                  "status": "%s"
                }""".formatted(
                nodeId,
                host,
                port,
                basePath,
                environment,
                clusterId,
                role,
                runtime,
                virtualThreadsEnabled,
                uptimeSeconds,
                availableProcessors,
                activeMachines,
                status
        );
    }
}
