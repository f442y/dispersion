package com.github.f442y.dispersion.server.api;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import org.jspecify.annotations.NonNull;

import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Universal interface for the Dispersion Control Plane HTTP server.
 * <p>
 * Allows hosting applications to start, stop, query port bindings, and lifecycle of the server,
 * regardless of whether it runs on a standalone embedded JDK server or inside an external framework.
 */
public interface ControlPlaneServer extends AutoCloseable {

    /**
     * Starts the HTTP server and begins listening for incoming requests.
     */
    void start();

    /**
     * Stops the HTTP server gracefully.
     */
    void stop();

    /**
     * Returns the actual local port the server is bound to (useful when configured with port {@code 0}).
     *
     * @return The bound port number
     */
    int port();

    /**
     * Returns {@code true} if the server is actively listening for requests.
     *
     * @return {@code true} if active
     */
    boolean isRunning();

    /**
     * Returns the base context path configured for the server (e.g., {@code "/api/v1"}).
     *
     * @return The base path
     */
    @NonNull
    String basePath();

    @Override
    default void close() {
        stop();
    }

    /**
     * Discovers and creates a {@link ControlPlaneServer} using {@link ServiceLoader}.
     *
     * @param config The server configuration
     * @param controlPlane The underlying control plane
     * @param jsonSerializer The JSON serializer
     * @return A configured {@link ControlPlaneServer}
     * @throws IllegalStateException If no {@link ControlPlaneServerFactory} provider is found on the classpath
     */
    @NonNull
    static ControlPlaneServer create(
            @NonNull ServerConfig config,
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(controlPlane, "controlPlane must not be null");
        Objects.requireNonNull(jsonSerializer, "jsonSerializer must not be null");

        return ServiceLoader.load(ControlPlaneServerFactory.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No ControlPlaneServerFactory found on the classpath. Ensure dispersion-server-standalone or an adapter is added."
                ))
                .create(config, controlPlane, jsonSerializer);
    }

    /**
     * Convenience method to create a {@link ControlPlaneServer} with default configuration and default JSON serializer.
     *
     * @param controlPlane The underlying control plane
     * @return A configured {@link ControlPlaneServer}
     */
    @NonNull
    static ControlPlaneServer createDefault(@NonNull ControlPlane controlPlane) {
        return create(ServerConfig.defaultConfig(), controlPlane, JsonSerializer.load());
    }
}
