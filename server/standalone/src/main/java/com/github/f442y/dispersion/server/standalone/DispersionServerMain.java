package com.github.f442y.dispersion.server.standalone;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ControlPlaneProvider;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ControlPlaneServer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Production standalone entrypoint for the Dispersion Control Plane Microservice.
 * Boots the Helidon SE virtual-thread server hosting both the embedded React 19 UI
 * and the REST / SSE inspection APIs.
 */
public final class DispersionServerMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(DispersionServerMain.class);

    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final String DEFAULT_BASE_PATH = "/api/v1";
    private static final String DEFAULT_CORS_ORIGIN = "*";

    private DispersionServerMain() {}

    /**
     * Application main entry point.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        start(args);
    }

    /**
     * Configures and starts the standalone Control Plane server on virtual threads.
     *
     * @param args Optional command-line arguments
     * @return The running {@link ControlPlaneServer}
     */
    public static @NonNull ControlPlaneServer start(String[] args) {
        int port = resolveInt("dispersion.server.port", "DISPERSION_SERVER_PORT", DEFAULT_PORT);
        String host = resolveString("dispersion.server.host", "DISPERSION_SERVER_HOST", DEFAULT_HOST);
        String basePath = resolveString("dispersion.base.path", "DISPERSION_BASE_PATH", DEFAULT_BASE_PATH);
        String corsString = resolveString("dispersion.cors.origins", "DISPERSION_CORS_ORIGINS", DEFAULT_CORS_ORIGIN);
        String environment = resolveString("dispersion.env", "DISPERSION_ENV", ServerConfig.DEFAULT_ENVIRONMENT);
        String clusterId = resolveString("dispersion.cluster.id", "DISPERSION_CLUSTER_ID", ServerConfig.DEFAULT_CLUSTER_ID);

        List<String> corsOrigins = Arrays.stream(corsString.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        ServerConfig config = ServerConfig.builder()
                .port(port)
                .host(host)
                .basePath(basePath)
                .environment(environment)
                .clusterId(clusterId)
                .allowedOrigins(corsOrigins)
                .build();

        ControlPlane controlPlane = resolveControlPlane();
        JsonSerializer jsonSerializer = resolveJsonSerializer();

        HelidonControlPlaneServer server = new HelidonControlPlaneServer(config, controlPlane, jsonSerializer);
        server.start();

        int boundPort = server.port();
        LOGGER.atInfo()
                .addKeyValue("port", boundPort)
                .addKeyValue("host", host)
                .addKeyValue("basePath", basePath)
                .addKeyValue("environment", environment)
                .addKeyValue("clusterId", clusterId)
                .addKeyValue("webUi", "http://localhost:" + boundPort + "/")
                .addKeyValue("api", "http://localhost:" + boundPort + basePath)
                .log("Dispersion Control Plane Microservice is online");

        Runtime.getRuntime().addShutdownHook(Thread.ofVirtual()
                .name("dispersion-server-shutdown")
                .unstarted(() -> {
                    LOGGER.atInfo().log("Shutting down Dispersion Control Plane Server...");
                    server.stop();
                }));

        return server;
    }

    private static @NonNull ControlPlane resolveControlPlane() {
        Optional<ControlPlaneProvider> provider = ServiceLoader.load(ControlPlaneProvider.class).findFirst();
        if (provider.isPresent()) {
            return provider.get().create();
        }
        throw new IllegalStateException(
                "No ControlPlaneProvider found on modulepath or classpath. Ensure dispersion-control-plane-core is present.");
    }

    private static @NonNull JsonSerializer resolveJsonSerializer() {
        Optional<JsonSerializer> serializer = ServiceLoader.load(JsonSerializer.class).findFirst();
        if (serializer.isPresent()) {
            return serializer.get();
        }
        throw new IllegalStateException(
                "No JsonSerializer found on modulepath or classpath. Ensure dispersion-serialization-avaje is present.");
    }

    private static int resolveInt(String sysProp, String envVar, int defaultValue) {
        String val = System.getProperty(sysProp);
        if (val == null || val.isBlank()) {
            val = System.getenv(envVar);
        }
        if (val != null && !val.isBlank()) {
            try {
                return Integer.parseInt(val.trim());
            } catch (NumberFormatException ex) {
                LOGGER.atWarn()
                        .setCause(ex)
                        .addKeyValue("property", sysProp)
                        .addKeyValue("value", val)
                        .addKeyValue("default", defaultValue)
                        .log("Invalid numeric property; falling back to default");
            }
        }
        return defaultValue;
    }

    private static String resolveString(String sysProp, String envVar, String defaultValue) {
        String val = System.getProperty(sysProp);
        if (val == null || val.isBlank()) {
            val = System.getenv(envVar);
        }
        if (val != null && !val.isBlank()) {
            return val.trim();
        }
        return defaultValue;
    }
}
