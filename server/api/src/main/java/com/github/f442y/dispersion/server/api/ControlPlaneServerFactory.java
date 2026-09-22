package com.github.f442y.dispersion.server.api;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import org.jspecify.annotations.NonNull;

/**
 * Pluggable factory SPI for instantiating concrete {@link ControlPlaneServer} implementations.
 */
@FunctionalInterface
public interface ControlPlaneServerFactory {

    /**
     * Creates a new unstarted {@link ControlPlaneServer} with the specified configuration,
     * control plane, and JSON serializer.
     *
     * @param config The server configuration
     * @param controlPlane The underlying control plane instance
     * @param jsonSerializer The JSON serializer for marshaling/unmarshaling payloads
     * @return A configured {@link ControlPlaneServer}
     */
    @NonNull
    ControlPlaneServer create(
            @NonNull ServerConfig config,
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    );
}
