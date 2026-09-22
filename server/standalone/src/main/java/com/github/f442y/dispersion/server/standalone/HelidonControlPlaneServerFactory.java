package com.github.f442y.dispersion.server.standalone;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ControlPlaneServer;
import com.github.f442y.dispersion.server.api.ControlPlaneServerFactory;
import com.github.f442y.dispersion.server.api.ServerConfig;
import org.jspecify.annotations.NonNull;

/**
 * ServiceLoader SPI factory providing {@link HelidonControlPlaneServer} instances.
 */
public final class HelidonControlPlaneServerFactory implements ControlPlaneServerFactory {

    public HelidonControlPlaneServerFactory() {}

    @Override
    @NonNull
    public ControlPlaneServer create(
            @NonNull ServerConfig config,
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        return new HelidonControlPlaneServer(config, controlPlane, jsonSerializer);
    }
}
