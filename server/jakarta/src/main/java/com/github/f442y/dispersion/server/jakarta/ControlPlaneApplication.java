package com.github.f442y.dispersion.server.jakarta;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import jakarta.ws.rs.ApplicationPath;
import jakarta.ws.rs.core.Application;
import org.jspecify.annotations.NonNull;

import java.util.Objects;
import java.util.Set;

/**
 * Standard Jakarta REST {@link Application} descriptor registering Dispersion Control Plane resources.
 */
@ApplicationPath("/")
public class ControlPlaneApplication extends Application {

    private final Set<Object> singletons;

    public ControlPlaneApplication(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer,
            @NonNull ServerConfig serverConfig
    ) {
        Objects.requireNonNull(controlPlane, "controlPlane must not be null");
        Objects.requireNonNull(jsonSerializer, "jsonSerializer must not be null");
        Objects.requireNonNull(serverConfig, "serverConfig must not be null");

        this.singletons = Set.of(
                new ControlPlaneResource(controlPlane, jsonSerializer, serverConfig),
                new ControlPlaneCorsFilter(serverConfig),
                new WebDashboardResource()
        );
    }

    public ControlPlaneApplication(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        this(controlPlane, jsonSerializer, ServerConfig.defaultConfig());
    }

    @Override
    @SuppressWarnings("deprecation")
    public Set<Object> getSingletons() {
        return singletons;
    }
}
