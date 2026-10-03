package com.github.f442y.dispersion.examples.web;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import com.github.f442y.dispersion.server.jakarta.ControlPlaneCorsFilter;
import com.github.f442y.dispersion.server.jakarta.ControlPlaneResource;
import com.github.f442y.dispersion.server.jakarta.WebDashboardResource;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Spring Boot 4.1 Jersey configuration registering canonical Jakarta REST resources
 * and filters from {@code dispersion-server-jakarta}.
 */
@Component
public class JerseyConfig extends ResourceConfig {

    public JerseyConfig(
            ControlPlane controlPlane,
            JsonSerializer jsonSerializer,
            @Value("${server.port:8080}") int port
    ) {
        ServerConfig config = ServerConfig.builder()
                .port(port)
                .basePath("/api/v1")
                .environment("spring-boot-demo")
                .clusterId("dispersion-cluster")
                .build();

        register(new ControlPlaneResource(controlPlane, jsonSerializer, config));
        register(new ControlPlaneCorsFilter(config));
        register(new WebDashboardResource());
    }
}
