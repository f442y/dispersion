package com.github.f442y.dispersion.server.jakarta;

import com.github.f442y.dispersion.server.api.ServerConfig;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.util.Objects;

/**
 * Portable Jakarta REST CORS filter applying configured allowed origins and preflight headers.
 */
@Provider
@PreMatching
public class ControlPlaneCorsFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private final ServerConfig config;

    public ControlPlaneCorsFilter(@NonNull ServerConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    public ControlPlaneCorsFilter() {
        this(ServerConfig.defaultConfig());
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if ("OPTIONS".equalsIgnoreCase(requestContext.getMethod())) {
            Response.ResponseBuilder response = Response.noContent();
            addCorsHeaders(response);
            requestContext.abortWith(response.build());
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        if (!config.allowedOrigins().isEmpty()) {
            responseContext.getHeaders().putSingle("Access-Control-Allow-Origin", String.join(", ", config.allowedOrigins()));
            responseContext.getHeaders().putSingle("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
            responseContext.getHeaders().putSingle("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, X-Requested-With");
            responseContext.getHeaders().putSingle("Access-Control-Max-Age", "3600");
        }
    }

    private void addCorsHeaders(Response.ResponseBuilder response) {
        if (!config.allowedOrigins().isEmpty()) {
            response.header("Access-Control-Allow-Origin", String.join(", ", config.allowedOrigins()));
            response.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
            response.header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, X-Requested-With");
            response.header("Access-Control-Max-Age", "3600");
        }
    }
}
