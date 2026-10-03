package com.github.f442y.dispersion.server.jakarta;

import com.github.f442y.dispersion.server.core.HttpResponse;
import com.github.f442y.dispersion.server.core.StaticAssetResolver;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jspecify.annotations.NonNull;

import java.util.Objects;

/**
 * Jakarta REST resource for serving the embedded Dispersion Web Dashboard SPA and static assets.
 */
@Path("/")
public class WebDashboardResource {

    private final StaticAssetResolver assetResolver;

    public WebDashboardResource() {
        this(new StaticAssetResolver());
    }

    public WebDashboardResource(@NonNull StaticAssetResolver assetResolver) {
        this.assetResolver = Objects.requireNonNull(assetResolver, "assetResolver must not be null");
    }

    @GET
    @Produces(MediaType.WILDCARD)
    public Response getRoot() {
        return toResponse(assetResolver.resolveRoot());
    }

    @GET
    @Path("{path: [^?#]+}")
    @Produces(MediaType.WILDCARD)
    public Response getPath(@PathParam("path") @NonNull String path) {
        return toResponse(assetResolver.resolveAsset(path));
    }

    private static Response toResponse(HttpResponse hr) {
        Response.ResponseBuilder builder = Response.status(hr.statusCode());
        if (hr.contentType() != null) {
            builder.type(hr.contentType());
        }
        hr.headers().forEach(builder::header);
        if (hr.bodyString() != null) {
            builder.entity(hr.bodyString());
        } else if (hr.bodyBytes() != null) {
            builder.entity(hr.bodyBytes());
        }
        return builder.build();
    }
}
