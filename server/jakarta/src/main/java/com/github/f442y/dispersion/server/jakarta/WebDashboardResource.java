package com.github.f442y.dispersion.server.jakarta;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;

import static java.util.Map.entry;

/**
 * Jakarta REST resource for serving the embedded Dispersion Web Dashboard SPA and static assets.
 */
@Path("/")
public class WebDashboardResource {

    private static final Logger log = LoggerFactory.getLogger(WebDashboardResource.class);
    private static final String CLASSPATH_PREFIX = "web/";
    private static final String FALLBACK_PREFIX = "static/";
    private static final String INDEX_HTML = "index.html";

    private static final Map<String, String> MIME_TYPES = Map.ofEntries(
            entry("html", "text/html; charset=UTF-8"),
            entry("js", "application/javascript; charset=UTF-8"),
            entry("mjs", "application/javascript; charset=UTF-8"),
            entry("css", "text/css; charset=UTF-8"),
            entry("svg", "image/svg+xml"),
            entry("png", "image/png"),
            entry("jpg", "image/jpeg"),
            entry("jpeg", "image/jpeg"),
            entry("ico", "image/x-icon"),
            entry("json", "application/json; charset=UTF-8"),
            entry("woff2", "font/woff2"),
            entry("woff", "font/woff")
    );

    public WebDashboardResource() {
    }

    @GET
    @Produces(MediaType.WILDCARD)
    public Response getRoot() {
        return serveResource(INDEX_HTML);
    }

    @GET
    @Path("{path: [^?#]+}")
    @Produces(MediaType.WILDCARD)
    public Response getPath(@PathParam("path") @NonNull String path) {
        if (path.startsWith("api/") || path.equals("api")) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        if (isPathTraversal(path)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\": \"Invalid path traversal attempt\"}")
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

        return serveResource(path);
    }

    private Response serveResource(@NonNull String rawPath) {
        if (isPathTraversal(rawPath)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\": \"Invalid path traversal attempt\"}")
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = getClass().getClassLoader();
        }

        InputStream is = cl.getResourceAsStream(CLASSPATH_PREFIX + rawPath);
        if (is == null) {
            is = cl.getResourceAsStream(FALLBACK_PREFIX + rawPath);
        }

        if (is != null) {
            String extension = getExtension(rawPath);
            String contentType = MIME_TYPES.getOrDefault(extension, MediaType.APPLICATION_OCTET_STREAM);
            Response.ResponseBuilder builder = Response.ok(is, contentType);
            if (!rawPath.endsWith(".html")) {
                builder.header("Cache-Control", "public, max-age=31536000, immutable");
            } else {
                builder.header("Cache-Control", "no-cache");
            }
            return builder.build();
        }

        // SPA fallback for HTML client navigation routes (paths without extension)
        if (!rawPath.contains(".")) {
            InputStream indexStream = cl.getResourceAsStream(CLASSPATH_PREFIX + INDEX_HTML);
            if (indexStream == null) {
                indexStream = cl.getResourceAsStream(FALLBACK_PREFIX + INDEX_HTML);
            }
            if (indexStream != null) {
                return Response.ok(indexStream, "text/html; charset=UTF-8")
                        .header("Cache-Control", "no-cache")
                        .build();
            }
        }

        // Default developer welcome dashboard if no web bundle is packaged
        if (rawPath.isEmpty() || rawPath.equals(INDEX_HTML) || !rawPath.contains(".")) {
            return Response.ok(defaultWelcomeHtml(), "text/html; charset=UTF-8")
                    .header("Cache-Control", "no-cache")
                    .build();
        }

        return Response.status(Response.Status.NOT_FOUND)
                .entity("{\"error\": \"Resource not found: " + rawPath + "\"}")
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    private static boolean isPathTraversal(@NonNull String path) {
        if (path.contains("..") || path.contains("\\") || path.indexOf('\0') != -1 || path.startsWith("/")) {
            return true;
        }
        try {
            java.nio.file.Path normalized = Paths.get(path).normalize();
            if (normalized.startsWith("..") || normalized.isAbsolute()) {
                return true;
            }
        } catch (Exception e) {
            return true;
        }
        return false;
    }

    private static String getExtension(String path) {
        int dot = path.lastIndexOf('.');
        if (dot >= 0 && dot < path.length() - 1) {
            return path.substring(dot + 1).toLowerCase();
        }
        return "";
    }

    private static String defaultWelcomeHtml() {
        return """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <title>Dispersion Control Plane</title>
                    <style>
                        body { font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #f8fafc; padding: 2rem; }
                        .card { background: #1e293b; border-radius: 8px; padding: 1.5rem; max-width: 600px; margin: 2rem auto; }
                        h1 { color: #38bdf8; font-size: 1.5rem; }
                        a { color: #38bdf8; text-decoration: none; }
                        a:hover { text-decoration: underline; }
                        code { background: #334155; padding: 0.2rem 0.4rem; border-radius: 4px; }
                    </style>
                </head>
                <body>
                    <div class="card">
                        <h1>⚡ Dispersion Control Plane Active</h1>
                        <p>Jakarta REST Control Plane service is running.</p>
                        <p>Explore API endpoints:</p>
                        <ul>
                            <li><a href="/api/v1/node"><code>GET /api/v1/node</code></a> (Node diagnostics)</li>
                            <li><a href="/api/v1/machines"><code>GET /api/v1/machines</code></a> (Registered state machines)</li>
                            <li><a href="/api/v1/executions"><code>GET /api/v1/executions</code></a> (Execution summaries)</li>
                        </ul>
                    </div>
                </body>
                </html>
                """;
    }
}
