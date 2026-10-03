package com.github.f442y.dispersion.server.core;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

import static java.util.Map.entry;

/**
 * Framework-agnostic static asset resolver engineered with strict path traversal defenses,
 * automatic SPA fallback routing to {@code index.html}, and classpath loading from {@code web/} and {@code static/}.
 */
public final class StaticAssetResolver {

    private static final Logger log = LoggerFactory.getLogger(StaticAssetResolver.class);
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

    public StaticAssetResolver() {
    }

    /**
     * Resolves the root SPA application index.
     *
     * @return {@link HttpResponse} containing the index.html or fallback welcome page
     */
    public @NonNull HttpResponse resolveRoot() {
        return resolveAsset(INDEX_HTML);
    }

    /**
     * Resolves an arbitrary static path or SPA client route.
     *
     * @param rawPath The relative URI path
     * @return {@link HttpResponse} representing 200 OK with asset, 400 Bad Request on traversal, or 404 Not Found
     */
    public @NonNull HttpResponse resolveAsset(@NonNull String rawPath) {
        Objects.requireNonNull(rawPath, "rawPath must not be null");

        if (rawPath.startsWith("api/") || rawPath.equals("api")) {
            return HttpResponse.notFoundJson("{\"error\": \"Not Found\"}");
        }

        if (isPathTraversal(rawPath)) {
            return HttpResponse.badRequestJson("{\"error\": \"Invalid path traversal attempt\"}");
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = getClass().getClassLoader();
        }

        // 1. Attempt to load exact asset
        byte[] assetBytes = readResource(cl, CLASSPATH_PREFIX + rawPath);
        if (assetBytes == null) {
            assetBytes = readResource(cl, FALLBACK_PREFIX + rawPath);
        }

        if (assetBytes != null) {
            String extension = getExtension(rawPath);
            String contentType = MIME_TYPES.getOrDefault(extension, "application/octet-stream");
            Map<String, String> headers = rawPath.endsWith(".html")
                    ? Map.of("Cache-Control", "no-cache")
                    : Map.of("Cache-Control", "public, max-age=31536000, immutable");
            return HttpResponse.okAsset(assetBytes, contentType, headers);
        }

        // 2. SPA client navigation fallback (routes without file extensions)
        if (!rawPath.contains(".")) {
            byte[] indexBytes = readResource(cl, CLASSPATH_PREFIX + INDEX_HTML);
            if (indexBytes == null) {
                indexBytes = readResource(cl, FALLBACK_PREFIX + INDEX_HTML);
            }
            if (indexBytes != null) {
                return HttpResponse.okAsset(indexBytes, "text/html; charset=UTF-8", Map.of("Cache-Control", "no-cache"));
            }
        }

        // 3. Default developer welcome dashboard if no packaged frontend bundle exists
        if (rawPath.isEmpty() || rawPath.equals(INDEX_HTML) || !rawPath.contains(".")) {
            return HttpResponse.okHtml(defaultWelcomeHtml(), Map.of("Cache-Control", "no-cache"));
        }

        return HttpResponse.notFoundJson("{\"error\": \"Resource not found: " + rawPath + "\"}");
    }

    /**
     * Checks if a requested path contains path traversal sequences or illegal characters.
     *
     * @param path The path string to inspect
     * @return {@code true} if traversal or illegal character detected
     */
    public static boolean isPathTraversal(@NonNull String path) {
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

    private static byte[] readResource(ClassLoader cl, String resourcePath) {
        try (InputStream is = cl.getResourceAsStream(resourcePath)) {
            if (is == null) {
                return null;
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int read;
            while ((read = is.read(buffer)) != -1) {
                baos.write(buffer, 0, read);
            }
            return baos.toByteArray();
        } catch (Exception ex) {
            log.trace("Failed reading classpath asset [{}]: {}", resourcePath, ex.getMessage());
            return null;
        }
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
                        <p>Dispersion Control Plane service is running.</p>
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
