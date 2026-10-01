package com.github.f442y.dispersion.server.standalone;

import io.helidon.http.HeaderNames;
import io.helidon.http.Method;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import io.helidon.webserver.staticcontent.ClasspathHandlerConfig;
import io.helidon.webserver.staticcontent.FileSystemHandlerConfig;
import io.helidon.webserver.staticcontent.StaticContentFeature;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Helidon {@link HttpService} that hosts the embedded React SPA Control Panel
 * dashboard static assets and performs Single-Page Application (SPA) fallback routing.
 * <p>
 * In local development mode, this service prioritizes files directly from the
 * local file system (e.g. {@code ui/dist}). If neither the file system dist
 * nor classpath static assets are found, it serves an informative developer guide
 * with exact instructions on how to build the UI with npm.
 */
final class WebDashboardStaticService implements HttpService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebDashboardStaticService.class);
    private static final String CLASSPATH_WEB_ROOT = "web";
    private static final String CLASSPATH_INDEX_HTML = "web/index.html";

    private final String apiPrefix;
    private final ClassLoader classLoader;
    private final @Nullable Path fileSystemDistPath;
    private final byte @Nullable [] indexHtmlBytes;
    private final @Nullable String indexEtag;

    WebDashboardStaticService() {
        this("/api", WebDashboardStaticService.class.getClassLoader(), null);
    }

    WebDashboardStaticService(@NonNull String apiPrefix) {
        this(apiPrefix, WebDashboardStaticService.class.getClassLoader(), null);
    }

    WebDashboardStaticService(@NonNull String apiPrefix, @NonNull ClassLoader classLoader) {
        this(apiPrefix, classLoader, null);
    }

    WebDashboardStaticService(
            @NonNull String apiPrefix,
            @NonNull ClassLoader classLoader,
            @Nullable Path fileSystemDistPath
    ) {
        this.apiPrefix = Objects.requireNonNull(apiPrefix, "apiPrefix must not be null");
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader must not be null");

        Path resolvedFsDist = fileSystemDistPath != null ? fileSystemDistPath : locateLocalUiDist();
        this.fileSystemDistPath = resolvedFsDist;

        byte[] loadedBytes = null;
        String loadedEtag = null;

        // 1. Try loading index.html from local file system directory (local dev mode)
        if (resolvedFsDist != null) {
            Path indexPath = resolvedFsDist.resolve("index.html");
            if (Files.isRegularFile(indexPath)) {
                try {
                    loadedBytes = Files.readAllBytes(indexPath);
                    loadedEtag = computeEtag(loadedBytes);
                    LOGGER.atInfo()
                            .addKeyValue("fileSystemPath", indexPath.toAbsolutePath().toString())
                            .addKeyValue("bytes", loadedBytes.length)
                            .addKeyValue("etag", loadedEtag)
                            .log("Loaded embedded Web Dashboard index.html from local file system");
                } catch (IOException ex) {
                    LOGGER.atWarn()
                            .setCause(ex)
                            .addKeyValue("fileSystemPath", indexPath.toString())
                            .log("Failed to read local Web Dashboard index.html from file system");
                }
            }
        }

        // 2. Fall back to classpath resource (packaged / standalone runtime)
        if (loadedBytes == null) {
            try (InputStream in = classLoader.getResourceAsStream(CLASSPATH_INDEX_HTML)) {
                if (in != null) {
                    loadedBytes = in.readAllBytes();
                    loadedEtag = computeEtag(loadedBytes);
                    LOGGER.atInfo()
                            .addKeyValue("resource", CLASSPATH_INDEX_HTML)
                            .addKeyValue("bytes", loadedBytes.length)
                            .addKeyValue("etag", loadedEtag)
                            .log("Cached embedded Web Dashboard index.html from classpath");
                } else {
                    LOGGER.atInfo()
                            .addKeyValue("resource", CLASSPATH_INDEX_HTML)
                            .log("Embedded Web Dashboard index.html not found on file system or classpath");
                }
            } catch (IOException ex) {
                LOGGER.atWarn()
                        .setCause(ex)
                        .addKeyValue("resource", CLASSPATH_INDEX_HTML)
                        .log("Failed to load and hash embedded Web Dashboard index.html from classpath");
            }
        }

        this.indexHtmlBytes = loadedBytes;
        this.indexEtag = loadedEtag;
    }

    @Override
    public void routing(HttpRules rules) {
        // Fast paths for root index.html to guarantee no-cache headers and ETag
        rules.get("/", this::handleIndex);
        rules.head("/", this::handleIndex);
        rules.get("/index.html", this::handleIndex);
        rules.head("/index.html", this::handleIndex);

        // Static asset serving: Local File System takes precedence if present
        if (fileSystemDistPath != null && Files.isDirectory(fileSystemDistPath)) {
            Path assetsPath = fileSystemDistPath.resolve("assets");
            if (Files.isDirectory(assetsPath)) {
                FileSystemHandlerConfig assetsConfig = FileSystemHandlerConfig.create(assetsPath);
                rules.register("/assets", StaticContentFeature.createService(assetsConfig));
            }

            FileSystemHandlerConfig rootConfig = FileSystemHandlerConfig.create(fileSystemDistPath);
            rules.register("/", StaticContentFeature.createService(rootConfig));
        }

        // Classpath static asset serving (fallback or packaged runtime)
        ClasspathHandlerConfig assetsConfig = ClasspathHandlerConfig.builder()
                .location(CLASSPATH_WEB_ROOT + "/assets")
                .classLoader(classLoader)
                .build();
        rules.register("/assets", StaticContentFeature.createService(assetsConfig));

        ClasspathHandlerConfig rootConfig = ClasspathHandlerConfig.builder()
                .location(CLASSPATH_WEB_ROOT)
                .classLoader(classLoader)
                .build();
        rules.register("/", StaticContentFeature.createService(rootConfig));

        // SPA Fallback and Route Isolation
        rules.get("/*", this::handleFallback);
        rules.head("/*", this::handleFallback);
    }

    private void handleIndex(ServerRequest req, ServerResponse res) {
        serveIndexHtml(req, res);
    }

    private void handleFallback(ServerRequest req, ServerResponse res) {
        String path = req.path().path();

        // 1. API routes must never fall back to HTML - return 404 JSON
        if (path.startsWith(apiPrefix) || path.startsWith("/api/")) {
            res.status(Status.NOT_FOUND_404)
                    .header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send("{\"error\":\"Not Found\",\"path\":\"" + path + "\"}");
            return;
        }

        // 2. Missing static assets (/assets/*) must never fall back to HTML - return 404 text
        if (path.startsWith("/assets/")) {
            res.status(Status.NOT_FOUND_404)
                    .header(HeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8")
                    .send("Asset not found: " + path);
            return;
        }

        // 3. SPA deep routes (/machines/*, /executions/*, etc.) fall back to index.html
        serveIndexHtml(req, res);
    }

    private void serveIndexHtml(ServerRequest req, ServerResponse res) {
        if (indexHtmlBytes == null || indexHtmlBytes.length == 0 || indexEtag == null) {
            String instructions = buildDevBuildInstructionsHtml();
            res.status(Status.NOT_FOUND_404)
                    .header(HeaderNames.CONTENT_TYPE, "text/html; charset=utf-8")
                    .send(instructions);
            return;
        }

        Optional<String> ifNoneMatch = req.headers().first(HeaderNames.IF_NONE_MATCH);
        if (ifNoneMatch.isPresent() && (ifNoneMatch.get().equals(indexEtag) || "*".equals(ifNoneMatch.get()))) {
            res.status(Status.NOT_MODIFIED_304)
                    .header(HeaderNames.ETAG, indexEtag)
                    .header(HeaderNames.CACHE_CONTROL, "no-cache, no-store, must-revalidate")
                    .send();
            return;
        }

        res.status(Status.OK_200)
                .header(HeaderNames.CONTENT_TYPE, "text/html; charset=utf-8")
                .header(HeaderNames.ETAG, indexEtag)
                .header(HeaderNames.CACHE_CONTROL, "no-cache, no-store, must-revalidate");

        if (req.prologue().method().equals(Method.HEAD)) {
            res.send();
        } else {
            res.send(indexHtmlBytes);
        }
    }

    private static @Nullable Path locateLocalUiDist() {
        // Candidate locations relative to current working directory (e.g. repo root or server/standalone)
        List<Path> candidates = List.of(
                Paths.get("ui", "dist"),
                Paths.get("..", "ui", "dist"),
                Paths.get("..", "..", "ui", "dist")
        );

        for (Path candidate : candidates) {
            if (Files.isDirectory(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
        }
        return null;
    }

    private static @NonNull String computeEtag(byte @NonNull [] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            return "\"" + HexFormat.of().formatHex(hash) + "\"";
        } catch (NoSuchAlgorithmException ex) {
            return "\"" + Integer.toHexString(bytes.length) + "\"";
        }
    }

    private static @NonNull String buildDevBuildInstructionsHtml() {
        return """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <title>Dispersion UI Not Built</title>
                    <style>
                        body {
                            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                            background-color: #0f172a;
                            color: #f8fafc;
                            display: flex;
                            align-items: center;
                            justify-content: center;
                            min-height: 100vh;
                            margin: 0;
                            padding: 24px;
                            box-sizing: border-box;
                        }
                        .container {
                            background-color: #1e293b;
                            border: 1px solid #334155;
                            border-radius: 8px;
                            padding: 32px;
                            max-width: 640px;
                            width: 100%;
                            box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5);
                        }
                        h1 {
                            font-size: 1.5rem;
                            margin-top: 0;
                            color: #38bdf8;
                            display: flex;
                            align-items: center;
                            gap: 8px;
                        }
                        p {
                            color: #94a3b8;
                            line-height: 1.6;
                            margin: 16px 0;
                        }
                        code {
                            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
                            background-color: #0f172a;
                            padding: 3px 6px;
                            border-radius: 4px;
                            color: #e2e8f0;
                            font-size: 0.9em;
                        }
                        pre {
                            background-color: #0f172a;
                            border: 1px solid #334155;
                            border-radius: 6px;
                            padding: 16px;
                            overflow-x: auto;
                            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
                            color: #38bdf8;
                            font-size: 0.95rem;
                            line-height: 1.5;
                        }
                        .footer {
                            margin-top: 24px;
                            padding-top: 16px;
                            border-top: 1px solid #334155;
                            font-size: 0.85rem;
                            color: #64748b;
                        }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>Web Dashboard Not Built</h1>
                        <p>
                            The Dispersion Control Plane server is running, but the frontend distribution assets
                            were not found in <code>ui/dist</code> or on the classpath (<code>web/index.html</code>).
                        </p>
                        <p>To compile the frontend application, run the following commands in your terminal:</p>
                        <pre>cd ui
                npm install
                npm run build</pre>
                        <p>
                            Once the build finishes, refresh this page to access the Dispersion Control Plane Dashboard.
                        </p>
                        <div class="footer">
                            Dispersion Standalone Control Plane &bull; Local Development Mode
                        </div>
                    </div>
                </body>
                </html>
                """;
    }
}
