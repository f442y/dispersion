package com.github.f442y.dispersion.server.standalone;

import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.serialization.avaje.AvajeJsonSerializer;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ControlPlaneServer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import io.helidon.webserver.WebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class WebDashboardStaticServiceTest {

    private DefaultControlPlane controlPlane;
    private JsonSerializer serializer;
    private ControlPlaneServer server;
    private HttpClient client;
    private String serverRootUri;
    private WebServer fallbackServer;

    @BeforeEach
    void setUp() {
        controlPlane = new DefaultControlPlane();
        serializer = new AvajeJsonSerializer();

        ServerConfig config = ServerConfig.builder()
                .host("127.0.0.1")
                .port(0)
                .basePath("/api/v1")
                .allowedOrigins(List.of("*"))
                .build();

        server = ControlPlaneServer.create(config, controlPlane, serializer);
        server.start();

        serverRootUri = "http://127.0.0.1:" + server.port();
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    @AfterEach
    void tearDown() {
        if (server != null && server.isRunning()) {
            server.stop();
        }
        if (fallbackServer != null && fallbackServer.isRunning()) {
            fallbackServer.stop();
        }
    }

    @Test
    @DisplayName("Root URL (/) serves index.html with ETag and no-cache headers")
    void shouldServeRootIndexHtmlWithNoCacheAndEtag() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                ct -> assertThat(ct).contains("text/html")
        );
        assertThat(response.headers().firstValue("ETag")).isPresent();
        assertThat(response.headers().firstValue("Cache-Control"))
                .contains("no-cache, no-store, must-revalidate");
        assertThat(response.body()).contains("Dispersion Control Panel");
    }

    @Test
    @DisplayName("Direct /index.html serves index.html with ETag and no-cache headers")
    void shouldServeIndexHtmlDirectly() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/index.html"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                ct -> assertThat(ct).contains("text/html")
        );
        assertThat(response.body()).contains("Dispersion Control Panel");
    }

    @Test
    @DisplayName("Conditional GET with matching If-None-Match yields HTTP 304 Not Modified")
    void shouldReturn304NotModifiedWhenEtagMatches() throws Exception {
        HttpRequest initialRequest = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/"))
                .GET()
                .build();

        HttpResponse<String> initialResponse = client.send(initialRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(initialResponse.statusCode()).isEqualTo(200);
        String etag = initialResponse.headers().firstValue("ETag").orElseThrow();

        HttpRequest conditionalRequest = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/"))
                .header("If-None-Match", etag)
                .GET()
                .build();

        HttpResponse<String> conditionalResponse = client.send(conditionalRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(conditionalResponse.statusCode()).isEqualTo(304);
        assertThat(conditionalResponse.headers().firstValue("ETag")).hasValue(etag);
        assertThat(conditionalResponse.body()).isEmpty();
    }

    @Test
    @DisplayName("Static assets in /assets/ are served directly")
    void shouldServeStaticAssetsFromAssetsDirectory() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/assets/test.js"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("Dispersion UI Test Asset");
    }

    @Test
    @DisplayName("Static assets at root like /favicon.svg are served")
    void shouldServeStaticFileFromWebRoot() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/favicon.svg"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("<svg");
    }

    @Test
    @DisplayName("Deep client SPA routes fall back to index.html with HTTP 200")
    void shouldFallbackToSpaForDeepClientRoutes() throws Exception {
        List<String> routes = List.of(
                "/machines",
                "/machines/OrderWorkflow",
                "/executions",
                "/executions/01928374-uuid",
                "/overview",
                "/settings"
        );

        for (String route : routes) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(serverRootUri + route))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                    ct -> assertThat(ct).contains("text/html")
            );
            assertThat(response.body()).contains("Dispersion Control Panel");
        }
    }

    @Test
    @DisplayName("API routes never fall back to index.html and return 404 JSON")
    void shouldReturn404JsonForMissingApiRoutes() throws Exception {
        List<String> apiRoutes = List.of(
                "/api/v1/nonexistent",
                "/api/v1/machines/nonexistent/invalid",
                "/api/v2/foo"
        );

        for (String route : apiRoutes) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(serverRootUri + route))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(404);
            assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                    ct -> assertThat(ct).contains("application/json")
            );
            assertThat(response.body()).contains("\"error\":\"Not Found\"");
        }
    }

    @Test
    @DisplayName("Missing static assets under /assets/ return 404 text, not HTML fallback")
    void shouldReturn404TextForMissingStaticAssets() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/assets/missing-bundle-xyz.js"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("Asset not found: /assets/missing-bundle-xyz.js");
    }

    @Test
    @DisplayName("HEAD requests on index.html and fallback routes return headers without body")
    void shouldSupportHeadRequests() throws Exception {
        HttpRequest rootHead = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> rootResponse = client.send(rootHead, HttpResponse.BodyHandlers.ofString());
        assertThat(rootResponse.statusCode()).isEqualTo(200);
        assertThat(rootResponse.headers().firstValue("ETag")).isPresent();
        assertThat(rootResponse.body()).isEmpty();

        HttpRequest spaHead = HttpRequest.newBuilder()
                .uri(URI.create(serverRootUri + "/machines/OrderWorkflow"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> spaResponse = client.send(spaHead, HttpResponse.BodyHandlers.ofString());
        assertThat(spaResponse.statusCode()).isEqualTo(200);
        assertThat(spaResponse.headers().firstValue("ETag")).isPresent();
        assertThat(spaResponse.body()).isEmpty();
    }

    @Test
    @DisplayName("When index.html is absent from file system and classpath, returns developer build guide")
    void shouldHandleMissingIndexHtmlGracefully() throws Exception {
        ClassLoader emptyClassLoader = new URLClassLoader(new URL[0], null);
        Path nonExistentPath = Path.of("target", "does-not-exist-dist");
        WebDashboardStaticService service = new WebDashboardStaticService("/api", emptyClassLoader, nonExistentPath);

        fallbackServer = WebServer.builder()
                .host("127.0.0.1")
                .port(0)
                .routing(routing -> routing.register("/", service))
                .build()
                .start();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + fallbackServer.port() + "/machines"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                ct -> assertThat(ct).contains("text/html")
        );
        assertThat(response.body())
                .contains("Web Dashboard Not Built")
                .contains("cd ui")
                .contains("npm run build");
    }
}
