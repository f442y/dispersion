package com.github.f442y.dispersion.server.standalone;

import com.github.f442y.dispersion.server.api.ControlPlaneServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

final class DispersionServerMainTest {

    private ControlPlaneServer server;
    private HttpClient client;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
        System.clearProperty("dispersion.server.port");
        System.clearProperty("dispersion.server.host");
        System.clearProperty("dispersion.base.path");
        System.clearProperty("dispersion.cors.origins");
    }

    @Test
    @DisplayName("DispersionServerMain boots with default discovery and serves API and Web UI")
    void shouldBootAndServeApiAndWebUi() throws Exception {
        System.setProperty("dispersion.server.port", "0");
        System.setProperty("dispersion.server.host", "127.0.0.1");
        System.setProperty("dispersion.base.path", "/api/v1");

        server = DispersionServerMain.start(new String[0]);
        assertThat(server.port()).isPositive();

        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        String baseUri = "http://127.0.0.1:" + server.port();

        // 1. Verify Node Health API endpoint
        HttpRequest apiRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/api/v1/node"))
                .GET()
                .build();
        HttpResponse<String> apiResponse = client.send(apiRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(apiResponse.statusCode()).isEqualTo(200);
        assertThat(apiResponse.body()).contains("\"status\"").contains("\"HEALTHY\"");

        // 2. Verify Root Web Dashboard endpoint
        HttpRequest rootRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/"))
                .GET()
                .build();
        HttpResponse<String> rootResponse = client.send(rootRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(rootResponse.statusCode()).isEqualTo(200);
        assertThat(rootResponse.headers().firstValue("content-type")).hasValueSatisfying(
                ct -> assertThat(ct).contains("text/html"));

        // 3. Verify SPA Deep Routing Fallback
        HttpRequest spaRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/machines/OrderWorkflow"))
                .GET()
                .build();
        HttpResponse<String> spaResponse = client.send(spaRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(spaResponse.statusCode()).isEqualTo(200);
        assertThat(spaResponse.headers().firstValue("content-type")).hasValueSatisfying(
                ct -> assertThat(ct).contains("text/html"));
    }
}
