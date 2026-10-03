package com.github.f442y.dispersion.server.jakarta;

import com.github.f442y.dispersion.server.api.ServerConfig;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WebDashboardResourceTest {

    @Test
    @DisplayName("WebDashboardResource serves HTML welcome page on root")
    void servesWelcomeHtml() {
        WebDashboardResource resource = new WebDashboardResource();
        Response rootResponse = resource.getRoot();
        assertThat(rootResponse.getStatus()).isEqualTo(200);
        assertThat(rootResponse.getMediaType().toString()).contains("text/html");

        Response spaRouteResponse = resource.getPath("executions");
        assertThat(spaRouteResponse.getStatus()).isEqualTo(200);
        assertThat(spaRouteResponse.getMediaType().toString()).contains("text/html");
    }

    @Test
    @DisplayName("WebDashboardResource returns 404 for api paths to allow JAX-RS api routing")
    void delegatesApiRoutes() {
        WebDashboardResource resource = new WebDashboardResource();
        Response apiResponse = resource.getPath("api/v1/machines");
        assertThat(apiResponse.getStatus()).isEqualTo(404);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "../secret.txt",
            "../../etc/passwd",
            "assets/../../secret",
            "..",
            "assets\\secret.js",
            "/leading/slash",
            "assets/\0nullbyte"
    })
    @DisplayName("WebDashboardResource rejects path traversal attempts with HTTP 400 Bad Request")
    void rejectsPathTraversalAttempts(String maliciousPath) {
        WebDashboardResource resource = new WebDashboardResource();
        Response response = resource.getPath(maliciousPath);

        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getMediaType().toString()).contains("application/json");
        assertThat(response.getEntity().toString()).contains("Invalid path traversal attempt");
    }

    @Test
    @DisplayName("ControlPlaneCorsFilter can be instantiated with custom or default config")
    void corsFilterInstantiation() {
        ServerConfig config = ServerConfig.builder()
                .allowedOrigins(List.of("http://localhost:3000"))
                .build();
        ControlPlaneCorsFilter filterWithConfig = new ControlPlaneCorsFilter(config);
        ControlPlaneCorsFilter defaultFilter = new ControlPlaneCorsFilter();

        assertThat(filterWithConfig).isNotNull();
        assertThat(defaultFilter).isNotNull();
    }
}
