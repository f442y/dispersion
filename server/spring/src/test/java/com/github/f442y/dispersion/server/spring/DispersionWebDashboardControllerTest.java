package com.github.f442y.dispersion.server.spring;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

class DispersionWebDashboardControllerTest {

    private DispersionWebDashboardController controller;

    @BeforeEach
    void setUp() {
        controller = new DispersionWebDashboardController();
    }

    @Test
    @DisplayName("GET / returns 200 HTML landing page")
    void getRoot() {
        ResponseEntity<?> response = controller.getRoot();
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getHeaders().getContentType()).isNotNull();
        assertThat(response.getHeaders().getContentType().toString()).contains("text/html");
        assertThat(response.getBody().toString()).contains("Dispersion Control Plane");
    }

    @Test
    @DisplayName("GET /{path} rejects path traversal with 400 Bad Request")
    void getPathRejectsTraversal() {
        ResponseEntity<?> response = controller.getPath("..\\secret.txt");
        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().toString()).contains("Invalid path traversal attempt");
    }

    @Test
    @DisplayName("GET /{path} returns 404 for nonexistent file with extension")
    void getPathNotFound() {
        ResponseEntity<?> response = controller.getPath("missing-file.png");
        assertThat(response.getStatusCode().value()).isEqualTo(404);
        assertThat(response.getBody().toString()).contains("Resource not found");
    }

    @Test
    @DisplayName("GET /{path} serves SPA fallback for route without extension")
    void getPathSpaFallback() {
        ResponseEntity<?> response = controller.getPath("dashboard/analytics");
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getHeaders().getContentType().toString()).contains("text/html");
        assertThat(response.getBody().toString()).contains("Dispersion Control Plane");
    }
}
