package com.github.f442y.dispersion.server.spring;

import com.github.f442y.dispersion.server.core.StaticAssetResolver;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.Objects;

import static com.github.f442y.dispersion.server.spring.DispersionControlPlaneController.toResponseEntity;

/**
 * Native Spring MVC controller for serving the embedded Dispersion Web Dashboard SPA and static assets.
 */
@Controller
public class DispersionWebDashboardController {

    private final StaticAssetResolver assetResolver;

    public DispersionWebDashboardController() {
        this(new StaticAssetResolver());
    }

    @Autowired
    public DispersionWebDashboardController(@Autowired(required = false) @Nullable StaticAssetResolver assetResolver) {
        this.assetResolver = (assetResolver != null) ? assetResolver : new StaticAssetResolver();
    }

    /**
     * Resolves the root SPA application index HTML.
     */
    @GetMapping("/")
    public ResponseEntity<?> getRoot() {
        return toResponseEntity(assetResolver.resolveRoot());
    }

    /**
     * Resolves static assets and provides client-side SPA fallback routing.
     */
    @GetMapping("/{*path}")
    public ResponseEntity<?> getPath(@PathVariable(value = "path", required = false) @Nullable String path) {
        String cleanPath = (path != null && path.startsWith("/")) ? path.substring(1) : (path != null ? path : "");
        if (cleanPath.isEmpty()) {
            return toResponseEntity(assetResolver.resolveRoot());
        }
        if (cleanPath.startsWith("api/") || cleanPath.equals("api")) {
            return ResponseEntity.notFound().build();
        }
        return toResponseEntity(assetResolver.resolveAsset(cleanPath));
    }
}
