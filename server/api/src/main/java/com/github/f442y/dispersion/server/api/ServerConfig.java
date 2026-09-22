package com.github.f442y.dispersion.server.api;

import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Immutable configuration parameters for a {@link ControlPlaneServer}.
 *
 * @param host The network interface hostname or IP to bind (e.g., {@code "0.0.0.0"} or {@code "localhost"})
 * @param port The port to bind, or {@code 0} to bind an ephemeral random available port
 * @param basePath The base HTTP context path prefix (defaults to {@code "/api/v1"})
 * @param backlog The socket listen backlog (0 or negative for system default)
 * @param shutdownGracePeriod The grace period to wait for in-flight requests during server shutdown
 * @param allowedOrigins List of allowed CORS origin strings, or {@code "*"}
 */
public record ServerConfig(
        @NonNull String host,
        int port,
        @NonNull String basePath,
        int backlog,
        @NonNull Duration shutdownGracePeriod,
        @NonNull List<String> allowedOrigins
) {

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_BASE_PATH = "/api/v1";
    public static final int DEFAULT_BACKLOG = 0;
    public static final Duration DEFAULT_GRACE_PERIOD = Duration.ofSeconds(5);
    public static final List<String> DEFAULT_ALLOWED_ORIGINS = List.of("*");

    public ServerConfig {
        Objects.requireNonNull(host, "host must not be null");
        Objects.requireNonNull(basePath, "basePath must not be null");
        Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod must not be null");
        Objects.requireNonNull(allowedOrigins, "allowedOrigins must not be null");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535, got: " + port);
        }
        if (!basePath.startsWith("/")) {
            throw new IllegalArgumentException("basePath must start with '/', got: " + basePath);
        }
        allowedOrigins = List.copyOf(allowedOrigins);
    }

    @NonNull
    public static ServerConfig defaultConfig() {
        return new ServerConfig(
                DEFAULT_HOST,
                DEFAULT_PORT,
                DEFAULT_BASE_PATH,
                DEFAULT_BACKLOG,
                DEFAULT_GRACE_PERIOD,
                DEFAULT_ALLOWED_ORIGINS
        );
    }

    @NonNull
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private String basePath = DEFAULT_BASE_PATH;
        private int backlog = DEFAULT_BACKLOG;
        private Duration shutdownGracePeriod = DEFAULT_GRACE_PERIOD;
        private List<String> allowedOrigins = DEFAULT_ALLOWED_ORIGINS;

        private Builder() {}

        @NonNull
        public Builder host(@NonNull String host) {
            this.host = Objects.requireNonNull(host, "host must not be null");
            return this;
        }

        @NonNull
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        @NonNull
        public Builder basePath(@NonNull String basePath) {
            this.basePath = Objects.requireNonNull(basePath, "basePath must not be null");
            return this;
        }

        @NonNull
        public Builder backlog(int backlog) {
            this.backlog = backlog;
            return this;
        }

        @NonNull
        public Builder shutdownGracePeriod(@NonNull Duration shutdownGracePeriod) {
            this.shutdownGracePeriod = Objects.requireNonNull(shutdownGracePeriod, "shutdownGracePeriod must not be null");
            return this;
        }

        @NonNull
        public Builder allowedOrigins(@NonNull List<String> allowedOrigins) {
            this.allowedOrigins = Objects.requireNonNull(allowedOrigins, "allowedOrigins must not be null");
            return this;
        }

        @NonNull
        public ServerConfig build() {
            return new ServerConfig(host, port, basePath, backlog, shutdownGracePeriod, allowedOrigins);
        }
    }
}
