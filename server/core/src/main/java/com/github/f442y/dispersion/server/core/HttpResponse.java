package com.github.f442y.dispersion.server.core;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * Framework-agnostic HTTP response value object containing status code, content type,
 * optional string/byte payload, and response headers.
 */
public record HttpResponse(
        int statusCode,
        @NonNull String contentType,
        @Nullable String bodyString,
        byte @Nullable [] bodyBytes,
        @NonNull Map<String, String> headers
) {
    public HttpResponse {
        Objects.requireNonNull(contentType, "contentType must not be null");
        headers = (headers == null || headers.isEmpty()) ? Map.of() : Map.copyOf(headers);
    }

    /**
     * Creates a 200 OK JSON response with UTF-8 encoding.
     */
    public static @NonNull HttpResponse okJson(@NonNull String json) {
        return new HttpResponse(200, "application/json; charset=UTF-8", json, null, Map.of());
    }

    /**
     * Creates a 400 Bad Request JSON error response.
     */
    public static @NonNull HttpResponse badRequestJson(@NonNull String json) {
        return new HttpResponse(400, "application/json; charset=UTF-8", json, null, Map.of());
    }

    /**
     * Creates a 404 Not Found JSON error response.
     */
    public static @NonNull HttpResponse notFoundJson(@NonNull String json) {
        return new HttpResponse(404, "application/json; charset=UTF-8", json, null, Map.of());
    }

    /**
     * Creates a 500 Internal Server Error JSON response.
     */
    public static @NonNull HttpResponse internalErrorJson(@NonNull String json) {
        return new HttpResponse(500, "application/json; charset=UTF-8", json, null, Map.of());
    }

    /**
     * Creates a 204 No Content response.
     */
    public static @NonNull HttpResponse noContent() {
        return new HttpResponse(204, "text/plain", null, null, Map.of());
    }

    /**
     * Creates a 200 OK response for an arbitrary binary or text asset with custom headers.
     */
    public static @NonNull HttpResponse okAsset(
            byte @NonNull [] bytes,
            @NonNull String contentType,
            @NonNull Map<String, String> headers
    ) {
        Objects.requireNonNull(bytes, "bytes must not be null");
        return new HttpResponse(200, contentType, null, bytes, headers);
    }

    /**
     * Creates a 200 OK HTML text response.
     */
    public static @NonNull HttpResponse okHtml(@NonNull String html, @NonNull Map<String, String> headers) {
        return new HttpResponse(200, "text/html; charset=UTF-8", html, null, headers);
    }

    /**
     * Returns the body bytes, converting {@link #bodyString()} to UTF-8 bytes if needed.
     */
    public byte @NonNull [] getBytes() {
        if (bodyBytes != null) {
            return bodyBytes;
        }
        if (bodyString != null) {
            return bodyString.getBytes(StandardCharsets.UTF_8);
        }
        return new byte[0];
    }
}
