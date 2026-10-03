package com.github.f442y.dispersion.server.core;

import org.jspecify.annotations.NonNull;

/**
 * Universal abstraction for transmitting Server-Sent Events (SSE) across diverse web frameworks
 * (such as Jakarta REST {@code SseEventSink}, Spring MVC {@code SseEmitter}, or raw sockets).
 */
public interface SseSink extends AutoCloseable {

    /**
     * Transmits a named data event frame to the connected client.
     *
     * @param eventName The event type identifier (e.g. {@code "message"})
     * @param data      The payload data (typically JSON string)
     * @throws Exception if writing to the underlying transport fails
     */
    void sendEvent(@NonNull String eventName, @NonNull String data) throws Exception;

    /**
     * Transmits an SSE comment frame (e.g. {@code : ping\n\n}) to probe connection liveness
     * and prevent reverse proxy timeouts.
     *
     * @param comment The comment body text (e.g. {@code "ping"})
     * @throws Exception if writing to the underlying transport fails
     */
    void sendComment(@NonNull String comment) throws Exception;

    /**
     * Checks if the sink is already closed or disconnected.
     *
     * @return {@code true} if closed
     */
    boolean isClosed();

    /**
     * Closes the sink connection.
     */
    @Override
    void close();
}
