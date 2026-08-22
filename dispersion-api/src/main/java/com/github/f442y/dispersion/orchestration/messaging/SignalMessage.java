package com.github.f442y.dispersion.orchestration.messaging;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Universal broker-agnostic message envelope for asynchronous event streaming and signal delivery.
 */
public record SignalMessage(
        @NonNull String destination,
        @NonNull String signalName,
        @Nullable String correlationKey,
        @NonNull UUID messageId,
        @NonNull Instant timestamp,
        @NonNull Map<String, String> headers,
        @Nullable Object payload
) {

    public SignalMessage {
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(signalName, "signalName must not be null");
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        headers = (headers != null) ? Map.copyOf(headers) : Collections.emptyMap();
    }

    public SignalMessage(
            @NonNull String destination,
            @NonNull String signalName,
            @Nullable String correlationKey,
            @Nullable Object payload
    ) {
        this(destination, signalName, correlationKey, UUID.randomUUID(), Instant.now(), Collections.emptyMap(), payload);
    }
}
