package com.github.f442y.dispersion.routing.worker;

import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;

public record WorkloadEnvelope<PAYLOAD>(
        @NonNull String correlationId,
        @NonNull String serviceName,
        @NonNull Instant timestamp,
        @Nullable String replyDestination,
        @NonNull WorkloadMetadata metadata,
        @Nullable PAYLOAD payload,
        @Nullable String error
) {
    public WorkloadEnvelope {
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
    }

    @NonNull
    public static <PAYLOAD> WorkloadEnvelope<PAYLOAD> request(
            @NonNull WorkloadMetadata metadata,
            @Nullable PAYLOAD payload
    ) {
        Objects.requireNonNull(metadata, "metadata must not be null");
        return new WorkloadEnvelope<>(
                metadata.correlationId(),
                metadata.serviceName(),
                Instant.now(),
                metadata.replyDestination(),
                metadata,
                payload,
                null
        );
    }

    @NonNull
    public static <PAYLOAD> WorkloadEnvelope<PAYLOAD> success(
            @NonNull String correlationId,
            @NonNull String serviceName,
            @NonNull WorkloadMetadata metadata,
            @Nullable PAYLOAD payload
    ) {
        return new WorkloadEnvelope<>(
                correlationId,
                serviceName,
                Instant.now(),
                null,
                metadata,
                payload,
                null
        );
    }

    @NonNull
    public static <PAYLOAD> WorkloadEnvelope<PAYLOAD> failure(
            @NonNull String correlationId,
            @NonNull String serviceName,
            @NonNull WorkloadMetadata metadata,
            @NonNull String error
    ) {
        Objects.requireNonNull(error, "error must not be null");
        return new WorkloadEnvelope<>(
                correlationId,
                serviceName,
                Instant.now(),
                null,
                metadata,
                null,
                error
        );
    }

    public boolean isSuccess() {
        return error == null;
    }

    public boolean isFailure() {
        return error != null;
    }
}
