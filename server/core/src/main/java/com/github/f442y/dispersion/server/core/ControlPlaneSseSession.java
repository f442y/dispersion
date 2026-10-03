package com.github.f442y.dispersion.server.core;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.event.DynamicTapManager;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Manages an active Server-Sent Events (SSE) streaming session on a Java 25 virtual thread,
 * handling pull-based event consumption, 15-second keep-alive ping frames, and automatic
 * {@link DynamicTapManager} lease acquisition, renewal, and teardown.
 */
public final class ControlPlaneSseSession {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneSseSession.class);
    private static final Duration SSE_HEARTBEAT_INTERVAL = Duration.ofSeconds(15);
    private static final Duration SSE_POLL_TIMEOUT = Duration.ofMillis(200);

    private ControlPlaneSseSession() {
    }

    /**
     * Spawns a dedicated virtual thread to run an SSE streaming session to completion.
     *
     * @param controlPlane   The active control plane instance
     * @param jsonSerializer JSON codec for serializing execution events
     * @param sink           The framework-specific SSE sink
     * @param machineName    Optional machine name filter
     * @param executionId    Optional execution ID filter
     * @param tier           Telemetry tier ("lifecycle" or "all")
     * @return The running virtual {@link Thread}
     */
    public static @NonNull Thread start(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer,
            @NonNull SseSink sink,
            @Nullable String machineName,
            @Nullable String executionId,
            @Nullable String tier
    ) {
        return start(controlPlane, jsonSerializer, sink, machineName, executionId, tier, "dispersion-sse-session-");
    }

    /**
     * Spawns a dedicated virtual thread to run an SSE streaming session with a custom thread prefix.
     */
    public static @NonNull Thread start(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer,
            @NonNull SseSink sink,
            @Nullable String machineName,
            @Nullable String executionId,
            @Nullable String tier,
            @NonNull String threadPrefix
    ) {
        Objects.requireNonNull(controlPlane, "controlPlane must not be null");
        Objects.requireNonNull(jsonSerializer, "jsonSerializer must not be null");
        Objects.requireNonNull(sink, "sink must not be null");
        Objects.requireNonNull(threadPrefix, "threadPrefix must not be null");

        boolean streamAllTiers = "all".equalsIgnoreCase(tier);
        UUID machineUuid = parseUuidOrNull(executionId);
        Optional<DynamicTapManager> tapManagerOpt = controlPlane.getTapManager();
        DynamicTapManager tapManager = (streamAllTiers && machineUuid != null && tapManagerOpt.isPresent())
                ? tapManagerOpt.get()
                : null;

        if (tapManager != null) {
            tapManager.registerTap(machineUuid);
        }

        EventStream stream = openStream(controlPlane, machineName, executionId, streamAllTiers);

        return Thread.ofVirtual().name(threadPrefix, 0).start(() -> {
            try (sink; stream) {
                long lastHeartbeat = System.currentTimeMillis();
                while (!sink.isClosed() && !stream.isClosed()) {
                    try {
                        long now = System.currentTimeMillis();
                        if (now - lastHeartbeat >= SSE_HEARTBEAT_INTERVAL.toMillis()) {
                            if (tapManager != null && machineUuid != null) {
                                tapManager.renewTap(machineUuid);
                            }
                            sink.sendComment("ping");
                            lastHeartbeat = now;
                        }

                        ExecutionEvent event = stream.poll(SSE_POLL_TIMEOUT);
                        if (event != null) {
                            String json = jsonSerializer.serializeEvent(event);
                            sink.sendEvent("message", json);
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception ex) {
                        log.debug("SSE client disconnected or write failed: {}", ex.getMessage());
                        break;
                    }
                }
            } catch (Exception ex) {
                log.debug("SSE session completed or closed: {}", ex.getMessage());
            } finally {
                if (tapManager != null && machineUuid != null) {
                    try {
                        tapManager.unregisterTap(machineUuid);
                    } catch (Exception ex) {
                        log.warn("Failed to unregister dynamic tap for [{}]", machineUuid, ex);
                    }
                }
            }
        });
    }

    private static EventStream openStream(
            ControlPlane controlPlane,
            @Nullable String machineName,
            @Nullable String executionId,
            boolean streamAllTiers
    ) {
        Predicate<ExecutionEvent> predicate;
        if (executionId != null && !executionId.isBlank()) {
            predicate = streamAllTiers
                    ? event -> executionId.equals(event.machineId().toString())
                    : event -> executionId.equals(event.machineId().toString()) && event.isLifecycle();
        } else if (machineName != null && !machineName.isBlank()) {
            predicate = streamAllTiers
                    ? event -> machineName.equals(event.machineName())
                    : event -> machineName.equals(event.machineName()) && event.isLifecycle();
        } else {
            predicate = streamAllTiers
                    ? _ -> true
                    : ExecutionEvent::isLifecycle;
        }
        return controlPlane.openStream(predicate);
    }

    private static @Nullable UUID parseUuidOrNull(@Nullable String id) {
        if (id == null || id.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(id.trim());
        } catch (IllegalArgumentException _) {
            return null;
        }
    }
}
