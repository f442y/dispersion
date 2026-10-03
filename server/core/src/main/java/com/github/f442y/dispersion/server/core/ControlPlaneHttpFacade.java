package com.github.f442y.dispersion.server.core;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.SignalRequest;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ControlPlaneNodeInfo;
import com.github.f442y.dispersion.server.api.ServerConfig;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Universal, framework-agnostic presentation facade for the Dispersion Control Plane.
 * <p>
 * Encapsulates all REST query dispatching, execution summaries, checkpoint introspection,
 * JSON error serialization, static UI dashboard asset resolution, and virtual-thread SSE orchestration.
 * Can be wrapped seamlessly by Jakarta REST resources, Spring MVC controllers, or raw socket listeners.
 */
public class ControlPlaneHttpFacade {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneHttpFacade.class);

    private final ControlPlane controlPlane;
    private final JsonSerializer jsonSerializer;
    private final ServerConfig serverConfig;
    private final StaticAssetResolver assetResolver;
    private final Instant startedAt;

    public ControlPlaneHttpFacade(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        this(controlPlane, jsonSerializer, ServerConfig.defaultConfig());
    }

    public ControlPlaneHttpFacade(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer,
            @NonNull ServerConfig serverConfig
    ) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane must not be null");
        this.jsonSerializer = Objects.requireNonNull(jsonSerializer, "jsonSerializer must not be null");
        this.serverConfig = Objects.requireNonNull(serverConfig, "serverConfig must not be null");
        this.assetResolver = new StaticAssetResolver();
        this.startedAt = Instant.now();
    }

    /**
     * Lists registered state machine topology descriptors.
     */
    public @NonNull HttpResponse listMachines() {
        List<MachineDescriptor> machineList = controlPlane.listMachines();
        String json = jsonSerializer.serializeDescriptors(machineList);
        return HttpResponse.okJson(json);
    }

    /**
     * Registers a new state machine descriptor with the control plane.
     */
    public @NonNull HttpResponse registerMachine(@NonNull String body) {
        try {
            MachineDescriptor descriptor = jsonSerializer.deserializeDescriptor(body);
            controlPlane.registerDescriptor(descriptor);
            return HttpResponse.okJson("{\"status\":\"REGISTERED\",\"machine\":\"" + descriptor.name() + "\"}");
        } catch (Exception ex) {
            log.error("Failed to register machine descriptor", ex);
            Throwable root = (ex.getCause() != null) ? ex.getCause() : ex;
            return HttpResponse.badRequestJson("{\"error\": \"Failed to register machine: " + escapeJson(root.getMessage()) + "\"}");
        }
    }

    /**
     * Gets a specific state machine descriptor by name.
     */
    public @NonNull HttpResponse getMachine(@NonNull String name) {
        return controlPlane.getMachine(name)
                .map(m -> HttpResponse.okJson(jsonSerializer.serializeDescriptor(m)))
                .orElseGet(() -> HttpResponse.notFoundJson("{\"error\": \"Machine [" + name + "] not found\"}"));
    }

    /**
     * Lists recent execution summaries across all state machines.
     */
    public @NonNull HttpResponse listExecutions(
            @Nullable String machineName,
            @Nullable String status,
            int limit
    ) {
        ExecutionStatus filterStatus = null;
        if (status != null && !status.isBlank()) {
            try {
                filterStatus = ExecutionStatus.valueOf(status.toUpperCase());
            } catch (IllegalArgumentException _) {
                return HttpResponse.badRequestJson("{\"error\": \"Unknown execution status: " + status + "\"}");
            }
        }

        List<ExecutionSummary> executions = controlPlane.listExecutions(machineName, filterStatus, limit);
        String json = jsonSerializer.serializeSummaries(executions);
        return HttpResponse.okJson(json);
    }

    /**
     * Gets summary details for a specific execution ID.
     */
    public @NonNull HttpResponse getExecution(@NonNull String executionId) {
        return controlPlane.getExecution(executionId)
                .map(e -> HttpResponse.okJson(jsonSerializer.serializeSummary(e)))
                .orElseGet(() -> HttpResponse.notFoundJson("{\"error\": \"Execution [" + executionId + "] not found\"}"));
    }

    /**
     * Retrieves the chronological event timeline for a specific execution ID.
     */
    public @NonNull HttpResponse getExecutionTimeline(@NonNull String executionId, int limit) {
        List<ExecutionEvent> timeline = controlPlane.getExecutionTimeline(executionId, limit);
        String json = jsonSerializer.serializeEvents(timeline);
        return HttpResponse.okJson(json);
    }

    /**
     * Inspects the latest checkpoint state snapshot for a specific execution.
     */
    public @NonNull HttpResponse inspectCheckpoint(@NonNull String machineName, @NonNull String correlationKey) {
        return controlPlane.inspectCheckpoint(machineName, correlationKey)
                .map(cp -> HttpResponse.okJson(formatCheckpointJson(cp)))
                .orElseGet(() -> HttpResponse.notFoundJson("{\"error\": \"Checkpoint for [" + machineName + "/" + correlationKey + "] not found\"}"));
    }

    /**
     * Asynchronously dispatches a new execution of a registered state machine.
     */
    public @NonNull HttpResponse dispatchExecution(@NonNull String machineName, @Nullable String input) {
        try {
            CompletableFuture<Object> future = controlPlane.dispatchExecution(machineName, input);
            Object result = future.join();
            String resultJson = formatResultJson(result);
            String responseJson = "{\"status\":\"DISPATCHED\",\"machine\":\"" + escapeJson(machineName) + "\",\"result\":"
                    + resultJson + "}";
            return HttpResponse.okJson(responseJson);
        } catch (Exception ex) {
            log.error("Failed to dispatch execution for machine [{}]", machineName, ex);
            Throwable root = (ex.getCause() != null) ? ex.getCause() : ex;
            return HttpResponse.badRequestJson("{\"error\": \"Failed to dispatch execution: " + escapeJson(root.getMessage()) + "\"}");
        }
    }

    /**
     * Delivers an external signal to a suspended orchestration workflow.
     */
    public @NonNull HttpResponse sendSignal(@NonNull String body) {
        SignalRequest request;
        try {
            request = jsonSerializer.deserializeSignalRequest(body);
        } catch (RuntimeException _) {
            return HttpResponse.badRequestJson("{\"error\": \"Malformed signal request payload\"}");
        }

        try {
            CompletableFuture<SignalDeliveryResult> future = controlPlane.sendSignal(
                    request.machineName(),
                    request.correlationKey(),
                    request.signalName(),
                    request.payload()
            );

            SignalDeliveryResult result = future.join();
            String json = jsonSerializer.serializeSignalResult(result);
            return HttpResponse.okJson(json);
        } catch (Exception ex) {
            log.error("Failed to deliver signal for machine [{}] key [{}]", request.machineName(), request.correlationKey(), ex);
            Throwable root = (ex.getCause() != null) ? ex.getCause() : ex;
            return HttpResponse.internalErrorJson("{\"error\": \"Failed to deliver signal: " + escapeJson(root.getMessage()) + "\"}");
        }
    }

    /**
     * Ingests a batch of polymorphic execution telemetry events from remote worker microservices.
     */
    public @NonNull HttpResponse ingestEvents(@NonNull String body) {
        try {
            List<ExecutionEvent> events = jsonSerializer.deserializeEvents(body);
            if (events == null || events.isEmpty()) {
                return HttpResponse.badRequestJson("{\"error\": \"Empty or missing events payload\"}");
            }

            for (ExecutionEvent event : events) {
                controlPlane.getEventListener().onEvent(event);
            }

            String responseJson = "{\"status\":\"INGESTED\",\"count\":" + events.size() + "}";
            return HttpResponse.okJson(responseJson);
        } catch (Exception ex) {
            log.error("Failed to ingest execution events batch", ex);
            Throwable root = (ex.getCause() != null) ? ex.getCause() : ex;
            return HttpResponse.badRequestJson("{\"error\": \"Failed to ingest events: " + escapeJson(root.getMessage()) + "\"}");
        }
    }

    /**
     * Returns node operational health, environment, clusterId, CPU, memory, and active engine counts.
     */
    public @NonNull HttpResponse getNode() {
        return getNode(null);
    }

    /**
     * Returns node operational health, environment, clusterId, CPU, memory, and active engine counts
     * with an optional runtime label.
     */
    public @NonNull HttpResponse getNode(@Nullable String runtimeLabel) {
        String label = (runtimeLabel != null && !runtimeLabel.isBlank())
                ? runtimeLabel
                : "Dispersion Core (Java " + System.getProperty("java.version", "25") + ")";
        ControlPlaneNodeInfo nodeInfo = ControlPlaneNodeInfo.create(
                serverConfig,
                controlPlane,
                serverConfig.port(),
                startedAt,
                label
        );
        return HttpResponse.okJson(nodeInfo.toJson());
    }

    /**
     * Starts an SSE session on a virtual thread using the provided {@link SseSink}.
     */
    public @NonNull Thread startSseSession(
            @NonNull SseSink sink,
            @Nullable String machineName,
            @Nullable String executionId,
            @Nullable String tier
    ) {
        return startSseSession(sink, machineName, executionId, tier, "dispersion-sse-");
    }

    /**
     * Starts an SSE session on a virtual thread with a custom thread prefix.
     */
    public @NonNull Thread startSseSession(
            @NonNull SseSink sink,
            @Nullable String machineName,
            @Nullable String executionId,
            @Nullable String tier,
            @NonNull String threadPrefix
    ) {
        return ControlPlaneSseSession.start(controlPlane, jsonSerializer, sink, machineName, executionId, tier, threadPrefix);
    }

    /**
     * Resolves the root SPA application index HTML.
     */
    public @NonNull HttpResponse resolveRoot() {
        return assetResolver.resolveRoot();
    }

    /**
     * Resolves an arbitrary static path or SPA client route.
     */
    public @NonNull HttpResponse resolveAsset(@NonNull String rawPath) {
        return assetResolver.resolveAsset(rawPath);
    }

    /**
     * Returns a 204 No Content for preflight CORS OPTIONS requests.
     */
    public @NonNull HttpResponse handleOptions() {
        return HttpResponse.noContent();
    }

    public @NonNull ControlPlane getControlPlane() {
        return controlPlane;
    }

    public @NonNull JsonSerializer getJsonSerializer() {
        return jsonSerializer;
    }

    public @NonNull ServerConfig getServerConfig() {
        return serverConfig;
    }

    public @NonNull StaticAssetResolver getAssetResolver() {
        return assetResolver;
    }

    private String formatCheckpointJson(@NonNull Object cp) {
        if (cp instanceof String s) {
            String trimmed = s.trim();
            if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                return trimmed;
            }
            return jsonSerializer.serialize(trimmed);
        }
        try {
            return jsonSerializer.serialize(cp);
        } catch (Exception _) {
            if (cp.getClass().isRecord()) {
                return formatRecordJson(cp);
            }
            return jsonSerializer.serialize(cp.toString());
        }
    }

    private String formatResultJson(@Nullable Object result) {
        if (result == null) {
            return "null";
        }
        if (result instanceof String s) {
            String trimmed = s.trim();
            if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                return trimmed;
            }
            return jsonSerializer.serialize(trimmed);
        }
        try {
            return jsonSerializer.serialize(result);
        } catch (Exception _) {
            if (result.getClass().isRecord()) {
                return formatRecordJson(result);
            }
            return jsonSerializer.serialize(result.toString());
        }
    }

    private String formatRecordJson(@NonNull Object record) {
        StringBuilder sb = new StringBuilder("{");
        RecordComponent[] components = record.getClass().getRecordComponents();
        if (components != null) {
            for (int i = 0; i < components.length; i++) {
                RecordComponent rc = components[i];
                if (i > 0) sb.append(",");
                sb.append("\"").append(rc.getName()).append("\":");
                try {
                    Object val = rc.getAccessor().invoke(record);
                    if (val == null) {
                        sb.append("null");
                    } else if (val instanceof Number || val instanceof Boolean) {
                        sb.append(val);
                    } else if (val instanceof String str) {
                        String trimmed = str.trim();
                        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                            sb.append(trimmed);
                        } else {
                            sb.append(jsonSerializer.serialize(str));
                        }
                    } else {
                        sb.append(jsonSerializer.serialize(val.toString()));
                    }
                } catch (Exception _) {
                    sb.append("null");
                }
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static String escapeJson(@Nullable String text) {
        if (text == null) return "";
        return text.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
