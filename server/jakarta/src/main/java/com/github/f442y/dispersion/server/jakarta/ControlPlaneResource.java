package com.github.f442y.dispersion.server.jakarta;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.SignalRequest;
import com.github.f442y.dispersion.event.DynamicTapManager;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.EventTier;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ControlPlaneNodeInfo;
import com.github.f442y.dispersion.server.api.ServerConfig;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.OPTIONS;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/**
 * Universal, framework-agnostic Jakarta REST resource for the Dispersion Control Plane.
 * <p>
 * Exposes inspection, topology, execution history, external signal dispatching,
 * telemetry ingestion, node health diagnostics, and Server-Sent Events (SSE) streaming.
 * Compatible with Helidon, Quarkus, Micronaut, Spring Boot (via Jersey), and embedded runtimes.
 */
@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ControlPlaneResource {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneResource.class);
    private static final Duration SSE_POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration SSE_HEARTBEAT_INTERVAL = Duration.ofSeconds(15);

    private final ControlPlane controlPlane;
    private final JsonSerializer jsonSerializer;
    private final ServerConfig serverConfig;
    private final Instant startedAt;

    public ControlPlaneResource(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        this(controlPlane, jsonSerializer, ServerConfig.defaultConfig());
    }

    public ControlPlaneResource(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer,
            @NonNull ServerConfig serverConfig
    ) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane must not be null");
        this.jsonSerializer = Objects.requireNonNull(jsonSerializer, "jsonSerializer must not be null");
        this.serverConfig = Objects.requireNonNull(serverConfig, "serverConfig must not be null");
        this.startedAt = Instant.now();
    }

    /**
     * Lists registered state machine topology descriptors.
     */
    @GET
    @Path("/machines")
    public Response listMachines() {
        List<MachineDescriptor> machineList = controlPlane.listMachines();
        String json = jsonSerializer.serializeDescriptors(machineList);
        return Response.ok(json).build();
    }

    /**
     * Registers a new state machine descriptor with the control plane.
     */
    @POST
    @Path("/machines")
    public Response registerMachine(@NonNull String body) {
        try {
            MachineDescriptor descriptor = jsonSerializer.deserializeDescriptor(body);
            controlPlane.registerDescriptor(descriptor);
            return Response.ok("{\"status\":\"REGISTERED\",\"machine\":\"" + descriptor.name() + "\"}").build();
        } catch (Exception ex) {
            log.error("Failed to register machine descriptor", ex);
            Throwable root = (ex.getCause() != null) ? ex.getCause() : ex;
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\": \"Failed to register machine: " + escapeJson(root.getMessage()) + "\"}")
                    .build();
        }
    }

    /**
     * Gets a specific state machine descriptor by name.
     */
    @GET
    @Path("/machines/{name}")
    public Response getMachine(@PathParam("name") @NonNull String name) {
        return controlPlane.getMachine(name)
                .map(m -> Response.ok(jsonSerializer.serializeDescriptor(m)).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity("{\"error\": \"Machine [" + name + "] not found\"}")
                        .build());
    }

    /**
     * Lists recent execution summaries across all state machines.
     */
    @GET
    @Path("/executions")
    public Response listExecutions(
            @QueryParam("machine") @Nullable String machineName,
            @QueryParam("status") @Nullable String status,
            @QueryParam("limit") @DefaultValue("50") int limit
    ) {
        ExecutionStatus filterStatus = (status != null && !status.isBlank())
                ? ExecutionStatus.valueOf(status.toUpperCase())
                : null;

        List<ExecutionSummary> executions = controlPlane.listExecutions(machineName, filterStatus, limit);
        String json = jsonSerializer.serializeSummaries(executions);
        return Response.ok(json).build();
    }

    /**
     * Gets summary details for a specific execution ID.
     */
    @GET
    @Path("/executions/{id}")
    public Response getExecution(@PathParam("id") @NonNull String executionId) {
        return controlPlane.getExecution(executionId)
                .map(e -> Response.ok(jsonSerializer.serializeSummary(e)).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity("{\"error\": \"Execution [" + executionId + "] not found\"}")
                        .build());
    }

    /**
     * Retrieves the chronological event timeline for a specific execution ID using default limit.
     */
    public Response getExecutionTimeline(@NonNull String executionId) {
        return getExecutionTimeline(executionId, 100);
    }

    /**
     * Retrieves the chronological event timeline for a specific execution ID.
     */
    @GET
    @Path("/executions/{id}/timeline")
    public Response getExecutionTimeline(
            @PathParam("id") @NonNull String executionId,
            @QueryParam("limit") @DefaultValue("100") int limit
    ) {
        List<ExecutionEvent> timeline = controlPlane.getExecutionTimeline(executionId, limit);
        String json = jsonSerializer.serializeEvents(timeline);
        return Response.ok(json).build();
    }

    /**
     * Inspects the latest checkpoint state snapshot for a specific execution.
     */
    @GET
    @Path("/executions/{machine}/{key}/checkpoint")
    public Response inspectCheckpoint(
            @PathParam("machine") @NonNull String machineName,
            @PathParam("key") @NonNull String correlationKey
    ) {
        return controlPlane.inspectCheckpoint(machineName, correlationKey)
                .map(cp -> Response.ok(formatCheckpointJson(cp)).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity("{\"error\": \"Checkpoint for [" + machineName + "/" + correlationKey + "] not found\"}")
                        .build());
    }

    /**
     * Asynchronously dispatches a new execution of a registered state machine.
     */
    @POST
    @Path("/machines/{name}/dispatch")
    public Response dispatchExecution(
            @PathParam("name") @NonNull String machineName,
            @Nullable String input
    ) {
        try {
            CompletableFuture<Object> future = controlPlane.dispatchExecution(machineName, input);
            Object result = future.join();
            String resultJson = formatResultJson(result);
            String responseJson = "{\"status\":\"DISPATCHED\",\"machine\":\"" + escapeJson(machineName) + "\",\"result\":"
                    + resultJson + "}";
            return Response.ok(responseJson).build();
        } catch (Exception ex) {
            log.error("Failed to dispatch execution for machine [{}]", machineName, ex);
            Throwable root = (ex.getCause() != null) ? ex.getCause() : ex;
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\": \"Failed to dispatch execution: " + escapeJson(root.getMessage()) + "\"}")
                    .build();
        }
    }

    /**
     * Delivers an external signal to a suspended orchestration workflow.
     */
    @POST
    @Path("/executions/signal")
    public Response sendSignal(@NonNull String body) {
        SignalRequest request;
        try {
            request = jsonSerializer.deserializeSignalRequest(body);
        } catch (RuntimeException ex) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\": \"Malformed signal request payload\"}")
                    .build();
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
            return Response.ok(json).build();
        } catch (Exception ex) {
            log.error("Failed to deliver signal for machine [{}] key [{}]", request.machineName(), request.correlationKey(), ex);
            Throwable root = (ex.getCause() != null) ? ex.getCause() : ex;
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("{\"error\": \"Failed to deliver signal: " + escapeJson(root.getMessage()) + "\"}")
                    .build();
        }
    }

    /**
     * Routes an external signal to an active state machine instance (alias).
     */
    @POST
    @Path("/signals")
    public Response sendSignalAlias(@NonNull String body) {
        return sendSignal(body);
    }

    /**
     * Ingests a batch of polymorphic execution telemetry events from remote worker microservices.
     */
    @POST
    @Path("/telemetry/events")
    public Response ingestEvents(@NonNull String body) {
        try {
            List<ExecutionEvent> events = jsonSerializer.deserializeEvents(body);
            if (events == null || events.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("{\"error\": \"Empty or missing events payload\"}")
                        .build();
            }

            for (ExecutionEvent event : events) {
                controlPlane.getEventListener().onEvent(event);
            }

            String responseJson = "{\"status\":\"INGESTED\",\"count\":" + events.size() + "}";
            return Response.ok(responseJson).build();
        } catch (Exception ex) {
            log.error("Failed to ingest execution events batch", ex);
            Throwable root = (ex.getCause() != null) ? ex.getCause() : ex;
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\": \"Failed to ingest events: " + escapeJson(root.getMessage()) + "\"}")
                    .build();
        }
    }

    /**
     * Returns node operational health, environment, clusterId, CPU, memory, and active engine counts.
     */
    @GET
    @Path("/node")
    public Response getNode() {
        ControlPlaneNodeInfo nodeInfo = ControlPlaneNodeInfo.create(
                serverConfig,
                controlPlane,
                serverConfig.port(),
                startedAt,
                "Jakarta REST (Java " + System.getProperty("java.version", "25") + ")"
        );
        return Response.ok(nodeInfo.toJson()).build();
    }

    /**
     * Streams live telemetry events via Server-Sent Events (SSE).
     * Works with imperative pull-based {@link EventStream} on Java 25 virtual threads.
     *
     * <p>Tiered telemetry behaviors:</p>
     * <ul>
     *   <li>Without query parameters: Streams strictly {@link EventTier#LIFECYCLE} events across all machines.</li>
     *   <li>With {@code ?machine={name}}: Streams lifecycle events for that machine (or all tiers if {@code tier=all}).</li>
     *   <li>With {@code ?executionId={id}&tier=all}: Registers a dynamic live tap with the {@link DynamicTapManager}
     *       for {@code {id}}, streaming both lifecycle and granular events. When the connection closes, the tap is
     *       immediately unregistered.</li>
     * </ul>
     */
    @GET
    @Path("/events/stream")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void streamEvents(
            @Context @NonNull SseEventSink eventSink,
            @Context @NonNull Sse sse,
            @QueryParam("machine") @Nullable String machineName,
            @QueryParam("executionId") @Nullable String executionId,
            @QueryParam("tier") @DefaultValue("lifecycle") @Nullable String tier
    ) {
        boolean streamAllTiers = "all".equalsIgnoreCase(tier);
        UUID machineUuid = parseUuidOrNull(executionId);
        Optional<DynamicTapManager> tapManagerOpt = controlPlane.getTapManager();
        DynamicTapManager tapManager = (streamAllTiers && machineUuid != null && tapManagerOpt.isPresent())
                ? tapManagerOpt.get()
                : null;

        if (tapManager != null) {
            tapManager.registerTap(machineUuid);
        }

        EventStream stream = openStream(machineName, executionId, streamAllTiers);

        Thread.ofVirtual().name("dispersion-jakarta-sse-", 0).start(() -> {
            try (eventSink; stream) {
                long lastHeartbeat = System.currentTimeMillis();
                while (!eventSink.isClosed() && !stream.isClosed()) {
                    try {
                        long now = System.currentTimeMillis();
                        if (now - lastHeartbeat >= SSE_HEARTBEAT_INTERVAL.toMillis()) {
                            // 1. Renew dynamic tap lease while client is connected without incrementing subscriber count
                            if (tapManager != null && machineUuid != null) {
                                tapManager.renewTap(machineUuid);
                            }
                            // 2. Transmit SSE comment frame to probe client connection & keep reverse proxies alive
                            OutboundSseEvent pingEvent = sse.newEventBuilder()
                                    .comment("ping")
                                    .build();
                            CompletionStage<?> pingStage = eventSink.send(pingEvent);
                            if (pingStage != null) {
                                pingStage.toCompletableFuture().join();
                            }
                            lastHeartbeat = now;
                        }

                        ExecutionEvent event = stream.poll(SSE_POLL_TIMEOUT);
                        if (event != null) {
                            String json = jsonSerializer.serializeEvent(event);
                            OutboundSseEvent sseEvent = sse.newEventBuilder()
                                    .name("message")
                                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                                    .data(String.class, json)
                                    .build();
                            CompletionStage<?> sendStage = eventSink.send(sseEvent);
                            if (sendStage != null) {
                                sendStage.toCompletableFuture().join();
                            }
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
                log.debug("SSE stream closed: {}", ex.getMessage());
            } finally {
                if (tapManager != null && machineUuid != null) {
                    tapManager.unregisterTap(machineUuid);
                }
            }
        });
    }

    /**
     * Handles preflight CORS OPTIONS requests for the API resource.
     */
    @OPTIONS
    @Path("{any:.*}")
    public Response handleOptions() {
        return Response.noContent().build();
    }

    private EventStream openStream(
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
                if (i > 0) {
                    sb.append(",");
                }
                sb.append("\"").append(rc.getName()).append("\":");
                try {
                    Object val = rc.getAccessor().invoke(record);
                    if (val == null) {
                        sb.append("null");
                    } else if (val instanceof Number || val instanceof Boolean) {
                        sb.append(val);
                    } else if (val.getClass().isEnum()) {
                        sb.append("\"").append(((Enum<?>) val).name()).append("\"");
                    } else {
                        sb.append("\"").append(escapeJson(val.toString())).append("\"");
                    }
                } catch (Exception _) {
                    sb.append("null");
                }
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static @Nullable UUID parseUuidOrNull(@Nullable String id) {
        if (id == null || id.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException _) {
            return null;
        }
    }

    private static String escapeJson(@Nullable String str) {
        if (str == null) {
            return "";
        }
        return str.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
