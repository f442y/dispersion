package com.github.f442y.dispersion.server.jakarta;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import com.github.f442y.dispersion.server.core.ControlPlaneHttpFacade;
import com.github.f442y.dispersion.server.core.HttpResponse;
import com.github.f442y.dispersion.server.core.SseSink;
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

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Universal, framework-agnostic Jakarta REST resource for the Dispersion Control Plane.
 * <p>
 * Exposes inspection, topology, execution history, external signal dispatching,
 * telemetry ingestion, node health diagnostics, and Server-Sent Events (SSE) streaming.
 * Adapts Jakarta REST 3.1 runtime to {@link ControlPlaneHttpFacade}.
 */
@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ControlPlaneResource {

    private final ControlPlaneHttpFacade facade;

    public ControlPlaneResource(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        this(new ControlPlaneHttpFacade(controlPlane, jsonSerializer, ServerConfig.defaultConfig()));
    }

    public ControlPlaneResource(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer,
            @NonNull ServerConfig serverConfig
    ) {
        this(new ControlPlaneHttpFacade(controlPlane, jsonSerializer, serverConfig));
    }

    public ControlPlaneResource(@NonNull ControlPlaneHttpFacade facade) {
        this.facade = Objects.requireNonNull(facade, "facade must not be null");
    }

    /**
     * Lists registered state machine topology descriptors.
     */
    @GET
    @Path("/machines")
    public Response listMachines() {
        return toResponse(facade.listMachines());
    }

    /**
     * Registers a new state machine descriptor with the control plane.
     */
    @POST
    @Path("/machines")
    public Response registerMachine(@NonNull String body) {
        return toResponse(facade.registerMachine(body));
    }

    /**
     * Gets a specific state machine descriptor by name.
     */
    @GET
    @Path("/machines/{name}")
    public Response getMachine(@PathParam("name") @NonNull String name) {
        return toResponse(facade.getMachine(name));
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
        return toResponse(facade.listExecutions(machineName, status, limit));
    }

    /**
     * Gets summary details for a specific execution ID.
     */
    @GET
    @Path("/executions/{id}")
    public Response getExecution(@PathParam("id") @NonNull String executionId) {
        return toResponse(facade.getExecution(executionId));
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
        return toResponse(facade.getExecutionTimeline(executionId, limit));
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
        return toResponse(facade.inspectCheckpoint(machineName, correlationKey));
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
        return toResponse(facade.dispatchExecution(machineName, input));
    }

    /**
     * Delivers an external signal to a suspended orchestration workflow.
     */
    @POST
    @Path("/executions/signal")
    public Response sendSignal(@NonNull String body) {
        return toResponse(facade.sendSignal(body));
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
        return toResponse(facade.ingestEvents(body));
    }

    /**
     * Returns node operational health, environment, clusterId, CPU, memory, and active engine counts.
     */
    @GET
    @Path("/node")
    public Response getNode() {
        return toResponse(facade.getNode("Jakarta REST (Java " + System.getProperty("java.version", "25") + ")"));
    }

    /**
     * Streams live telemetry events via Server-Sent Events (SSE).
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
        facade.startSseSession(new JakartaSseSink(eventSink, sse), machineName, executionId, tier, "dispersion-jakarta-sse-");
    }

    /**
     * Handles preflight CORS OPTIONS requests for the API resource.
     */
    @OPTIONS
    @Path("{any:.*}")
    public Response handleOptions() {
        return toResponse(facade.handleOptions());
    }

    private static Response toResponse(HttpResponse hr) {
        Response.ResponseBuilder builder = Response.status(hr.statusCode());
        if (hr.contentType() != null) {
            builder.type(hr.contentType());
        }
        hr.headers().forEach(builder::header);
        if (hr.bodyString() != null) {
            builder.entity(hr.bodyString());
        } else if (hr.bodyBytes() != null) {
            builder.entity(hr.bodyBytes());
        }
        return builder.build();
    }

    private record JakartaSseSink(SseEventSink eventSink, Sse sse) implements SseSink {

        @Override
        public void sendEvent(String eventName, String data) throws Exception {
            OutboundSseEvent event = sse.newEventBuilder()
                    .name(eventName)
                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                    .data(String.class, data)
                    .build();
            CompletionStage<?> stage = eventSink.send(event);
            if (stage != null) {
                stage.toCompletableFuture().join();
            }
        }

        @Override
        public void sendComment(String comment) throws Exception {
            OutboundSseEvent pingEvent = sse.newEventBuilder()
                    .comment(comment)
                    .build();
            CompletionStage<?> stage = eventSink.send(pingEvent);
            if (stage != null) {
                stage.toCompletableFuture().join();
            }
        }

        @Override
        public boolean isClosed() {
            return eventSink.isClosed();
        }

        @Override
        public void close() {
            try {
                eventSink.close();
            } catch (Exception _) {
            }
        }
    }
}
