package com.github.f442y.dispersion.server.jakarta;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.SignalRequest;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
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

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Universal, framework-agnostic Jakarta REST resource for the Dispersion Control Plane.
 * <p>
 * Exposes inspection, topology, execution history, external signal dispatching,
 * and Server-Sent Events (SSE) streaming. Compatible with Helidon, Quarkus, Micronaut,
 * Spring Boot (via Jersey), and embedded runtimes.
 */
@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ControlPlaneResource {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneResource.class);
    private static final Duration SSE_POLL_TIMEOUT = Duration.ofSeconds(1);

    private final ControlPlane controlPlane;
    private final JsonSerializer jsonSerializer;

    public ControlPlaneResource(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane must not be null");
        this.jsonSerializer = Objects.requireNonNull(jsonSerializer, "jsonSerializer must not be null");
    }

    /**
     * Lists all registered state machine topology descriptors.
     */
    @GET
    @Path("/machines")
    public Response listMachines() {
        List<MachineDescriptor> machines = controlPlane.listMachines();
        String json = jsonSerializer.serializeDescriptors(machines);
        return Response.ok(json).build();
    }

    /**
     * Fetches the topology descriptor for a specific state machine.
     */
    @GET
    @Path("/machines/{name}")
    public Response getMachine(@PathParam("name") @NonNull String name) {
        Optional<MachineDescriptor> descriptor = controlPlane.getMachine(name);
        if (descriptor.isPresent()) {
            return Response.ok(jsonSerializer.serializeDescriptor(descriptor.get())).build();
        }
        return Response.status(Response.Status.NOT_FOUND)
                .entity("{\"error\": \"Machine not found: " + name + "\"}")
                .build();
    }

    /**
     * Lists executions matching optional machine name and status filters.
     */
    @GET
    @Path("/executions")
    public Response listExecutions(
            @QueryParam("machine") @Nullable String machineName,
            @QueryParam("status") @Nullable String statusName,
            @QueryParam("limit") @DefaultValue("50") int limit
    ) {
        ExecutionStatus status = null;
        if (statusName != null && !statusName.isBlank()) {
            try {
                status = ExecutionStatus.valueOf(statusName.toUpperCase());
            } catch (IllegalArgumentException ex) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("{\"error\": \"Invalid execution status: " + statusParam(statusName) + "\"}")
                        .build();
            }
        }
        List<ExecutionSummary> executions = controlPlane.listExecutions(machineName, status, limit);
        String json = jsonSerializer.serializeSummaries(executions);
        return Response.ok(json).build();
    }

    /**
     * Retrieves the execution summary for a specific execution UUID.
     */
    @GET
    @Path("/executions/{id}")
    public Response getExecution(@PathParam("id") @NonNull String id) {
        Optional<ExecutionSummary> summary = controlPlane.getExecution(id);
        if (summary.isPresent()) {
            return Response.ok(jsonSerializer.serializeSummary(summary.get())).build();
        }
        return Response.status(Response.Status.NOT_FOUND)
                .entity("{\"error\": \"Execution not found: " + id + "\"}")
                .build();
    }

    /**
     * Retrieves the chronological event timeline for a specific execution instance.
     */
    @GET
    @Path("/executions/{id}/timeline")
    public Response getExecutionTimeline(@PathParam("id") @NonNull String id) {
        List<ExecutionEvent> timeline = controlPlane.getExecutionTimeline(id);
        String json = jsonSerializer.serializeEvents(timeline);
        return Response.ok(json).build();
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

        CompletableFuture<SignalDeliveryResult> future = controlPlane.sendSignal(
                request.machineName(),
                request.correlationKey(),
                request.signalName(),
                request.payload()
        );

        SignalDeliveryResult result = future.join();
        String json = jsonSerializer.serializeSignalResult(result);
        return Response.ok(json).build();
    }

    /**
     * Streams live telemetry events via Server-Sent Events (SSE).
     * Works with imperative pull-based {@link EventStream} on Java 25 virtual threads.
     */
    @GET
    @Path("/events/stream")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void streamEvents(
            @Context @NonNull SseEventSink eventSink,
            @Context @NonNull Sse sse,
            @QueryParam("machine") @Nullable String machineName,
            @QueryParam("executionId") @Nullable String executionId
    ) {
        Thread.ofVirtual().name("dispersion-sse-worker-", 0).start(() -> {
            try (eventSink; EventStream stream = openStream(machineName, executionId)) {
                while (!eventSink.isClosed() && !stream.isClosed()) {
                    try {
                        ExecutionEvent event = stream.poll(SSE_POLL_TIMEOUT);
                        if (event != null) {
                            String json = jsonSerializer.serializeEvent(event);
                            OutboundSseEvent sseEvent = sse.newEventBuilder()
                                    .name(event.getClass().getSimpleName())
                                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                                    .data(String.class, json)
                                    .build();
                            eventSink.send(sseEvent);
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (Exception ex) {
                log.debug("SSE stream closed or client disconnected: {}", ex.getMessage());
            }
        });
    }

    private EventStream openStream(@Nullable String machineName, @Nullable String executionId) {
        if (executionId != null && !executionId.isBlank()) {
            return controlPlane.watchExecution(executionId);
        }
        if (machineName != null && !machineName.isBlank()) {
            return controlPlane.watchMachine(machineName);
        }
        return controlPlane.listMachines().stream()
                .findFirst()
                .map(desc -> controlPlane.watchMachine(desc.name()))
                .orElseGet(() -> controlPlane.watchMachine(""));
    }

    private static String statusParam(String s) {
        return s.replace("\"", "\\\"");
    }
}
