package com.github.f442y.dispersion.server.standalone;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.SignalRequest;
import com.github.f442y.dispersion.event.EventStream;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ControlPlaneServer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lightweight, high-performance standalone HTTP server powered by Helidon SE (Níma) WebServer,
 * running entirely on Java virtual threads.
 */
public final class HelidonControlPlaneServer implements ControlPlaneServer {

    private static final Logger log = LoggerFactory.getLogger(HelidonControlPlaneServer.class);
    private static final Duration SSE_POLL_TIMEOUT = Duration.ofSeconds(1);

    private final ServerConfig config;
    private final ControlPlane controlPlane;
    private final JsonSerializer jsonSerializer;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private WebServer server;
    private int boundPort;

    public HelidonControlPlaneServer(
            @NonNull ServerConfig config,
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane must not be null");
        this.jsonSerializer = Objects.requireNonNull(jsonSerializer, "jsonSerializer must not be null");
    }

    @Override
    public synchronized void start() {
        if (running.get()) {
            return;
        }

        String basePath = sanitizeBasePath(config.basePath());

        server = WebServer.builder()
                .port(config.port())
                .routing(routing -> routing.register(basePath, this::configureRoutes))
                .build()
                .start();

        boundPort = server.port();
        running.set(true);

        log.info("Dispersion Control Plane Server (Helidon SE / Níma) started on port {} with base path '{}'",
                boundPort, basePath);
    }

    @Override
    public synchronized void stop() {
        if (!running.get() || server == null) {
            return;
        }

        log.info("Stopping Dispersion Control Plane Server on port {}", boundPort);
        server.stop();
        running.set(false);
    }

    @Override
    public int port() {
        return boundPort;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    @NonNull
    public String basePath() {
        return config.basePath();
    }

    private void configureRoutes(HttpRules rules) {
        rules.get("/machines", this::handleListMachines)
                .get("/machines/{name}", this::handleGetMachine)
                .get("/executions", this::handleListExecutions)
                .get("/executions/{id}", this::handleGetExecution)
                .get("/executions/{id}/timeline", this::handleGetExecutionTimeline)
                .post("/executions/signal", this::handleSendSignal)
                .get("/events/stream", this::handleEventStream);
    }

    private void handleListMachines(ServerRequest req, ServerResponse res) {
        addCorsHeaders(res);
        List<MachineDescriptor> machines = controlPlane.listMachines();
        String json = jsonSerializer.serializeDescriptors(machines);
        res.header(HeaderNames.CONTENT_TYPE, "application/json")
                .send(json.getBytes(StandardCharsets.UTF_8));
    }

    private void handleGetMachine(ServerRequest req, ServerResponse res) {
        addCorsHeaders(res);
        String name = req.path().pathParameters().get("name");
        Optional<MachineDescriptor> descriptor = controlPlane.getMachine(name);
        if (descriptor.isPresent()) {
            String json = jsonSerializer.serializeDescriptor(descriptor.get());
            res.header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(json.getBytes(StandardCharsets.UTF_8));
        } else {
            res.status(Status.NOT_FOUND_404)
                    .header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(("{\"error\": \"Machine not found: " + name + "\"}").getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleListExecutions(ServerRequest req, ServerResponse res) {
        addCorsHeaders(res);
        String machineName = req.query().first("machine").orElse(null);
        String statusName = req.query().first("status").orElse(null);
        int limit = req.query().first("limit").map(Integer::parseInt).orElse(50);

        ExecutionStatus status = null;
        if (statusName != null && !statusName.isBlank()) {
            try {
                status = ExecutionStatus.valueOf(statusName.toUpperCase());
            } catch (IllegalArgumentException ignored) {
                res.status(Status.BAD_REQUEST_400)
                        .header(HeaderNames.CONTENT_TYPE, "application/json")
                        .send(("{\"error\": \"Unknown ExecutionStatus: " + statusName + "\"}").getBytes(StandardCharsets.UTF_8));
                return;
            }
        }

        List<ExecutionSummary> summaries = controlPlane.listExecutions(machineName, status, limit);
        String json = jsonSerializer.serializeSummaries(summaries);
        res.header(HeaderNames.CONTENT_TYPE, "application/json")
                .send(json.getBytes(StandardCharsets.UTF_8));
    }

    private void handleGetExecution(ServerRequest req, ServerResponse res) {
        addCorsHeaders(res);
        String idStr = req.path().pathParameters().get("id");
        Optional<ExecutionSummary> summary = controlPlane.getExecution(idStr);
        if (summary.isPresent()) {
            String json = jsonSerializer.serializeSummary(summary.get());
            res.header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(json.getBytes(StandardCharsets.UTF_8));
        } else {
            res.status(Status.NOT_FOUND_404)
                    .header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(("{\"error\": \"Execution not found: " + idStr + "\"}").getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleGetExecutionTimeline(ServerRequest req, ServerResponse res) {
        addCorsHeaders(res);
        String idStr = req.path().pathParameters().get("id");
        List<ExecutionEvent> timeline = controlPlane.getExecutionTimeline(idStr);
        String json = jsonSerializer.serializeEvents(timeline);
        res.header(HeaderNames.CONTENT_TYPE, "application/json")
                .send(json.getBytes(StandardCharsets.UTF_8));
    }

    private void handleSendSignal(ServerRequest req, ServerResponse res) {
        addCorsHeaders(res);
        try {
            byte[] bodyBytes = req.content().inputStream().readAllBytes();
            String body = new String(bodyBytes, StandardCharsets.UTF_8);

            SignalRequest request = jsonSerializer.deserializeSignalRequest(body);
            if (request == null || request.machineName().isBlank() || request.correlationKey().isBlank() || request.signalName().isBlank()) {
                res.status(Status.BAD_REQUEST_400)
                        .header(HeaderNames.CONTENT_TYPE, "application/json")
                        .send("{\"error\": \"Missing required fields: machineName, correlationKey, signalName\"}".getBytes(StandardCharsets.UTF_8));
                return;
            }

            CompletableFuture<SignalDeliveryResult> future = controlPlane.sendSignal(
                    request.machineName(),
                    request.correlationKey(),
                    request.signalName(),
                    request.payload()
            );

            SignalDeliveryResult result = future.join();
            String json = jsonSerializer.serializeSignalResult(result);
            res.header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(json.getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            log.error("Failed to process signal delivery", ex);
            res.status(Status.INTERNAL_SERVER_ERROR_500)
                    .header(HeaderNames.CONTENT_TYPE, "application/json")
                    .send(("{\"error\": \"Failed to deliver signal: " + ex.getMessage() + "\"}").getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleEventStream(ServerRequest req, ServerResponse res) {
        addCorsHeaders(res);
        String machineName = req.query().first("machine").orElse(null);
        String executionId = req.query().first("executionId").orElse(null);

        res.status(Status.OK_200)
                .header(HeaderNames.CONTENT_TYPE, "text/event-stream")
                .header(HeaderNames.CACHE_CONTROL, "no-cache")
                .header(HeaderNames.CONNECTION, "keep-alive");

        OutputStream os = res.outputStream();
        try (EventStream stream = openStream(machineName, executionId)) {
            while (running.get() && !stream.isClosed()) {
                ExecutionEvent event = stream.poll(SSE_POLL_TIMEOUT);
                if (event != null) {
                    String json = jsonSerializer.serializeEvent(event);
                    String sseChunk = "event: " + event.getClass().getSimpleName() + "\n"
                            + "data: " + json + "\n\n";
                    os.write(sseChunk.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
            }
        } catch (IOException | InterruptedException ex) {
            log.debug("SSE stream client disconnected: {}", ex.getMessage());
        }
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

    private void addCorsHeaders(ServerResponse res) {
        if (!config.allowedOrigins().isEmpty()) {
            res.header(HeaderNames.create("Access-Control-Allow-Origin"), String.join(", ", config.allowedOrigins()));
            res.header(HeaderNames.create("Access-Control-Allow-Methods"), "GET, POST, OPTIONS");
            res.header(HeaderNames.create("Access-Control-Allow-Headers"), "Content-Type, Authorization");
        }
    }

    private static String sanitizeBasePath(@Nullable String path) {
        if (path == null || path.isBlank() || path.equals("/")) {
            return "/";
        }
        return path.startsWith("/") ? path : "/" + path;
    }
}
