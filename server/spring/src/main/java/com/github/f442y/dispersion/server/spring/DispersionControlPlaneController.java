package com.github.f442y.dispersion.server.spring;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import com.github.f442y.dispersion.server.core.ControlPlaneHttpFacade;
import com.github.f442y.dispersion.server.core.HttpResponse;
import com.github.f442y.dispersion.server.core.SseSink;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Universal, native Spring MVC REST controller for the Dispersion Control Plane.
 * <p>
 * Exposes inspection, topology, execution history, external signal dispatching,
 * telemetry ingestion, node health diagnostics, and Server-Sent Events (SSE) streaming
 * via Spring's native {@link SseEmitter} on Java 25 virtual threads.
 */
@RestController
@RequestMapping("${dispersion.control-plane.base-path:/api/v1}")
public class DispersionControlPlaneController {

    private final ControlPlaneHttpFacade facade;

    public DispersionControlPlaneController(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer
    ) {
        this(controlPlane, jsonSerializer, null);
    }

    @Autowired
    public DispersionControlPlaneController(
            @NonNull ControlPlane controlPlane,
            @NonNull JsonSerializer jsonSerializer,
            @Autowired(required = false) @Nullable ServerConfig serverConfig
    ) {
        this(new ControlPlaneHttpFacade(
                controlPlane,
                jsonSerializer,
                serverConfig != null ? serverConfig : ServerConfig.defaultConfig()
        ));
    }

    public DispersionControlPlaneController(@NonNull ControlPlaneHttpFacade facade) {
        this.facade = Objects.requireNonNull(facade, "facade must not be null");
    }

    /**
     * Lists registered state machine topology descriptors.
     */
    @GetMapping(value = "/machines", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> listMachines() {
        return toResponseEntity(facade.listMachines());
    }

    /**
     * Registers a new state machine descriptor with the control plane.
     */
    @PostMapping(value = "/machines", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> registerMachine(@RequestBody @NonNull String body) {
        return toResponseEntity(facade.registerMachine(body));
    }

    /**
     * Gets a specific state machine descriptor by name.
     */
    @GetMapping(value = "/machines/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getMachine(@PathVariable("name") @NonNull String name) {
        return toResponseEntity(facade.getMachine(name));
    }

    /**
     * Lists recent execution summaries across all state machines.
     */
    @GetMapping(value = "/executions", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> listExecutions(
            @RequestParam(value = "machine", required = false) @Nullable String machineName,
            @RequestParam(value = "status", required = false) @Nullable String status,
            @RequestParam(value = "limit", defaultValue = "50") int limit
    ) {
        return toResponseEntity(facade.listExecutions(machineName, status, limit));
    }

    /**
     * Gets summary details for a specific execution ID.
     */
    @GetMapping(value = "/executions/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getExecution(@PathVariable("id") @NonNull String executionId) {
        return toResponseEntity(facade.getExecution(executionId));
    }

    /**
     * Retrieves the chronological event timeline for a specific execution ID.
     */
    @GetMapping(value = "/executions/{id}/timeline", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getExecutionTimeline(
            @PathVariable("id") @NonNull String executionId,
            @RequestParam(value = "limit", defaultValue = "100") int limit
    ) {
        return toResponseEntity(facade.getExecutionTimeline(executionId, limit));
    }

    /**
     * Inspects the latest checkpoint state snapshot for a specific execution.
     */
    @GetMapping(value = "/executions/{machine}/{key}/checkpoint", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> inspectCheckpoint(
            @PathVariable("machine") @NonNull String machineName,
            @PathVariable("key") @NonNull String correlationKey
    ) {
        return toResponseEntity(facade.inspectCheckpoint(machineName, correlationKey));
    }

    /**
     * Asynchronously dispatches a new execution of a registered state machine.
     */
    @PostMapping(value = "/machines/{name}/dispatch", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> dispatchExecution(
            @PathVariable("name") @NonNull String machineName,
            @RequestBody(required = false) @Nullable String input
    ) {
        return toResponseEntity(facade.dispatchExecution(machineName, input));
    }

    /**
     * Delivers an external signal to a suspended orchestration workflow.
     */
    @PostMapping(value = {"/executions/signal", "/signals"}, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> sendSignal(@RequestBody @NonNull String body) {
        return toResponseEntity(facade.sendSignal(body));
    }

    /**
     * Ingests a batch of polymorphic execution telemetry events from remote worker microservices.
     */
    @PostMapping(value = "/telemetry/events", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> ingestEvents(@RequestBody @NonNull String body) {
        return toResponseEntity(facade.ingestEvents(body));
    }

    /**
     * Returns node operational health, environment, clusterId, CPU, memory, and active engine counts.
     */
    @GetMapping(value = "/node", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getNode() {
        return toResponseEntity(facade.getNode("Spring MVC (Java " + System.getProperty("java.version", "25") + ")"));
    }

    /**
     * Streams live telemetry events via Server-Sent Events (SSE).
     */
    @GetMapping(value = "/events/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamEvents(
            @RequestParam(value = "machine", required = false) @Nullable String machineName,
            @RequestParam(value = "executionId", required = false) @Nullable String executionId,
            @RequestParam(value = "tier", defaultValue = "lifecycle") @Nullable String tier
    ) {
        SseEmitter emitter = new SseEmitter(0L);
        AtomicBoolean closed = new AtomicBoolean(false);
        emitter.onCompletion(() -> closed.set(true));
        emitter.onTimeout(() -> closed.set(true));
        emitter.onError(_ -> closed.set(true));

        SpringSseSink sink = new SpringSseSink(emitter, closed);
        facade.startSseSession(sink, machineName, executionId, tier, "dispersion-spring-sse-");
        return emitter;
    }

    /**
     * Handles preflight CORS OPTIONS requests for the API controller.
     */
    @RequestMapping(value = "/**", method = RequestMethod.OPTIONS)
    public ResponseEntity<?> handleOptions() {
        return toResponseEntity(facade.handleOptions());
    }

    public static ResponseEntity<?> toResponseEntity(HttpResponse hr) {
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(hr.statusCode());
        if (hr.contentType() != null) {
            builder.contentType(MediaType.parseMediaType(hr.contentType()));
        }
        hr.headers().forEach(builder::header);
        if (hr.bodyString() != null) {
            return builder.body(hr.bodyString());
        } else if (hr.bodyBytes() != null) {
            return builder.body(hr.bodyBytes());
        }
        return builder.build();
    }

    private record SpringSseSink(SseEmitter emitter, AtomicBoolean closed) implements SseSink {

        @Override
        public void sendEvent(String eventName, String data) throws Exception {
            if (closed.get()) {
                return;
            }
            emitter.send(SseEmitter.event()
                    .name(eventName)
                    .data(data, MediaType.APPLICATION_JSON));
        }

        @Override
        public void sendComment(String comment) throws Exception {
            if (closed.get()) {
                return;
            }
            emitter.send(SseEmitter.event().comment(comment));
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                try {
                    emitter.complete();
                } catch (Exception _) {
                }
            }
        }
    }
}
