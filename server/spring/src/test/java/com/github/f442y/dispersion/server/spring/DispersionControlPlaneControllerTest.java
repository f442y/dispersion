package com.github.f442y.dispersion.server.spring;

import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.serialization.avaje.AvajeJsonSerializer;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class DispersionControlPlaneControllerTest {

    private DefaultControlPlane controlPlane;
    private JsonSerializer jsonSerializer;
    private DispersionControlPlaneController controller;

    @BeforeEach
    void setUp() {
        controlPlane = new DefaultControlPlane();
        jsonSerializer = new AvajeJsonSerializer();
        controller = new DispersionControlPlaneController(controlPlane, jsonSerializer);
    }

    @Test
    @DisplayName("GET /machines returns registered machines")
    void listMachines() {
        controlPlane.register(new InspectableMachine() {
            @Override
            public @NonNull MachineDescriptor descriptor() {
                return new MachineDescriptor("SpringWorkflow", MachineType.ORCHESTRATION, "INIT", Set.of("PAID"), List.of("INIT", "PAID"), "stateDiagram-v2");
            }

            @Override
            public @NonNull CompletableFuture<SignalDeliveryResult> sendSignal(@NonNull String correlationKey, @NonNull String signalName, @Nullable Object payload) {
                return CompletableFuture.completedFuture(new SignalDeliveryResult(true, "Delivered", "SpringWorkflow", correlationKey, signalName, false, false, "RUNNING", null));
            }

            @Override
            public @NonNull Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.empty();
            }
        });

        ResponseEntity<?> response = controller.listMachines();
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().toString()).contains("SpringWorkflow");
    }

    @Test
    @DisplayName("GET /executions lists summaries and filters by status")
    void listExecutions() {
        UUID executionId = UUID.randomUUID();
        TurnStartedEvent event = new TurnStartedEvent(executionId, "SpringWorkflow", "corr-spring", Instant.now());
        controlPlane.getEventListener().onEvent(event);

        ResponseEntity<?> response = controller.listExecutions("SpringWorkflow", "RUNNING", 10);
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody().toString()).contains("SpringWorkflow");
        assertThat(response.getBody().toString()).contains("corr-spring");

        ResponseEntity<?> completedResponse = controller.listExecutions("SpringWorkflow", "COMPLETED", 10);
        assertThat(completedResponse.getStatusCode().value()).isEqualTo(200);
        assertThat(completedResponse.getBody().toString()).isEqualTo("[]");
    }

    @Test
    @DisplayName("POST /executions/signal delivers signal via JSON payload")
    void sendSignal() {
        controlPlane.register(new InspectableMachine() {
            @Override
            public @NonNull MachineDescriptor descriptor() {
                return new MachineDescriptor("OrderMachine", MachineType.ORCHESTRATION, "INIT", Set.of(), List.of(), "stateDiagram-v2");
            }

            @Override
            public @NonNull CompletableFuture<SignalDeliveryResult> sendSignal(@NonNull String correlationKey, @NonNull String signalName, @Nullable Object payload) {
                return CompletableFuture.completedFuture(new SignalDeliveryResult(true, "Delivered", "OrderMachine", correlationKey, signalName, false, false, "RUNNING", null));
            }

            @Override
            public @NonNull Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.empty();
            }
        });

        String payload = """
                {
                    "machineName": "OrderMachine",
                    "correlationKey": "ORD-1",
                    "signalName": "PaymentReceived",
                    "payload": "{\\"amount\\": 100}"
                }
                """;

        ResponseEntity<?> response = controller.sendSignal(payload);
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody().toString()).contains("\"delivered\":true");
        assertThat(response.getBody().toString()).contains("PaymentReceived");
    }

    @Test
    @DisplayName("POST /executions/signal returns 400 for malformed payload")
    void sendSignalMalformed() {
        ResponseEntity<?> response = controller.sendSignal("not-valid-json");
        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().toString()).contains("Malformed signal request payload");
    }

    @Test
    @DisplayName("GET /node returns operational health diagnostics")
    void getNode() {
        ResponseEntity<?> response = controller.getNode();
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody().toString()).contains("Spring MVC");
        assertThat(response.getBody().toString()).contains("HEALTHY");
    }

    @Test
    @DisplayName("GET /events/stream returns valid SseEmitter")
    void streamEvents() {
        SseEmitter emitter = controller.streamEvents(null, null, "lifecycle");
        assertThat(emitter).isNotNull();
    }

    @Test
    @DisplayName("OPTIONS returns 204 No Content for CORS preflight")
    void handleOptions() {
        ResponseEntity<?> response = controller.handleOptions();
        assertThat(response.getStatusCode().value()).isEqualTo(204);
    }
}
