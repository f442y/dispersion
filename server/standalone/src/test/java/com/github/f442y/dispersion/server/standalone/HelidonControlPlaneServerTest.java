package com.github.f442y.dispersion.server.standalone;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.event.turn.TurnSuspendedEvent;
import com.github.f442y.dispersion.serialization.avaje.AvajeJsonSerializer;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ControlPlaneServer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

final class HelidonControlPlaneServerTest {

    private DefaultControlPlane controlPlane;
    private JsonSerializer serializer;
    private ControlPlaneServer server;
    private HttpClient client;
    private String baseUri;

    @BeforeEach
    void setUp() {
        controlPlane = new DefaultControlPlane();
        serializer = new AvajeJsonSerializer();

        ServerConfig config = ServerConfig.builder()
                .host("127.0.0.1")
                .port(0)
                .basePath("/api/v1")
                .allowedOrigins(List.of("*"))
                .build();

        server = ControlPlaneServer.create(config, controlPlane, serializer);
        server.start();

        baseUri = "http://127.0.0.1:" + server.port() + "/api/v1";
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    @AfterEach
    void tearDown() {
        if (server != null && server.isRunning()) {
            server.stop();
        }
        if (controlPlane != null) {
            controlPlane.close();
        }
    }

    @Test
    @DisplayName("ServiceLoader discovers HelidonControlPlaneServerFactory and boots Helidon SE WebServer")
    void shouldDiscoverViaServiceLoaderAndRun() {
        assertThat(server).isInstanceOf(HelidonControlPlaneServer.class);
        assertThat(server.isRunning()).isTrue();
        assertThat(server.port()).isGreaterThan(0);
    }

    @Test
    @DisplayName("GET /api/v1/machines returns registered machine descriptors")
    void shouldListMachines() throws Exception {
        MachineDescriptor descriptor = new MachineDescriptor(
                "OrderWorkflow",
                MachineType.ORCHESTRATION,
                "CREATED",
                Set.of("COMPLETED", "CANCELLED"),
                List.of("CREATED", "PROCESSING", "COMPLETED", "CANCELLED"),
                "graph TD;\nCREATED-->PROCESSING;"
        );
        controlPlane.register(descriptor, null);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/machines"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("OrderWorkflow");
        assertThat(response.body()).contains("ORCHESTRATION");
    }

    @Test
    @DisplayName("GET /api/v1/machines/{name} returns machine descriptor or 404")
    void shouldGetMachineByName() throws Exception {
        MachineDescriptor descriptor = new MachineDescriptor(
                "OrderWorkflow",
                MachineType.ORCHESTRATION,
                "CREATED",
                Set.of("COMPLETED"),
                List.of("CREATED", "COMPLETED"),
                ""
        );
        controlPlane.register(descriptor, null);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/machines/OrderWorkflow"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("OrderWorkflow");

        HttpRequest notFoundRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/machines/NonExistent"))
                .GET()
                .build();

        HttpResponse<String> notFoundResponse = client.send(notFoundRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(notFoundResponse.statusCode()).isEqualTo(404);
    }

    @Test
    @DisplayName("GET /api/v1/executions and /executions/{id} return execution history")
    void shouldQueryExecutions() throws Exception {
        UUID execId = UUID.randomUUID();
        Instant now = Instant.now();

        controlPlane.onEvent(new TurnStartedEvent(execId, "OrderWorkflow", "corr-100", now));
        controlPlane.onEvent(new TurnSuspendedEvent(execId, "OrderWorkflow", "PAYMENT_PENDING", "PaymentConfirmed", "corr-100", Duration.ofMillis(50), now));

        HttpRequest listReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/executions?machine=OrderWorkflow&status=SUSPENDED"))
                .GET()
                .build();

        HttpResponse<String> listResp = client.send(listReq, HttpResponse.BodyHandlers.ofString());
        assertThat(listResp.statusCode()).isEqualTo(200);
        assertThat(listResp.body()).contains(execId.toString());
        assertThat(listResp.body()).contains("PAYMENT_PENDING");

        HttpRequest singleReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/executions/" + execId))
                .GET()
                .build();

        HttpResponse<String> singleResp = client.send(singleReq, HttpResponse.BodyHandlers.ofString());
        assertThat(singleResp.statusCode()).isEqualTo(200);
        assertThat(singleResp.body()).contains("PaymentConfirmed");
    }

    @Test
    @DisplayName("POST /api/v1/executions/signal delivers signal and returns result")
    void shouldDeliverSignal() throws Exception {
        MachineDescriptor descriptor = new MachineDescriptor(
                "OrderWorkflow",
                MachineType.ORCHESTRATION,
                "START",
                Set.of("COMPLETED"),
                List.of("START", "PAYMENT", "COMPLETED"),
                ""
        );

        controlPlane.register(descriptor, (corr, signal, payload) ->
                CompletableFuture.completedFuture(new SignalDeliveryResult(
                        true,
                        "Delivered",
                        "OrderWorkflow",
                        corr,
                        signal,
                        false,
                        false,
                        "FULFILLED",
                        null
                ))
        );

        String payloadJson = "{\"machineName\":\"OrderWorkflow\",\"correlationKey\":\"corr-1\",\"signalName\":\"PaymentSignal\",\"payload\":\"credit\"}";
        HttpRequest postReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/executions/signal"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();

        HttpResponse<String> response = client.send(postReq, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"delivered\":true");
        assertThat(response.body()).contains("\"resultingState\":\"FULFILLED\"");
    }

    @Test
    @DisplayName("GET /api/v1/events/stream streams events over Server-Sent Events (SSE)")
    void shouldStreamEventsViaSse() throws Exception {
        CountDownLatch eventReceivedLatch = new CountDownLatch(1);
        AtomicReference<String> receivedChunk = new AtomicReference<>();

        HttpRequest sseRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "/events/stream?machine=OrderWorkflow"))
                .GET()
                .build();

        CompletableFuture<Void> streamFuture = client.sendAsync(sseRequest, HttpResponse.BodyHandlers.ofLines())
                .thenAccept(resp -> {
                    resp.body().forEach(line -> {
                        if (line.startsWith("data: ")) {
                            receivedChunk.set(line);
                            eventReceivedLatch.countDown();
                        }
                    });
                });

        Thread.sleep(200);

        UUID machineId = UUID.randomUUID();
        StateEnteredEvent event = new StateEnteredEvent(machineId, "OrderWorkflow", "PAYMENT_STEP", Instant.now());
        controlPlane.onEvent(event);

        boolean reached = eventReceivedLatch.await(3, TimeUnit.SECONDS);
        assertThat(reached).isTrue();
        assertThat(receivedChunk.get()).contains("PAYMENT_STEP");

        streamFuture.cancel(true);
    }
}
