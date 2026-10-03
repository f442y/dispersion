package com.github.f442y.dispersion.server.core;

import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.serialization.avaje.AvajeJsonSerializer;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class ControlPlaneHttpFacadeTest {

    private DefaultControlPlane controlPlane;
    private JsonSerializer jsonSerializer;
    private ControlPlaneHttpFacade facade;

    @BeforeEach
    public void setUp() {
        controlPlane = new DefaultControlPlane();
        jsonSerializer = new AvajeJsonSerializer();
        ServerConfig config = ServerConfig.builder()
                .port(8080)
                .basePath("/api/v1")
                .environment("test")
                .clusterId("test-cluster")
                .build();
        facade = new ControlPlaneHttpFacade(controlPlane, jsonSerializer, config);
    }

    @AfterEach
    public void tearDown() throws Exception {
        controlPlane.close();
    }

    @Test
    @DisplayName("Should list and register machines via facade")
    public void testMachineOperations() {
        registerMockMachine("OrderFSM");

        HttpResponse listResp = facade.listMachines();
        assertThat(listResp.statusCode()).isEqualTo(200);
        assertThat(listResp.bodyString()).contains("OrderFSM");

        HttpResponse getResp = facade.getMachine("OrderFSM");
        assertThat(getResp.statusCode()).isEqualTo(200);
        assertThat(getResp.bodyString()).contains("OrderFSM");

        HttpResponse notFoundResp = facade.getMachine("UnknownMachine");
        assertThat(notFoundResp.statusCode()).isEqualTo(404);

        // Remote descriptor registration via JSON
        String descriptorJson = """
                {
                  "name": "RemoteServiceFSM",
                  "type": "ORCHESTRATION",
                  "initialState": "INIT",
                  "endStates": ["DONE"],
                  "allStates": ["INIT", "DONE"],
                  "mermaidGraph": "graph TD;"
                }
                """;
        HttpResponse regResp = facade.registerMachine(descriptorJson);
        assertThat(regResp.statusCode()).isEqualTo(200);
        assertThat(regResp.bodyString()).contains("RemoteServiceFSM");
    }

    @Test
    @DisplayName("Should list executions and filter by status")
    public void testExecutionQueries() {
        UUID exec1 = UUID.randomUUID();
        Instant now = Instant.now();
        controlPlane.onEvent(new TurnStartedEvent(exec1, "OrderFSM", "CORR-1", now));

        HttpResponse listResp = facade.listExecutions(null, null, 10);
        assertThat(listResp.statusCode()).isEqualTo(200);
        assertThat(listResp.bodyString()).contains(exec1.toString());

        HttpResponse getResp = facade.getExecution(exec1.toString());
        assertThat(getResp.statusCode()).isEqualTo(200);
        assertThat(getResp.bodyString()).contains("CORR-1");

        HttpResponse invalidStatusResp = facade.listExecutions(null, "INVALID_STATUS", 10);
        assertThat(invalidStatusResp.statusCode()).isEqualTo(400);

        HttpResponse timelineResp = facade.getExecutionTimeline(exec1.toString(), 10);
        assertThat(timelineResp.statusCode()).isEqualTo(200);
    }

    @Test
    @DisplayName("Should inspect checkpoint and send signals")
    public void testCheckpointAndSignal() {
        registerMockMachine("SagaMachine");

        HttpResponse cpResp = facade.inspectCheckpoint("SagaMachine", "CORR-99");
        assertThat(cpResp.statusCode()).isEqualTo(200);
        assertThat(cpResp.bodyString()).contains("MOCK_STATE");

        HttpResponse signalResp = facade.sendSignal("""
                {
                  "machineName": "SagaMachine",
                  "correlationKey": "CORR-99",
                  "signalName": "ApproveSignal",
                  "payload": "Approved"
                }
                """);
        assertThat(signalResp.statusCode()).isEqualTo(200);
        assertThat(signalResp.bodyString()).contains("\"delivered\":true");
    }

    @Test
    @DisplayName("Should ingest telemetry events batch")
    public void testEventBatchIngestion() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        String json = jsonSerializer.serializeEvents(List.of(
                new TurnStartedEvent(id, "TelemetryMachine", "C1", now),
                new TurnCompletedEvent(id, "TelemetryMachine", "C1", "DONE", Duration.ofMillis(5), now.plusMillis(5))
        ));

        HttpResponse resp = facade.ingestEvents(json);
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.bodyString()).contains("\"count\":2");

        assertThat(controlPlane.getExecution(id.toString())).isPresent();
    }

    @Test
    @DisplayName("Should return node diagnostics and handle OPTIONS")
    public void testNodeAndOptions() {
        HttpResponse nodeResp = facade.getNode();
        assertThat(nodeResp.statusCode()).isEqualTo(200);
        assertThat(nodeResp.bodyString()).contains("test-cluster").contains("status");

        HttpResponse optResp = facade.handleOptions();
        assertThat(optResp.statusCode()).isEqualTo(204);
    }

    @Test
    @DisplayName("Should prevent path traversal attacks in static asset resolver")
    public void testPathTraversalDefenses() {
        assertThat(StaticAssetResolver.isPathTraversal("../secret.txt")).isTrue();
        assertThat(StaticAssetResolver.isPathTraversal("..\\secret.txt")).isTrue();
        assertThat(StaticAssetResolver.isPathTraversal("assets/../../etc/passwd")).isTrue();
        assertThat(StaticAssetResolver.isPathTraversal("assets/test\0.js")).isTrue();
        assertThat(StaticAssetResolver.isPathTraversal("/absolute/path")).isTrue();
        assertThat(StaticAssetResolver.isPathTraversal("assets/app.js")).isFalse();

        HttpResponse badReq1 = facade.resolveAsset("../secret.txt");
        assertThat(badReq1.statusCode()).isEqualTo(400);

        HttpResponse badReq2 = facade.resolveAsset("sub/..\\evil");
        assertThat(badReq2.statusCode()).isEqualTo(400);
    }

    @Test
    @DisplayName("Should stream events and heartbeats over SSE sink")
    public void testSseStreaming() throws Exception {
        TestSseSink sink = new TestSseSink();
        Thread thread = facade.startSseSession(sink, null, null, "lifecycle");

        UUID id = UUID.randomUUID();
        controlPlane.onEvent(new TurnStartedEvent(id, "StreamMachine", "C1", Instant.now()));

        // Allow virtual thread to poll event
        Thread.sleep(300);
        sink.close();
        thread.join(2000);

        assertThat(sink.events).isNotEmpty();
        assertThat(sink.events.getFirst()).contains("StreamMachine");
    }

    private void registerMockMachine(String name) {
        controlPlane.register(new InspectableMachine() {
            @Override
            public @NonNull MachineDescriptor descriptor() {
                return new MachineDescriptor(name, MachineType.ORCHESTRATION, "INIT", Set.of("DONE"), List.of("INIT", "DONE"), "graph TD;");
            }

            @Override
            public @NonNull CompletableFuture<SignalDeliveryResult> sendSignal(@NonNull String correlationKey, @NonNull String signalName, @Nullable Object payload) {
                return CompletableFuture.completedFuture(new SignalDeliveryResult(true, "Signal delivered", name, correlationKey, signalName, true, false, "DONE", null));
            }

            @Override
            public @NonNull Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.of(Map.of("state", "MOCK_STATE", "key", correlationKey));
            }
        });
    }

    private static final class TestSseSink implements SseSink {
        final List<String> events = new CopyOnWriteArrayList<>();
        final List<String> comments = new CopyOnWriteArrayList<>();
        final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void sendEvent(@NonNull String eventName, @NonNull String data) {
            events.add(data);
        }

        @Override
        public void sendComment(@NonNull String comment) {
            comments.add(comment);
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
