package com.github.f442y.dispersion.examples;

import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.orchestration.batch.BarrierPolicy;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationBuilder;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationExecutor;
import com.github.f442y.dispersion.orchestration.core.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.core.OrchestrationBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationExecutor;
import com.github.f442y.dispersion.serialization.avaje.AvajeJsonSerializer;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Dispersion Demo App End-to-End HTTP Integration Tests")
class DispersionDemoAppIntegrationTests {

    private DefaultControlPlane controlPlane;
    private ControlPlaneServer server;
    private HttpClient httpClient;
    private String baseUrl;

    private OrchestrationExecutor<DispersionDemoApp.OrderContext, DispersionDemoApp.OrderState, DispersionDemoApp.OrderContext, String> orderExecutor;
    private AtomicStateMachineExecutor<DispersionDemoApp.PipelineContext, DispersionDemoApp.PipelineState, Double, Double> pipelineExecutor;
    private BatchOrchestrationExecutor<DispersionDemoApp.BatchCtx, DispersionDemoApp.ItemCtx, DispersionDemoApp.BatchStage, String> batchExecutor;

    @BeforeEach
    void setUp() {
        controlPlane = new DefaultControlPlane();
        AvajeJsonSerializer jsonSerializer = new AvajeJsonSerializer();

        // 1. Order Saga Workflow
        InMemoryCheckpointStore<DispersionDemoApp.OrderContext, DispersionDemoApp.OrderState> orderStore =
                new InMemoryCheckpointStore<>();

        orderExecutor = OrchestrationBuilder.<DispersionDemoApp.OrderContext, DispersionDemoApp.OrderState, DispersionDemoApp.OrderContext, String>create(
                        "OrderWorkflow", DispersionDemoApp.OrderState.class)
                .checkpointStore(orderStore)
                .eventListener(controlPlane.getEventListener())
                .context(DispersionDemoApp.OrderContext::new)
                .correlationKey(ctx -> ctx.orderId)
                .initialState(DispersionDemoApp.OrderState.VALIDATE_ORDER)
                .endStates(DispersionDemoApp.OrderState.COMPLETED, DispersionDemoApp.OrderState.CANCELLED)
                .input((ctx, in) -> {
                    if (in != null) {
                        ctx.orderId = in.orderId;
                        ctx.customerId = in.customerId;
                        ctx.amountCents = in.amountCents;
                    }
                    return ctx;
                })
                .state(DispersionDemoApp.OrderState.VALIDATE_ORDER)
                    .action(ctx -> { ctx.history.add("VALIDATED"); return ctx; })
                    .transition(DispersionDemoApp.OrderState.RESERVE_INVENTORY)
                .state(DispersionDemoApp.OrderState.RESERVE_INVENTORY)
                    .action(ctx -> { ctx.inventoryReserved = true; ctx.history.add("INVENTORY_RESERVED"); return ctx; })
                    .compensate(ctx -> { ctx.inventoryReserved = false; ctx.history.add("INVENTORY_RELEASED"); return ctx; })
                    .transition(DispersionDemoApp.OrderState.AWAIT_PAYMENT_SIGNAL)
                .state(DispersionDemoApp.OrderState.AWAIT_PAYMENT_SIGNAL)
                    .waitForSignal("PaymentSignal", Object.class, (ctx, payload) -> {
                        DispersionDemoApp.PaymentSignal sig = DispersionDemoApp.PaymentSignal.fromPayload(payload);
                        ctx.paymentMethod = sig.paymentMethod();
                        ctx.history.add("PAYMENT_CONFIRMED_" + sig.paymentMethod());
                        return ctx;
                    })
                    .transition(DispersionDemoApp.OrderState.DISPATCH_SHIPMENT)
                .state(DispersionDemoApp.OrderState.DISPATCH_SHIPMENT)
                    .action(ctx -> { ctx.history.add("SHIPPED"); return ctx; })
                    .transition(DispersionDemoApp.OrderState.COMPLETED)
                .output(ctx -> "ORDER_PROCESSED:" + ctx.orderId)
                .buildExecutor();

        controlPlane.register(orderExecutor.asInspectableMachine());

        // 2. Metrics Pipeline
        pipelineExecutor = AtomicStateMachineBuilder.<DispersionDemoApp.PipelineContext, DispersionDemoApp.PipelineState, Double, Double>create(
                        "MetricsPipeline", DispersionDemoApp.PipelineState.class)
                .context(DispersionDemoApp.PipelineContext::new)
                .initialState(DispersionDemoApp.PipelineState.INGEST)
                .endStates(DispersionDemoApp.PipelineState.FINISHED)
                .eventListener(controlPlane.getEventListener())
                .input((ctx, val) -> {
                    ctx.sensorId = "sensor-test";
                    ctx.metricValue = val != null ? val : 0.0;
                    return ctx;
                })
                .state(DispersionDemoApp.PipelineState.INGEST)
                    .action(ctx -> ctx)
                    .transition(DispersionDemoApp.PipelineState.ENRICH)
                .state(DispersionDemoApp.PipelineState.ENRICH)
                    .action(ctx -> { ctx.metricValue *= 1.25; return ctx; })
                    .transition(DispersionDemoApp.PipelineState.TRANSFORM)
                .state(DispersionDemoApp.PipelineState.TRANSFORM)
                    .action(ctx -> { ctx.metricValue = Math.round(ctx.metricValue * 100.0) / 100.0; return ctx; })
                    .transition(DispersionDemoApp.PipelineState.EXPORT)
                .state(DispersionDemoApp.PipelineState.EXPORT)
                    .action(ctx -> ctx)
                    .transition(DispersionDemoApp.PipelineState.FINISHED)
                .output(ctx -> ctx.metricValue)
                .buildExecutor();

        controlPlane.register(pipelineExecutor.asInspectableMachine());

        // 3. Batch Processor
        batchExecutor = BatchOrchestrationBuilder.<DispersionDemoApp.BatchCtx, DispersionDemoApp.ItemCtx, DispersionDemoApp.BatchStage, String>create(
                        "BatchProcessor", DispersionDemoApp.BatchStage.class)
                .batchContext(DispersionDemoApp.BatchCtx::new)
                .batchKey(ctx -> ctx.batchId)
                .itemKey(ctx -> ctx.itemId)
                .initialState(DispersionDemoApp.BatchStage.IMPORT)
                .endStates(DispersionDemoApp.BatchStage.COMPLETE)
                .output(ctx -> "BATCH_DONE:" + ctx.batchId)
                .itemState(DispersionDemoApp.BatchStage.IMPORT)
                    .action(ctx -> { ctx.status = "IMPORTED"; return ctx; })
                    .transition(DispersionDemoApp.BatchStage.RUN_CHECKS)
                .itemState(DispersionDemoApp.BatchStage.RUN_CHECKS)
                    .barrier(BarrierPolicy.ALL_ITEMS_ARRIVED)
                    .transition(DispersionDemoApp.BatchStage.AWAIT_TRIGGER)
                .itemState(DispersionDemoApp.BatchStage.AWAIT_TRIGGER)
                    .barrier(BarrierPolicy.SIGNAL_TRIGGERED)
                    .transition(DispersionDemoApp.BatchStage.COMPLETE)
                .buildExecutor();

        controlPlane.register(batchExecutor.asInspectableMachine());

        // Start server on ephemeral port (port 0)
        ServerConfig config = ServerConfig.builder()
                .port(0)
                .basePath("/api/v1")
                .allowedOrigins(List.of("*"))
                .build();

        server = ControlPlaneServer.create(config, controlPlane, jsonSerializer);
        server.start();

        baseUrl = "http://localhost:" + server.port() + "/api/v1";
        httpClient = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    @DisplayName("GET /machines returns all 3 registered machine definitions")
    void testListMachinesOverHttp() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/machines"))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body())
                .contains("OrderWorkflow")
                .contains("MetricsPipeline")
                .contains("BatchProcessor");
    }

    @Test
    @DisplayName("GET /machines/{machineName} returns detailed topology for OrderWorkflow")
    void testGetMachineTopologyOverHttp() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/machines/OrderWorkflow"))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body())
                .contains("\"name\":\"OrderWorkflow\"")
                .contains("VALIDATE_ORDER")
                .contains("RESERVE_INVENTORY")
                .contains("AWAIT_PAYMENT_SIGNAL")
                .contains("DISPATCH_SHIPMENT");
    }

    @Test
    @DisplayName("POST /executions/signal delivers signal and resumes suspended order workflow")
    void testDeliverSignalOverHttp() throws Exception {
        // 1. Dispatch an order turn that suspends at AWAIT_PAYMENT_SIGNAL
        DispersionDemoApp.OrderContext order = new DispersionDemoApp.OrderContext();
        order.orderId = "TEST-ORDER-101";
        order.customerId = "CUST-99";
        order.amountCents = 4999;
        orderExecutor.dispatchTurnSync(null, order);

        // 2. Query suspended executions over HTTP
        HttpRequest listReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/executions?status=SUSPENDED"))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> listResp = httpClient.send(listReq, HttpResponse.BodyHandlers.ofString());
        assertThat(listResp.statusCode()).isEqualTo(200);
        assertThat(listResp.body()).contains("TEST-ORDER-101");

        // 3. Post signal over HTTP to deliver PaymentSignal
        String signalJson = """
            {
              "machineName": "OrderWorkflow",
              "correlationKey": "TEST-ORDER-101",
              "signalName": "PaymentSignal",
              "payload": {
                "correlationKey": "TEST-ORDER-101",
                "paymentMethod": "CREDIT_CARD",
                "amountCents": 4999
              }
            }
            """;

        HttpRequest signalReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/executions/signal"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(signalJson))
                .build();

        HttpResponse<String> signalResp = httpClient.send(signalReq, HttpResponse.BodyHandlers.ofString());
        assertThat(signalResp.statusCode()).isEqualTo(200);
        assertThat(signalResp.body()).contains("\"delivered\":true");
    }
}
