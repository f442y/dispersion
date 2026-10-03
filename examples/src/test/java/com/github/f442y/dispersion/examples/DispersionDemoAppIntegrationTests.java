package com.github.f442y.dispersion.examples;

import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.event.control.ExecutionCancelledEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.orchestration.core.OrchestrationExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DisplayName("Dispersion Demo App End-to-End HTTP Integration Tests (Spring Boot 4.1)")
class DispersionDemoAppIntegrationTests {

    @LocalServerPort
    private int port;

    @Autowired
    private DefaultControlPlane controlPlane;

    @Autowired
    private OrchestrationExecutor<DispersionDemoApp.OrderContext, DispersionDemoApp.OrderState, Object, String> orderExecutor;

    @Autowired
    private AtomicStateMachineExecutor<DispersionDemoApp.PipelineContext, DispersionDemoApp.PipelineState, Object, Double> metricsPipelineExecutor;

    private HttpClient httpClient;
    private String baseUrl;

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port + "/api/v1";
        httpClient = HttpClient.newHttpClient();
    }

    @Test
    @DisplayName("GET /node returns operational node diagnostics and registered machine count")
    void testNodeDiagnosticsOverHttp() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/node"))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body())
                .contains("\"status\"")
                .contains("HEALTHY")
                .contains("\"activeMachines\"");
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

    @Test
    @DisplayName("GET /executions/{machine}/{key}/checkpoint and /executions/{id}/timeline return structured telemetry")
    void testExecutionTimelineAndCheckpointOverHttp() throws Exception {
        // 1. Dispatch an order turn that suspends
        DispersionDemoApp.OrderContext order = new DispersionDemoApp.OrderContext();
        order.orderId = "TEST-ORDER-202";
        order.customerId = "CUST-202";
        order.amountCents = 2500;
        orderExecutor.dispatchTurnSync(null, order);

        // 2. Fetch checkpoint over HTTP
        HttpRequest cpReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/executions/OrderWorkflow/TEST-ORDER-202/checkpoint"))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> cpResp = httpClient.send(cpReq, HttpResponse.BodyHandlers.ofString());
        assertThat(cpResp.statusCode()).isEqualTo(200);
        assertThat(cpResp.body())
                .contains("\"correlationKey\":\"TEST-ORDER-202\"")
                .contains("\"currentStateKey\":\"AWAIT_PAYMENT_SIGNAL\"");

        // 3. Find execution summary to get executionId
        var summaryOpt = controlPlane.listExecutions("OrderWorkflow", null, 20).stream()
                .filter(s -> "TEST-ORDER-202".equals(s.correlationKey()))
                .findFirst();
        assertThat(summaryOpt).isPresent();
        String executionId = summaryOpt.get().executionId();

        // 4. Query execution timeline over HTTP
        HttpRequest tlReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/executions/" + executionId + "/timeline?limit=50"))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> tlResp = httpClient.send(tlReq, HttpResponse.BodyHandlers.ofString());
        assertThat(tlResp.statusCode()).isEqualTo(200);
        assertThat(tlResp.body().trim()).startsWith("[").endsWith("]");
        assertThat(tlResp.body())
                .contains("OrderWorkflow")
                .contains("VALIDATE_ORDER");
    }

    @Test
    @DisplayName("POST /machines/{name}/dispatch synchronously triggers and evaluates state machine execution")
    void testDispatchExecutionOverHttp() throws Exception {
        // 1. Dispatch MetricsPipeline with input 42.0 (42.0 * 1.25 = 52.5)
        HttpRequest dispatchMetricsReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/machines/MetricsPipeline/dispatch"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("42.0"))
                .build();

        HttpResponse<String> metricsResp = httpClient.send(dispatchMetricsReq, HttpResponse.BodyHandlers.ofString());
        assertThat(metricsResp.statusCode()).isEqualTo(200);
        assertThat(metricsResp.body())
                .contains("\"status\":\"DISPATCHED\"")
                .contains("\"machine\":\"MetricsPipeline\"")
                .contains("\"result\":52.5");

        // 2. Dispatch OrderWorkflow with empty input
        HttpRequest dispatchOrderReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/machines/OrderWorkflow/dispatch"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> orderResp = httpClient.send(dispatchOrderReq, HttpResponse.BodyHandlers.ofString());
        assertThat(orderResp.statusCode()).isEqualTo(200);
        assertThat(orderResp.body())
                .contains("\"status\":\"DISPATCHED\"")
                .contains("\"machine\":\"OrderWorkflow\"")
                .contains("\"currentStateKey\":\"AWAIT_PAYMENT_SIGNAL\"");
    }

    @Test
    @DisplayName("GET /events/stream streams live Server-Sent Events (SSE) telemetry frames")
    void testLiveSseStreamingWithDynamicTapOverHttp() throws Exception {
        HttpRequest sseReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/events/stream?machine=MetricsPipeline&tier=all"))
                .header("Accept", "text/event-stream")
                .GET()
                .build();

        CompletableFuture<String> firstDataLineFuture = new CompletableFuture<>();

        Thread.ofVirtual().name("test-sse-consumer-", 0).start(() -> {
            try {
                HttpResponse<Stream<String>> resp = httpClient.send(sseReq, HttpResponse.BodyHandlers.ofLines());
                if (resp.statusCode() == 200) {
                    try (Stream<String> lines = resp.body()) {
                        lines.filter(line -> line.startsWith("data:"))
                                .findFirst()
                                .ifPresent(firstDataLineFuture::complete);
                    }
                } else {
                    firstDataLineFuture.completeExceptionally(
                            new IllegalStateException("SSE returned status " + resp.statusCode()));
                }
            } catch (Exception e) {
                firstDataLineFuture.completeExceptionally(e);
            }
        });

        // Repeatedly emit events until the consumer establishes connection and captures an event frame
        Thread.ofVirtual().name("test-sse-producer-", 0).start(() -> {
            while (!firstDataLineFuture.isDone()) {
                try {
                    metricsPipelineExecutor.dispatchSync(88.0);
                    Thread.sleep(100);
                } catch (Exception _) {
                    break;
                }
            }
        });

        String dataLine = firstDataLineFuture.get(5, TimeUnit.SECONDS);
        assertThat(dataLine).isNotBlank();
        assertThat(dataLine).contains("\"machineName\":\"MetricsPipeline\"");
    }

    @Test
    @DisplayName("Cancelled executions are routed to terminal eviction pool and filtered from RUNNING")
    void testCancelledExecutionTerminalEvictionOverHttp() throws Exception {
        UUID cancelId = UUID.randomUUID();
        Instant now = Instant.now();

        // 1. Emit TurnStartedEvent to place execution into RUNNING active pool
        controlPlane.getEventListener().onEvent(new TurnStartedEvent(
                cancelId,
                "OrderWorkflow",
                "CORR-CANCEL-202",
                now
        ));

        // Verify it is visible in RUNNING
        HttpRequest runningReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/executions?status=RUNNING"))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> runningResp = httpClient.send(runningReq, HttpResponse.BodyHandlers.ofString());
        assertThat(runningResp.statusCode()).isEqualTo(200);
        assertThat(runningResp.body()).contains(cancelId.toString());

        // 2. Emit ExecutionCancelledEvent
        controlPlane.getEventListener().onEvent(new ExecutionCancelledEvent(
                cancelId,
                "OrderWorkflow",
                "VALIDATE_ORDER",
                "test-operator",
                "Order cancelled by test suite",
                now.plusMillis(100)
        ));

        // Verify it was evicted from RUNNING active pool
        HttpResponse<String> runningAfterResp = httpClient.send(runningReq, HttpResponse.BodyHandlers.ofString());
        assertThat(runningAfterResp.statusCode()).isEqualTo(200);
        assertThat(runningAfterResp.body()).doesNotContain(cancelId.toString());

        // Verify it is present in CANCELLED terminal pool
        HttpRequest cancelledReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/executions?status=CANCELLED"))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> cancelledResp = httpClient.send(cancelledReq, HttpResponse.BodyHandlers.ofString());
        assertThat(cancelledResp.statusCode()).isEqualTo(200);
        assertThat(cancelledResp.body())
                .contains(cancelId.toString())
                .contains("CANCELLED")
                .contains("Order cancelled by test suite");
    }

    @Test
    @DisplayName("WebDashboardResource enforces path traversal rejection and returns 400 Bad Request")
    void testPathTraversalSecurityOverHttp() throws Exception {
        // 1. Path traversal attempt with encoded slashes rejected by server with 400 Bad Request
        HttpRequest traversalReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/assets/..%2fsecret.txt"))
                .GET()
                .build();
        HttpResponse<String> traversalResp = httpClient.send(traversalReq, HttpResponse.BodyHandlers.ofString());
        assertThat(traversalResp.statusCode()).isEqualTo(400);

        // 2. Missing asset with extension returns 404
        HttpRequest missingReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/assets/nonexistent-file.png"))
                .GET()
                .build();
        HttpResponse<String> missingResp = httpClient.send(missingReq, HttpResponse.BodyHandlers.ofString());
        assertThat(missingResp.statusCode()).isEqualTo(404);
        assertThat(missingResp.body()).contains("Resource not found");
    }

    @Test
    @DisplayName("GET / (root) serves HTML dashboard landing page")
    void testLandingPageServedOverHttp() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/"))
                .header("Accept", "text/html")
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("Dispersion Control");
    }
}
