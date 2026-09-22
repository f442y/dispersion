package com.github.f442y.dispersion.examples;

import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.batch.BarrierPolicy;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationBuilder;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationExecutor;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.core.OrchestrationBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationExecutor;
import com.github.f442y.dispersion.serialization.avaje.AvajeJsonSerializer;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import com.github.f442y.dispersion.server.api.ControlPlaneServer;
import com.github.f442y.dispersion.server.api.ServerConfig;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Interactive developer demo application running the Dispersion Control Plane
 * and Helidon SE Níma HTTP/SSE server locally on Java 25 virtual threads.
 * <p>
 * Can be run directly from IntelliJ IDEA (right-click -> 'Run DispersionDemoApp.main()')
 * or via Maven:
 * {@code .\mvnw compile exec:java -pl examples -Dexec.mainClass="com.github.f442y.dispersion.examples.DispersionDemoApp"}
 */
public final class DispersionDemoApp {

    private static final Logger log = LoggerFactory.getLogger(DispersionDemoApp.class);
    private static final int SERVER_PORT = 8080;
    private static final String BASE_PATH = "/api/v1";

    private DispersionDemoApp() {}

    // =========================================================================
    // 1. Order Checkout Workflow (Orchestration Saga + Signal Suspension)
    // =========================================================================

    public enum OrderState implements StateKey {
        VALIDATE_ORDER,
        RESERVE_INVENTORY,
        AWAIT_PAYMENT_SIGNAL,
        DISPATCH_SHIPMENT,
        COMPLETED,
        CANCELLED
    }

    public record PaymentSignal(
            @NonNull String correlationKey,
            @NonNull String paymentMethod,
            int amountCents
    ) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "PaymentSignal";
        }

        public static PaymentSignal fromPayload(Object payload) {
            if (payload instanceof PaymentSignal p) {
                return p;
            }
            if (payload instanceof Map<?, ?> map) {
                Object corrVal = map.get("correlationKey");
                String corr = (corrVal != null) ? corrVal.toString() : "";
                Object methodVal = map.get("paymentMethod");
                String method = (methodVal != null) ? methodVal.toString() : "UNKNOWN";
                int cents = 0;
                Object centsObj = map.get("amountCents");
                if (centsObj instanceof Number num) {
                    cents = num.intValue();
                }
                return new PaymentSignal(corr, method, cents);
            }
            return new PaymentSignal("", "UNKNOWN", 0);
        }
    }

    public static class OrderContext implements StateMachineContext {
        public String orderId;
        public String customerId;
        public int amountCents;
        public String paymentMethod;
        public boolean inventoryReserved;
        public List<String> history = new ArrayList<>();
    }

    // =========================================================================
    // 2. High-Throughput Metrics Pipeline (Atomic Finite State Machine)
    // =========================================================================

    public enum PipelineState implements StateKey {
        INGEST,
        ENRICH,
        TRANSFORM,
        EXPORT,
        FINISHED
    }

    public static class PipelineContext implements StateMachineContext {
        public String sensorId;
        public double metricValue;
    }

    // =========================================================================
    // 3. Batch Ingestion Pipeline (Turn-Based Item Batching with Quorum Barrier)
    // =========================================================================

    public enum BatchStage implements StateKey {
        IMPORT,
        RUN_CHECKS,
        AWAIT_TRIGGER,
        COMPLETE
    }

    public static class BatchCtx implements StateMachineContext {
        public String batchId = "BATCH-" + System.currentTimeMillis();
    }

    public static class ItemCtx implements StateMachineContext {
        public String itemId;
        public String status = "PENDING";

        public ItemCtx() {}

        public ItemCtx(String itemId) {
            this.itemId = itemId;
        }
    }

    // =========================================================================
    // Main Entry Point
    // =========================================================================

    public static void main(String[] args) throws Exception {
        log.info("Starting Dispersion Control Plane Demo Application...");

        // 1. Initialize Control Plane Engine & Serializer
        DefaultControlPlane controlPlane = new DefaultControlPlane();
        JsonSerializer jsonSerializer = new AvajeJsonSerializer();

        // 2. Build and register Order Orchestration Saga Engine
        InMemoryCheckpointStore<OrderContext, OrderState> orderStore = new InMemoryCheckpointStore<>();

        OrchestrationExecutor<OrderContext, OrderState, OrderContext, String> orderExecutor =
                OrchestrationBuilder.<OrderContext, OrderState, OrderContext, String>create("OrderWorkflow", OrderState.class)
                        .checkpointStore(orderStore)
                        .eventListener(controlPlane.getEventListener())
                        .context(OrderContext::new)
                        .correlationKey(ctx -> ctx.orderId)
                        .initialState(OrderState.VALIDATE_ORDER)
                        .endStates(OrderState.COMPLETED, OrderState.CANCELLED)
                        .input((ctx, in) -> {
                            if (in != null) {
                                ctx.orderId = in.orderId;
                                ctx.customerId = in.customerId;
                                ctx.amountCents = in.amountCents;
                            }
                            return ctx;
                        })
                        .state(OrderState.VALIDATE_ORDER)
                            .action(ctx -> {
                                ctx.history.add("VALIDATED");
                                return ctx;
                            })
                            .transition(OrderState.RESERVE_INVENTORY)
                        .state(OrderState.RESERVE_INVENTORY)
                            .action(ctx -> {
                                ctx.inventoryReserved = true;
                                ctx.history.add("INVENTORY_RESERVED");
                                return ctx;
                            })
                            .compensate(ctx -> {
                                ctx.inventoryReserved = false;
                                ctx.history.add("INVENTORY_RELEASED");
                                return ctx;
                            })
                            .transition(OrderState.AWAIT_PAYMENT_SIGNAL)
                        .state(OrderState.AWAIT_PAYMENT_SIGNAL)
                            .waitForSignal("PaymentSignal", Object.class, (ctx, payload) -> {
                                PaymentSignal sig = PaymentSignal.fromPayload(payload);
                                ctx.paymentMethod = sig.paymentMethod();
                                ctx.history.add("PAYMENT_CONFIRMED_" + sig.paymentMethod());
                                return ctx;
                            })
                            .transition(OrderState.DISPATCH_SHIPMENT)
                        .state(OrderState.DISPATCH_SHIPMENT)
                            .action(ctx -> {
                                ctx.history.add("SHIPPED");
                                return ctx;
                            })
                            .transition(OrderState.COMPLETED)
                        .output(ctx -> "ORDER_PROCESSED:" + ctx.orderId)
                        .buildExecutor();

        controlPlane.register(orderExecutor.asInspectableMachine());

        // 3. Build and register High-Throughput Atomic Metrics Pipeline
        AtomicStateMachineExecutor<PipelineContext, PipelineState, Double, Double> pipelineExecutor =
                AtomicStateMachineBuilder.<PipelineContext, PipelineState, Double, Double>create("MetricsPipeline", PipelineState.class)
                        .context(PipelineContext::new)
                        .initialState(PipelineState.INGEST)
                        .endStates(PipelineState.FINISHED)
                        .eventListener(controlPlane.getEventListener())
                        .input((ctx, val) -> {
                            ctx.sensorId = "sensor-" + (System.currentTimeMillis() % 10);
                            ctx.metricValue = val != null ? val : 0.0;
                            return ctx;
                        })
                        .state(PipelineState.INGEST)
                            .action(ctx -> ctx)
                            .transition(PipelineState.ENRICH)
                        .state(PipelineState.ENRICH)
                            .action(ctx -> { ctx.metricValue *= 1.25; return ctx; })
                            .transition(PipelineState.TRANSFORM)
                        .state(PipelineState.TRANSFORM)
                            .action(ctx -> { ctx.metricValue = Math.round(ctx.metricValue * 100.0) / 100.0; return ctx; })
                            .transition(PipelineState.EXPORT)
                        .state(PipelineState.EXPORT)
                            .action(ctx -> ctx)
                            .transition(PipelineState.FINISHED)
                        .output(ctx -> ctx.metricValue)
                        .buildExecutor();

        controlPlane.register(pipelineExecutor.asInspectableMachine());

        // 4. Build and register Turn-Based Batch Processor
        BatchOrchestrationExecutor<BatchCtx, ItemCtx, BatchStage, String> batchExecutor =
                BatchOrchestrationBuilder.<BatchCtx, ItemCtx, BatchStage, String>create("BatchProcessor", BatchStage.class)
                        .batchContext(BatchCtx::new)
                        .batchKey(ctx -> ctx.batchId)
                        .itemKey(ctx -> ctx.itemId)
                        .initialState(BatchStage.IMPORT)
                        .endStates(BatchStage.COMPLETE)
                        .output(ctx -> "BATCH_DONE:" + ctx.batchId)
                        .itemState(BatchStage.IMPORT)
                            .action(ctx -> { ctx.status = "IMPORTED"; return ctx; })
                            .transition(BatchStage.RUN_CHECKS)
                        .itemState(BatchStage.RUN_CHECKS)
                            .barrier(BarrierPolicy.ALL_ITEMS_ARRIVED)
                            .transition(BatchStage.AWAIT_TRIGGER)
                        .itemState(BatchStage.AWAIT_TRIGGER)
                            .barrier(BarrierPolicy.SIGNAL_TRIGGERED)
                            .transition(BatchStage.COMPLETE)
                        .buildExecutor();

        controlPlane.register(batchExecutor.asInspectableMachine());

        // 5. Pre-seed a suspended order workflow for immediate manual signal injection testing
        OrderContext preSeededOrder = new OrderContext();
        preSeededOrder.orderId = "ORDER-DEMO-99";
        preSeededOrder.customerId = "CUST-42";
        preSeededOrder.amountCents = 9995;
        orderExecutor.dispatchTurnSync(null, preSeededOrder);

        // Pre-seed batch items
        batchExecutor.dispatchBatchSync(List.of(new ItemCtx("ITEM-A"), new ItemCtx("ITEM-B")));

        // 6. Start Helidon SE Níma HTTP Server
        ServerConfig config = ServerConfig.builder()
                .port(SERVER_PORT)
                .basePath(BASE_PATH)
                .allowedOrigins(List.of("*"))
                .build();

        ControlPlaneServer server = ControlPlaneServer.create(config, controlPlane, jsonSerializer);
        server.start();

        printBanner(server.port());

        // 7. Start background virtual thread activity generator
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger orderCounter = new AtomicInteger(100);
        Random random = new Random();

        Thread.ofVirtual().name("dispersion-traffic-sim-", 0).start(() -> {
            while (running.get()) {
                try {
                    // Periodic pipeline bursts (continuous live telemetry for SSE)
                    pipelineExecutor.dispatchSync(random.nextDouble() * 100.0);

                    // Occasional order workflows (every ~3 seconds)
                    if (random.nextInt(6) == 0) {
                        int num = orderCounter.incrementAndGet();
                        OrderContext order = new OrderContext();
                        order.orderId = "ORD-" + num;
                        order.customerId = "CUST-" + (num % 5);
                        order.amountCents = (num * 100) % 5000 + 500;
                        orderExecutor.dispatchTurnSync(null, order);
                    }

                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.warn("Simulator burst error: {}", e.getMessage());
                }
            }
        });

        // 8. Add JVM shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received. Stopping demo gracefully...");
            running.set(false);
            server.stop();
            log.info("Dispersion Control Plane demo stopped.");
        }, "dispersion-shutdown-hook"));

        // Wait indefinitely
        Thread.currentThread().join();
    }

    private static void printBanner(int port) {
        String base = "http://localhost:" + port + BASE_PATH;
        System.out.println("""
            ========================================================================================
            🚀 DISPERSION CONTROL PLANE & HELIDON SE DEMO IS RUNNING
            ========================================================================================
            Base HTTP URL: %s

            ✨ Pre-registered Machines:
               • OrderWorkflow    (Orchestration Saga: Suspended order 'ORDER-DEMO-99' awaiting signal)
               • MetricsPipeline  (Atomic FSM: Emitting continuous background events)
               • BatchProcessor   (Turn-based item batch with quorum barrier)

            📡 Live Exploratory Endpoints (Universal curl & PowerShell):
               1. List Registered Machines:
                  curl -s "%s/machines"
                  (PowerShell: curl.exe -s "%s/machines")

               2. Inspect 'OrderWorkflow' Machine Topology:
                  curl -s "%s/machines/OrderWorkflow"
                  (PowerShell: curl.exe -s "%s/machines/OrderWorkflow")

               3. List Active/Suspended Executions:
                  curl -s "%s/executions?status=SUSPENDED"
                  (PowerShell: curl.exe -s "%s/executions?status=SUSPENDED")

               4. Stream Real-Time Telemetry Events (SSE):
                  curl -N "%s/events/stream"
                  (PowerShell: curl.exe -N "%s/events/stream")

               5. Deliver Signal to Resume Suspended Order 'ORDER-DEMO-99':
                  Universal curl (single-line JSON for Bash, Zsh, CMD, PowerShell):
                  curl -X POST "%s/executions/signal" -H "Content-Type: application/json" -d "{\\"machineName\\":\\"OrderWorkflow\\",\\"correlationKey\\":\\"ORDER-DEMO-99\\",\\"signalName\\":\\"PaymentSignal\\",\\"payload\\":{\\"correlationKey\\":\\"ORDER-DEMO-99\\",\\"paymentMethod\\":\\"APPLE_PAY\\",\\"amountCents\\":9995}}"

                  PowerShell native (Invoke-RestMethod):
                  Invoke-RestMethod -Method Post -Uri "%s/executions/signal" -ContentType "application/json" -Body '{"machineName":"OrderWorkflow","correlationKey":"ORDER-DEMO-99","signalName":"PaymentSignal","payload":{"correlationKey":"ORDER-DEMO-99","paymentMethod":"APPLE_PAY","amountCents":9995}}'
            ========================================================================================
            Press Ctrl+C or Stop in IntelliJ to terminate.
            """.formatted(base, base, base, base, base, base, base, base, base, base, base));
    }
}
