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
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Interactive developer demo application running the Dispersion Control Plane
 * embedded in a modern Spring Boot 4.1 application on Java 25 virtual threads.
 * <p>
 * Can be run directly from IntelliJ IDEA (right-click -&gt; 'Run DispersionDemoApp.main()')
 * or via Maven:
 * {@code .\mvnw compile exec:java -pl examples -Dexec.mainClass="com.github.f442y.dispersion.examples.DispersionDemoApp"}
 */
@SpringBootApplication
public class DispersionDemoApp {

    private static final Logger log = LoggerFactory.getLogger(DispersionDemoApp.class);
    private static final String BASE_PATH = "/api/v1";

    public DispersionDemoApp() {}

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
    // Spring Beans Configuration
    // =========================================================================

    @Bean(destroyMethod = "close")
    public DefaultControlPlane controlPlane() {
        return new DefaultControlPlane();
    }

    @Bean
    public JsonSerializer jsonSerializer() {
        return new AvajeJsonSerializer();
    }

    @Bean(destroyMethod = "close")
    public OrchestrationExecutor<OrderContext, OrderState, Object, String> orderWorkflowExecutor(
            DefaultControlPlane controlPlane
    ) {
        InMemoryCheckpointStore<OrderContext, OrderState> orderStore = new InMemoryCheckpointStore<>();

        OrchestrationExecutor<OrderContext, OrderState, Object, String> executor =
                OrchestrationBuilder.<OrderContext, OrderState, Object, String>create("OrderWorkflow", OrderState.class)
                        .checkpointStore(orderStore)
                        .eventListener(controlPlane.getEventListener())
                        .context(OrderContext::new)
                        .correlationKey(ctx -> ctx.orderId)
                        .initialState(OrderState.VALIDATE_ORDER)
                        .endStates(OrderState.COMPLETED, OrderState.CANCELLED)
                        .input((ctx, in) -> {
                            if (in instanceof OrderContext orderIn) {
                                ctx.orderId = orderIn.orderId;
                                ctx.customerId = orderIn.customerId;
                                ctx.amountCents = orderIn.amountCents;
                            }
                            if (ctx.orderId == null || ctx.orderId.isBlank()) {
                                long id = System.currentTimeMillis();
                                ctx.orderId = "ORD-" + (id % 10000);
                                ctx.customerId = "CUST-" + (id % 100);
                                ctx.amountCents = (int) ((id % 4500) + 500);
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

        controlPlane.register(executor.asInspectableMachine());
        return executor;
    }

    @Bean
    public AtomicStateMachineExecutor<PipelineContext, PipelineState, Object, Double> metricsPipelineExecutor(
            DefaultControlPlane controlPlane
    ) {
        AtomicStateMachineExecutor<PipelineContext, PipelineState, Object, Double> executor =
                AtomicStateMachineBuilder.<PipelineContext, PipelineState, Object, Double>create("MetricsPipeline", PipelineState.class)
                        .context(PipelineContext::new)
                        .initialState(PipelineState.INGEST)
                        .endStates(PipelineState.FINISHED)
                        .eventListener(controlPlane.getEventListener())
                        .input((ctx, val) -> {
                            ctx.sensorId = "sensor-" + (System.currentTimeMillis() % 10);
                            double d;
                            if (val instanceof Number num) {
                                d = num.doubleValue();
                            } else if (val instanceof String s && !s.isBlank()) {
                                try {
                                    d = Double.parseDouble(s.trim());
                                } catch (NumberFormatException _) {
                                    d = Math.round((Math.random() * 85.0 + 15.0) * 10.0) / 10.0;
                                }
                            } else {
                                d = Math.round((Math.random() * 85.0 + 15.0) * 10.0) / 10.0;
                            }
                            ctx.metricValue = d;
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

        controlPlane.register(executor.asInspectableMachine());
        return executor;
    }

    @Bean(destroyMethod = "close")
    public BatchOrchestrationExecutor<BatchCtx, ItemCtx, BatchStage, String> batchProcessorExecutor(
            DefaultControlPlane controlPlane
    ) {
        BatchOrchestrationExecutor<BatchCtx, ItemCtx, BatchStage, String> executor =
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

        controlPlane.register(executor.asInspectableMachine());
        return executor;
    }

    // =========================================================================
    // Startup Seeding & Background Activity Simulator
    // =========================================================================

    @EventListener(ApplicationReadyEvent.class)
    @SuppressWarnings("unchecked")
    public void onApplicationReady(ApplicationReadyEvent event) throws Exception {
        ApplicationContext context = event.getApplicationContext();
        Integer port = context.getEnvironment().getProperty("local.server.port", Integer.class);
        if (port == null) {
            port = context.getEnvironment().getProperty("server.port", Integer.class, 8080);
        }

        var orderExecutor = (OrchestrationExecutor<OrderContext, OrderState, Object, String>)
                context.getBean("orderWorkflowExecutor");
        var pipelineExecutor = (AtomicStateMachineExecutor<PipelineContext, PipelineState, Object, Double>)
                context.getBean("metricsPipelineExecutor");
        var batchExecutor = (BatchOrchestrationExecutor<BatchCtx, ItemCtx, BatchStage, String>)
                context.getBean("batchProcessorExecutor");

        // 1. Pre-seed a suspended order workflow for immediate manual signal injection testing
        OrderContext preSeededOrder = new OrderContext();
        preSeededOrder.orderId = "ORDER-DEMO-99";
        preSeededOrder.customerId = "CUST-42";
        preSeededOrder.amountCents = 9995;
        orderExecutor.dispatchTurnSync(null, preSeededOrder);

        // Pre-seed batch items
        batchExecutor.dispatchBatchSync(List.of(new ItemCtx("ITEM-A"), new ItemCtx("ITEM-B")));

        printBanner("localhost", port);

        // 2. Start background virtual thread activity generator
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
    }

    private static void printBanner(String host, int port) {
        String base = "http://" + host + ":" + port + BASE_PATH;
        String webUrl = "http://" + host + ":" + port;
        System.out.println("""
            ========================================================================================
            🚀 DISPERSION CONTROL PLANE & SPRING BOOT 4.1 DEMO IS RUNNING
            ========================================================================================
            Base HTTP URL: %s
            Web Landing:   %s

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
            """.formatted(base, webUrl, base, base, base, base, base, base, base, base, base, base));
    }

    // =========================================================================
    // Main Entry Point
    // =========================================================================

    public static void main(String[] args) {
        log.info("Starting Dispersion Control Plane Demo Application on Spring Boot 4.1...");
        SpringApplication.run(DispersionDemoApp.class, args);
    }
}
