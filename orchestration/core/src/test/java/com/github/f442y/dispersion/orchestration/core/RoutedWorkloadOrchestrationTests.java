package com.github.f442y.dispersion.orchestration.core;

import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.WorkloadExecutionMode;
import com.github.f442y.dispersion.routing.WorkloadRouter;
import com.github.f442y.dispersion.routing.core.DefaultWorkloadRouter;
import com.github.f442y.dispersion.routing.core.endpoint.LocalFsmEndpoint;
import com.github.f442y.dispersion.routing.core.endpoint.RemoteWorkloadEndpoint;
import com.github.f442y.dispersion.routing.core.policy.CanaryWeightedRoutingPolicy;
import com.github.f442y.dispersion.routing.core.policy.LocalFirstRoutingPolicy;
import com.github.f442y.dispersion.routing.core.transport.InMemoryChannelTransport;
import com.github.f442y.dispersion.routing.core.worker.DefaultWorkerHost;
import com.github.f442y.dispersion.routing.endpoint.EndpointType;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import com.github.f442y.dispersion.routing.test.FakeWorkloadEndpoint;
import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests verifying unified location-agnostic workload routing:
 * in-process monolith execution, remote distributed workers over message transport,
 * Canary traffic routing, Developer Sandbox isolation, and automated routed Saga compensations.
 */
public class RoutedWorkloadOrchestrationTests {

    private static final Logger log = LoggerFactory.getLogger(RoutedWorkloadOrchestrationTests.class);

    // --- State Keys ---
    public enum OrderState implements StateKey {
        INIT,
        RESERVE_INVENTORY,
        PROCESS_PAYMENT,
        SHIP_ORDER,
        COMPLETED,
        FAILED
    }

    public enum InventoryAtomicState implements StateKey {
        CHECK_AND_RESERVE,
        DONE
    }

    // --- Contexts ---
    public static class OrderContext implements StateMachineContext {
        public String orderId;
        public String reservationId;
        public String trackingId;
        public List<String> sagaTrace = new CopyOnWriteArrayList<>();
    }

    public static class InventoryContext implements StateMachineContext {
        public ReserveRequest request;
        public ReserveResponse response;
    }

    // --- Typed Domain Records ---
    public record ReserveRequest(String orderId, int quantity) {}
    public record ReserveResponse(boolean success, String reservationId) {}
    public record ReleaseRequest(String reservationId) {}
    public record ReleaseResponse(boolean released) {}

    public record ShippingRequest(String orderId, String destination) {}
    public record ShippingResponse(boolean shipped, String trackingId) {}

    /**
     * 1. Monolith In-Process Execution:
     * Invocations route to an in-memory LocalFsmEndpoint with zero network overhead.
     */
    @Test
    public void testMonolithLocalInProcessExecution() throws Exception {
        try (WorkloadRouter router = new DefaultWorkloadRouter()) {

            // Build local Inventory Atomic State Machine
            StateMachineConfiguration<InventoryContext, InventoryAtomicState, ReserveRequest, ReserveResponse> inventoryMachine =
                    AtomicStateMachineBuilder.<InventoryContext, InventoryAtomicState, ReserveRequest, ReserveResponse>create(InventoryAtomicState.class)
                            .context(InventoryContext::new)
                            .initialState(InventoryAtomicState.CHECK_AND_RESERVE)
                            .input((ctx, req) -> {
                                ctx.request = req;
                                return ctx;
                            })
                            .state(InventoryAtomicState.CHECK_AND_RESERVE)
                                .action(ctx -> {
                                    ctx.response = new ReserveResponse(true, "RES-LOCAL-" + ctx.request.orderId());
                                    return ctx;
                                })
                                .transition(InventoryAtomicState.DONE)
                            .endStates(InventoryAtomicState.DONE)
                            .output(ctx -> ctx.response)
                            .build();

            // Register as LocalFsmEndpoint on the router
            LocalFsmEndpoint<InventoryContext, InventoryAtomicState, ReserveRequest, ReserveResponse> localEndpoint =
                    new LocalFsmEndpoint<>(
                            "inventory-local-1",
                            inventoryMachine,
                            EndpointTags.of("tier", "monolith"),
                            100
                    );
            router.registerEndpoint("inventory-service", localEndpoint);

            // Build Orchestration using fluent .invoke(...)
            try (OrchestrationStateMachineExecutor<OrderContext, OrderState, String, OrderContext> executor =
                    OrchestrationStateMachineBuilder.<OrderContext, OrderState, String, OrderContext>create("MonolithOrchestrator", OrderState.class)
                            .context(OrderContext::new)
                            .initialState(OrderState.INIT)
                            .workloadRouter(router)
                            .input((ctx, orderId) -> {
                                ctx.orderId = orderId;
                                return ctx;
                            })
                            .state(OrderState.INIT)
                                .action(ctx -> {
                                    ctx.sagaTrace.add("INIT");
                                    return ctx;
                                })
                                .transition(OrderState.RESERVE_INVENTORY)
                            .state(OrderState.RESERVE_INVENTORY)
                                .invoke("inventory-service", ReserveRequest.class, ReserveResponse.class)
                                    .input(ctx -> new ReserveRequest(ctx.orderId, 2))
                                    .output((ctx, res) -> {
                                        ctx.reservationId = res.reservationId();
                                        ctx.sagaTrace.add("RESERVED:" + res.reservationId());
                                        return ctx;
                                    })
                                    .transition(OrderState.COMPLETED)
                            .endStates(OrderState.COMPLETED, OrderState.FAILED)
                            .output(ctx -> ctx)
                            .buildExecutor()) {

                OrderContext result = executor.dispatchSync("ORD-1001");
                assertNotNull(result);
                assertEquals("RES-LOCAL-ORD-1001", result.reservationId);
                assertThat(result.sagaTrace).containsExactly("INIT", "RESERVED:RES-LOCAL-ORD-1001");
            }
        }
    }

    /**
     * 2. Distributed Worker Remote Execution:
     * Controller dispatches through WorkloadRouter over an InMemoryChannelTransport
     * to a standalone WorkerHost executing on virtual threads.
     */
    @Test
    public void testDistributedWorkerRemoteExecution() throws Exception {
        try (InMemoryChannelTransport transport = new InMemoryChannelTransport();
             WorkloadRouter router = new DefaultWorkloadRouter();
             DefaultWorkerHost workerHost = new DefaultWorkerHost(
                     "shipping-worker-1",
                     EndpointTags.of("tier", "worker", "region", "us-east-1"),
                     transport
             )) {

            // Register shipping handler on standalone WorkerHost
            workerHost.registerHandler(
                    "shipping-service",
                    (WorkloadEnvelope<ShippingRequest> env) -> {
                        ShippingRequest sr = env.payload();
                        ShippingResponse res = new ShippingResponse(true, "TRACK-DISTRIBUTED-" + (sr != null ? sr.orderId() : "NONE"));
                        return CompletableFuture.completedFuture(
                                WorkloadEnvelope.success(env.correlationId(), env.serviceName(), env.metadata(), res)
                        );
                    }
            );
            workerHost.start();

            // Register RemoteWorkloadEndpoint on the router
            RemoteWorkloadEndpoint<ShippingRequest, ShippingResponse> remoteEndpoint =
                    new RemoteWorkloadEndpoint<>(
                            "shipping-remote-1",
                            EndpointType.REMOTE_MESSAGING,
                            "shipping-service",
                            "orchestrator-replies",
                            transport,
                            EndpointTags.of("tier", "worker"),
                            50
                    );
            router.registerEndpoint("shipping-service", remoteEndpoint);

            // Build Orchestration calling distributed worker
            try (OrchestrationStateMachineExecutor<OrderContext, OrderState, String, OrderContext> executor =
                    OrchestrationStateMachineBuilder.<OrderContext, OrderState, String, OrderContext>create("DistributedOrchestrator", OrderState.class)
                            .context(OrderContext::new)
                            .initialState(OrderState.INIT)
                            .workloadRouter(router)
                            .input((ctx, orderId) -> {
                                ctx.orderId = orderId;
                                return ctx;
                            })
                            .state(OrderState.INIT)
                                .action(ctx -> ctx)
                                .transition(OrderState.SHIP_ORDER)
                            .state(OrderState.SHIP_ORDER)
                                .invoke("shipping-service", ShippingRequest.class, ShippingResponse.class)
                                    .input(ctx -> new ShippingRequest(ctx.orderId, "Seattle, WA"))
                                    .output((ctx, res) -> {
                                        ctx.trackingId = res.trackingId();
                                        return ctx;
                                    })
                                    .timeout(Duration.ofSeconds(5))
                                    .transition(OrderState.COMPLETED)
                            .endStates(OrderState.COMPLETED, OrderState.FAILED)
                            .output(ctx -> ctx)
                            .buildExecutor()) {

                OrderContext result = executor.dispatchSync("ORD-555");
                assertNotNull(result);
                assertEquals("TRACK-DISTRIBUTED-ORD-555", result.trackingId);
            }
        }
    }

    /**
     * 3. Developer Sandbox Isolation Test:
     * Validates that requests tagged with developer=faizan strictly route to the developer's
     * sandbox node, while default traffic routes to the production pool.
     */
    @Test
    public void testDeveloperSandboxIsolation() throws Exception {
        try (WorkloadRouter router = new DefaultWorkloadRouter()) {

            FakeWorkloadEndpoint<String, String> prodWorker =
                    FakeWorkloadEndpoint.of("prod-worker", EndpointTags.of("tier", "prod"), (String in) -> "PROD:" + in);

            FakeWorkloadEndpoint<String, String> faizanSandbox =
                    FakeWorkloadEndpoint.of("faizan-worker", EndpointTags.of("developer", "faizan"), (String in) -> "SANDBOX_FAIZAN:" + in);

            router.registerEndpoint("scoring-service", prodWorker);
            router.registerEndpoint("scoring-service", faizanSandbox);

            // Step 1: Default orchestration without developer tag hits production worker
            try (OrchestrationStateMachineExecutor<OrderContext, OrderState, String, String> prodOrch =
                    OrchestrationStateMachineBuilder.<OrderContext, OrderState, String, String>create("ProdOrchestrator", OrderState.class)
                            .context(OrderContext::new)
                            .initialState(OrderState.INIT)
                            .workloadRouter(router)
                            .state(OrderState.INIT)
                                .invoke("scoring-service", String.class, String.class)
                                    .input(_ -> "order-data")
                                    .output((ctx, out) -> { ctx.orderId = out; return ctx; })
                                    .routingSelector(RoutingSelector.requireTag("tier", "prod"))
                                    .transition(OrderState.COMPLETED)
                            .endStates(OrderState.COMPLETED)
                            .output(ctx -> ctx.orderId)
                            .buildExecutor()) {

                String prodResult = prodOrch.dispatchSync(null);
                assertEquals("PROD:order-data", prodResult);
            }

            // Step 2: Developer sandbox orchestration strictly routes to Faizan's worker
            try (OrchestrationStateMachineExecutor<OrderContext, OrderState, String, String> devOrch =
                    OrchestrationStateMachineBuilder.<OrderContext, OrderState, String, String>create("DevOrchestrator", OrderState.class)
                            .context(OrderContext::new)
                            .initialState(OrderState.INIT)
                            .workloadRouter(router)
                            .state(OrderState.INIT)
                                .invoke("scoring-service", String.class, String.class)
                                    .input(_ -> "order-data")
                                    .output((ctx, out) -> { ctx.orderId = out; return ctx; })
                                    .routingSelector(RoutingSelector.requireTag("developer", "faizan"))
                                    .transition(OrderState.COMPLETED)
                            .endStates(OrderState.COMPLETED)
                            .output(ctx -> ctx.orderId)
                            .buildExecutor()) {

                String devResult = devOrch.dispatchSync(null);
                assertEquals("SANDBOX_FAIZAN:order-data", devResult);
            }
        }
    }

    /**
     * 4. Canary Rolling Update Traffic Split Test:
     * Verifies that CanaryWeightedRoutingPolicy splits traffic between version 1.0.0 and 2.0.0.
     */
    @Test
    public void testCanaryRollingUpdateTrafficSplit() throws Exception {
        try (WorkloadRouter router = new DefaultWorkloadRouter()) {
            router.setDefaultPolicy(CanaryWeightedRoutingPolicy.of(
                    EndpointTags.of("version", "2.0.0"), 25, // 25% canary
                    EndpointTags.of("version", "1.0.0"), 75  // 75% stable
            ));

            FakeWorkloadEndpoint<String, String> v1 =
                    FakeWorkloadEndpoint.of("worker-v1", EndpointTags.of("version", "1.0.0"), (String in) -> "V1");

            FakeWorkloadEndpoint<String, String> v2 =
                    FakeWorkloadEndpoint.of("worker-v2", EndpointTags.of("version", "2.0.0"), (String in) -> "V2");

            router.registerEndpoint("pricing-service", v1);
            router.registerEndpoint("pricing-service", v2);

            try (OrchestrationStateMachineExecutor<OrderContext, OrderState, Void, String> orch =
                    OrchestrationStateMachineBuilder.<OrderContext, OrderState, Void, String>create("CanaryOrchestrator", OrderState.class)
                            .context(OrderContext::new)
                            .initialState(OrderState.INIT)
                            .workloadRouter(router)
                            .state(OrderState.INIT)
                                .invoke("pricing-service", String.class, String.class)
                                    .input(_ -> "calc")
                                    .output((ctx, out) -> { ctx.orderId = out; return ctx; })
                                    .transition(OrderState.COMPLETED)
                            .endStates(OrderState.COMPLETED)
                            .output(ctx -> ctx.orderId)
                            .buildExecutor()) {

                int v1Count = 0;
                int v2Count = 0;
                for (int i = 0; i < 200; i++) {
                    String res = orch.dispatchSync(null);
                    if ("V1".equals(res)) v1Count++;
                    else if ("V2".equals(res)) v2Count++;
                }

                log.atInfo()
                        .addKeyValue("v1_count", v1Count)
                        .addKeyValue("v2_count", v2Count)
                        .log("Canary distribution observed");

                assertTrue(v1Count > 100, "V1 primary should receive majority traffic");
                assertTrue(v2Count > 20, "V2 canary should receive non-trivial traffic");
            }
        }
    }

    /**
     * 5. Distributed LIFO Saga Compensation Rollback:
     * Step 1: Local state step (records LocalCompensation).
     * Step 2: Routed step calling remote inventory worker (reserves, records RoutedCompensation).
     * Step 3: Fails downstream during payment!
     * Driver rolls back LIFO:
     * - Dispatches ReleaseRequest compensation envelope back to inventory worker over transport!
     * - Executes Step 1 local compensation!
     */
    @Test
    public void testDistributedLifoSagaCompensationRollback() throws Exception {
        try (InMemoryChannelTransport transport = new InMemoryChannelTransport();
             WorkloadRouter router = new DefaultWorkloadRouter();
             DefaultWorkerHost workerHost = new DefaultWorkerHost(
                     "inventory-worker-host",
                     EndpointTags.of("tier", "worker"),
                     transport
             )) {

            List<String> releasedReservations = new CopyOnWriteArrayList<>();

            // Bind worker handling both reservations and compensations
            workerHost.registerHandler(
                    "inventory-service",
                    (WorkloadEnvelope<Object> env) -> {
                        Object req = env.payload();
                        Object res;
                        if (req instanceof ReserveRequest rr) {
                            res = new ReserveResponse(true, "RES-" + rr.orderId());
                        } else if (req instanceof ReleaseRequest rel) {
                            releasedReservations.add(rel.reservationId());
                            res = new ReleaseResponse(true);
                        } else {
                            throw new IllegalArgumentException("Unknown request: " + req);
                        }
                        return CompletableFuture.completedFuture(
                                WorkloadEnvelope.success(env.correlationId(), env.serviceName(), env.metadata(), res)
                        );
                    }
            );
            workerHost.start();

            // Register RemoteWorkloadEndpoint on router
            RemoteWorkloadEndpoint<Object, Object> remoteEndpoint =
                    new RemoteWorkloadEndpoint<>(
                            "inventory-remote-1",
                            EndpointType.REMOTE_MESSAGING,
                            "inventory-service",
                            "orch-replies",
                            transport,
                            EndpointTags.of("tier", "worker"),
                            50
                    );
            router.registerEndpoint("inventory-service", remoteEndpoint);

            // Build Orchestration with Step 1 (Local), Step 2 (Routed), Step 3 (Failing)
            try (OrchestrationStateMachineExecutor<OrderContext, OrderState, String, Void> executor =
                    OrchestrationStateMachineBuilder.<OrderContext, OrderState, String, Void>create("SagaOrchestrator", OrderState.class)
                            .context(OrderContext::new)
                            .initialState(OrderState.INIT)
                            .workloadRouter(router)
                            .input((ctx, id) -> { ctx.orderId = id; return ctx; })
                            // Step 1: Local Step
                            .state(OrderState.INIT)
                                .action(ctx -> {
                                    ctx.sagaTrace.add("STEP_1_LOCAL_EXEC");
                                    return ctx;
                                })
                                .compensate(ctx -> {
                                    ctx.sagaTrace.add("STEP_1_LOCAL_COMPENSATED");
                                    return ctx;
                                })
                                .transition(OrderState.RESERVE_INVENTORY)
                            // Step 2: Routed Service Step with Routed Compensation!
                            .state(OrderState.RESERVE_INVENTORY)
                                .invoke("inventory-service", ReserveRequest.class, ReserveResponse.class)
                                    .input(ctx -> new ReserveRequest(ctx.orderId, 1))
                                    .output((ctx, res) -> {
                                        ctx.reservationId = res.reservationId();
                                        ctx.sagaTrace.add("STEP_2_RESERVED:" + res.reservationId());
                                        return ctx;
                                    })
                                    .compensate(ctx -> new ReleaseRequest(ctx.reservationId)) // Routed Compensation!
                                    .transition(OrderState.PROCESS_PAYMENT)
                            // Step 3: Fails downstream!
                            .state(OrderState.PROCESS_PAYMENT)
                                .action(_ -> {
                                    throw new IllegalStateException("Payment gateway unavailable - triggering Saga rollback");
                                })
                                .transition(OrderState.COMPLETED)
                            .endStates(OrderState.COMPLETED, OrderState.FAILED)
                            .buildExecutor()) {

                OrderContext context = new OrderContext();
                assertThrows(Exception.class, () -> executor.dispatchSync(context, "ORD-SAGA-999"));

                // Verify LIFO compensation order:
                // 1. Remote worker received ReleaseRequest compensation!
                assertThat(releasedReservations).containsExactly("RES-ORD-SAGA-999");

                // 2. Local step 1 compensation ran!
                assertThat(context.sagaTrace).contains(
                        "STEP_1_LOCAL_EXEC",
                        "STEP_2_RESERVED:RES-ORD-SAGA-999",
                        "STEP_1_LOCAL_COMPENSATED"
                );
            }
        }
    }
}
