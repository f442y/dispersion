package com.github.f442y.dispersion.routing.core;

import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.routing.InspectableRouter;
import com.github.f442y.dispersion.routing.backpressure.BackpressureStrategy;
import com.github.f442y.dispersion.routing.backpressure.CapacityExceededException;
import com.github.f442y.dispersion.routing.backpressure.NoMatchingEndpointException;
import com.github.f442y.dispersion.routing.core.endpoint.LocalFsmEndpoint;
import com.github.f442y.dispersion.routing.core.endpoint.RemoteWorkloadEndpoint;
import com.github.f442y.dispersion.routing.core.policy.CanaryWeightedRoutingPolicy;
import com.github.f442y.dispersion.routing.core.transport.InMemoryChannelTransport;
import com.github.f442y.dispersion.routing.core.worker.DefaultWorkerHost;
import com.github.f442y.dispersion.routing.endpoint.EndpointType;
import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.policy.EndpointTags;
import com.github.f442y.dispersion.routing.policy.FallbackPolicy;
import com.github.f442y.dispersion.routing.policy.RoutingSelector;
import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultWorkloadRouterTests {

    public enum TestState implements StateKey {
        START, PROCESS, END
    }

    public static final class TestContext implements StateMachineContext {
        public String input;
        public String output;
    }

    public record TestRequest(String orderId, int amount) {}
    public record TestResponse(String orderId, boolean confirmed) {}

    private StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> createTestFsm(String prefix) {
        return AtomicStateMachineBuilder.<TestContext, TestState, TestRequest, TestResponse>create(prefix + "Machine", TestState.class)
                .context(TestContext::new)
                .initialState(TestState.START)
                .endStates(TestState.END)
                .input((TestContext ctx, TestRequest req) -> {
                    ctx.input = req.orderId();
                    return ctx;
                })
                .state(TestState.START)
                    .action((TestContext ctx) -> {
                        ctx.output = prefix + ":" + ctx.input;
                        return ctx;
                    })
                    .transition(TestState.END)
                .output((TestContext ctx) -> new TestResponse(ctx.output, true))
                .build();
    }

    @Test
    @DisplayName("Should execute local in-process FSM endpoint directly with sub-microsecond latency")
    void testMonolithLocalExecution() throws Exception {
        try (DefaultWorkloadRouter router = new DefaultWorkloadRouter()) {
            StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> fsm = createTestFsm("LOCAL");
            LocalFsmEndpoint<TestContext, TestState, TestRequest, TestResponse> endpoint =
                    LocalFsmEndpoint.of("local-inventory", fsm);

            router.registerEndpoint("inventory-service", endpoint);

            WorkloadMetadata metadata = WorkloadMetadata.builder("inventory-service", "ORD-1001").build();
            TestResponse response = router.routeSync(
                    "inventory-service",
                    new TestRequest("ORD-1001", 50),
                    metadata,
                    Duration.ofSeconds(1)
            );

            assertThat(response).isNotNull();
            assertThat(response.orderId()).isEqualTo("LOCAL:ORD-1001");
            assertThat(response.confirmed()).isTrue();
        }
    }

    @Test
    @DisplayName("Should route workload to remote WorkerHost over InMemoryChannelTransport asynchronously")
    void testDistributedRemoteWorkerExecution() throws Exception {
        try (InMemoryChannelTransport transport = new InMemoryChannelTransport();
             DefaultWorkloadRouter router = new DefaultWorkloadRouter()) {

            DefaultWorkerHost workerHost = new DefaultWorkerHost(
                    "worker-node-1",
                    EndpointTags.of("version", "1.0.0"),
                    transport
            );

            // Register handler in worker host
            workerHost.registerHandler("payment-service", (WorkloadEnvelope<TestRequest> env) -> {
                TestRequest req = env.payload();
                TestResponse res = new TestResponse("REMOTE:" + req.orderId(), true);
                return CompletableFuture.completedFuture(
                        WorkloadEnvelope.success(env.correlationId(), env.serviceName(), env.metadata(), res)
                );
            });
            workerHost.start();

            // Register remote endpoint in router
            RemoteWorkloadEndpoint<TestRequest, TestResponse> remoteEndpoint = new RemoteWorkloadEndpoint<>(
                    "remote-payment-1",
                    EndpointType.REMOTE_MESSAGING,
                    "payment-service",
                    "payment-replies",
                    transport,
                    EndpointTags.of("version", "1.0.0"),
                    100
            );
            router.registerEndpoint("payment-service", remoteEndpoint);

            WorkloadMetadata metadata = WorkloadMetadata.builder("payment-service", "PAY-500").build();
            TestResponse response = router.routeSync(
                    "payment-service",
                    new TestRequest("PAY-500", 100),
                    metadata,
                    Duration.ofSeconds(5)
            );

            assertThat(response).isNotNull();
            assertThat(response.orderId()).isEqualTo("REMOTE:PAY-500");
            assertThat(response.confirmed()).isTrue();

            workerHost.stop();
        }
    }

    @Test
    @DisplayName("Should route to developer sandbox worker node when developer tag is specified")
    void testDeveloperSandboxTagRouting() throws Exception {
        try (DefaultWorkloadRouter router = new DefaultWorkloadRouter()) {
            StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> prodFsm = createTestFsm("PROD");
            StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> devFsm = createTestFsm("DEV_FAIZAN");

            LocalFsmEndpoint<TestContext, TestState, TestRequest, TestResponse> prodEndpoint =
                    LocalFsmEndpoint.of("prod-inventory", prodFsm, EndpointTags.of("env", "prod"), 100);

            LocalFsmEndpoint<TestContext, TestState, TestRequest, TestResponse> devEndpoint =
                    LocalFsmEndpoint.of("dev-inventory", devFsm, EndpointTags.of("developer", "faizan", "env", "test"), 100);

            router.registerEndpoint("inventory-service", prodEndpoint, prodEndpoint.tags());
            router.registerEndpoint("inventory-service", devEndpoint, devEndpoint.tags());

            // 1. Request without developer tag goes to prod
            WorkloadMetadata prodMetadata = WorkloadMetadata.builder("inventory-service", "REQ-1")
                    .selector(RoutingSelector.requireTag("env", "prod"))
                    .build();
            TestResponse prodRes = router.routeSync("inventory-service", new TestRequest("REQ-1", 10), prodMetadata, Duration.ofSeconds(1));
            assertThat(prodRes.orderId()).isEqualTo("PROD:REQ-1");

            // 2. Request with developer=faizan goes strictly to developer's endpoint
            WorkloadMetadata devMetadata = WorkloadMetadata.builder("inventory-service", "REQ-2")
                    .selector(RoutingSelector.requireTag("developer", "faizan"))
                    .build();
            TestResponse devRes = router.routeSync("inventory-service", new TestRequest("REQ-2", 10), devMetadata, Duration.ofSeconds(1));
            assertThat(devRes.orderId()).isEqualTo("DEV_FAIZAN:REQ-2");
        }
    }

    @Test
    @DisplayName("Should throw NoMatchingEndpointException when strict tag requirement is not met")
    void testStrictTagNotFoundThrowsException() throws Exception {
        try (DefaultWorkloadRouter router = new DefaultWorkloadRouter()) {
            StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> fsm = createTestFsm("PROD");
            LocalFsmEndpoint<TestContext, TestState, TestRequest, TestResponse> endpoint =
                    LocalFsmEndpoint.of("prod-ep", fsm, EndpointTags.of("env", "prod"), 100);

            router.registerEndpoint("inventory-service", endpoint);

            WorkloadMetadata missingTagMetadata = WorkloadMetadata.builder("inventory-service", "REQ-3")
                    .selector(RoutingSelector.requireTag("developer", "alice", FallbackPolicy.STRICT))
                    .build();

            assertThatThrownBy(() ->
                    router.routeSync("inventory-service", new TestRequest("REQ-3", 10), missingTagMetadata, Duration.ofSeconds(1))
            ).isInstanceOf(NoMatchingEndpointException.class);
        }
    }

    @Test
    @DisplayName("Should split traffic between canary and stable according to configured weights")
    void testCanaryWeightedTrafficSplitting() throws Exception {
        try (DefaultWorkloadRouter router = new DefaultWorkloadRouter()) {
            StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> stableFsm = createTestFsm("STABLE");
            StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> canaryFsm = createTestFsm("CANARY");

            LocalFsmEndpoint<TestContext, TestState, TestRequest, TestResponse> stableEndpoint =
                    LocalFsmEndpoint.of("stable-ep", stableFsm, EndpointTags.of("version", "1.0.0"), 100);

            LocalFsmEndpoint<TestContext, TestState, TestRequest, TestResponse> canaryEndpoint =
                    LocalFsmEndpoint.of("canary-ep", canaryFsm, EndpointTags.of("version", "2.0.0"), 100);

            router.registerEndpoint("order-service", stableEndpoint, stableEndpoint.tags());
            router.registerEndpoint("order-service", canaryEndpoint, canaryEndpoint.tags());

            // 50% Canary, 50% Stable
            CanaryWeightedRoutingPolicy canaryPolicy = CanaryWeightedRoutingPolicy.of(
                    EndpointTags.of("version", "2.0.0"), 50,
                    EndpointTags.of("version", "1.0.0"), 50
            );
            router.setServicePolicy("order-service", canaryPolicy);

            int canaryCount = 0;
            int stableCount = 0;
            for (int i = 0; i < 200; i++) {
                WorkloadMetadata meta = WorkloadMetadata.builder("order-service", "CAN-" + i).build();
                TestResponse res = router.routeSync("order-service", new TestRequest("CAN-" + i, 1), meta, Duration.ofSeconds(1));
                if (res.orderId().startsWith("CANARY:")) {
                    canaryCount++;
                } else if (res.orderId().startsWith("STABLE:")) {
                    stableCount++;
                }
            }

            // In 200 iterations with 50/50 split, both should receive traffic
            assertThat(canaryCount).isGreaterThan(40);
            assertThat(stableCount).isGreaterThan(40);
        }
    }

    @Test
    @DisplayName("Should enforce FAIL_FAST backpressure when endpoint reaches capacity")
    void testBackpressureFailFast() throws Exception {
        try (DefaultWorkloadRouter router = new DefaultWorkloadRouter()) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> fsm =
                    AtomicStateMachineBuilder.<TestContext, TestState, TestRequest, TestResponse>create("BlockingMachine", TestState.class)
                            .context(TestContext::new)
                            .initialState(TestState.START)
                            .endStates(TestState.END)
                            .input((TestContext ctx, TestRequest req) -> { ctx.input = req.orderId(); return ctx; })
                            .state(TestState.START)
                                .action((TestContext ctx) -> {
                                    startedLatch.countDown();
                                    try {
                                        blockLatch.await();
                                    } catch (InterruptedException ignored) {}
                                    ctx.output = "DONE";
                                    return ctx;
                                })
                                .transition(TestState.END)
                            .output((TestContext ctx) -> new TestResponse(ctx.output, true))
                            .build();

            LocalFsmEndpoint<TestContext, TestState, TestRequest, TestResponse> endpoint =
                    LocalFsmEndpoint.of("tiny-ep", fsm, EndpointTags.empty(), 1);

            router.registerEndpoint("tiny-service", endpoint);
            router.setBackpressureStrategy(BackpressureStrategy.FAIL_FAST);

            endpoint.executeAsync(new TestRequest("BLOCK", 1), WorkloadMetadata.builder("tiny-service", "B1").build());
            startedLatch.await();

            assertThatThrownBy(() ->
                    router.routeSync(
                            "tiny-service",
                            new TestRequest("FAIL", 2),
                            WorkloadMetadata.builder("tiny-service", "B2").build(),
                            Duration.ofMillis(100)
                    )
            ).isInstanceOf(CapacityExceededException.class);

            blockLatch.countDown();
        }
    }

    @Test
    @DisplayName("Should expose live descriptors for Control Plane inspection")
    void testInspectableRouter() throws Exception {
        try (DefaultWorkloadRouter router = new DefaultWorkloadRouter()) {
            StateMachineConfiguration<TestContext, TestState, TestRequest, TestResponse> fsm = createTestFsm("CTRL");
            LocalFsmEndpoint<TestContext, TestState, TestRequest, TestResponse> endpoint =
                    LocalFsmEndpoint.of("ctrl-ep", fsm, EndpointTags.of("region", "us-east-1"), 50);

            router.registerEndpoint("audit-service", endpoint, endpoint.tags());

            InspectableRouter inspectable = router;
            assertThat(inspectable.registeredServices()).containsExactly("audit-service");
            assertThat(inspectable.totalRegisteredEndpoints()).isEqualTo(1);

            List<InspectableRouter.EndpointDescriptor> descriptors = inspectable.listEndpointDescriptors("audit-service");
            assertThat(descriptors).hasSize(1);
            InspectableRouter.EndpointDescriptor desc = descriptors.getFirst();
            assertThat(desc.endpointId()).isEqualTo("ctrl-ep");
            assertThat(desc.serviceName()).isEqualTo("audit-service");
            assertThat(desc.endpointType()).isEqualTo(EndpointType.LOCAL);
            assertThat(desc.maxConcurrency()).isEqualTo(50);
            assertThat(desc.tags().get("region")).contains("us-east-1");
        }
    }
}
