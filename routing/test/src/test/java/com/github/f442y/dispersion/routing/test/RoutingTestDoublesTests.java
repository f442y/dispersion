package com.github.f442y.dispersion.routing.test;

import com.github.f442y.dispersion.routing.endpoint.WorkloadMetadata;
import com.github.f442y.dispersion.routing.worker.WorkloadEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RoutingTestDoublesTests {

    public record TestMsg(String message) {}

    @Test
    @DisplayName("RecordingWorkloadRouter should record all sync and async routing calls")
    void testRecordingRouter() throws Exception {
        try (RecordingWorkloadRouter router = new RecordingWorkloadRouter()) {
            FakeWorkloadEndpoint<String, String> endpoint = FakeWorkloadEndpoint.of("fake-1", (String in) -> "ECHO:" + in);
            router.registerEndpoint("echo-service", endpoint);

            WorkloadMetadata meta = WorkloadMetadata.builder("echo-service", "C-1").build();
            String result = router.routeSync("echo-service", "hello", meta, Duration.ofSeconds(1));

            assertThat(result).isEqualTo("ECHO:hello");
            assertThat(router.routedCalls()).hasSize(1);
            assertThat(router.routedCalls().getFirst().serviceName()).isEqualTo("echo-service");
            assertThat(router.routedCalls().getFirst().input()).isEqualTo("hello");
            assertThat(endpoint.receivedInputs()).containsExactly("hello");
        }
    }

    @Test
    @DisplayName("SimulatedNetworkTransport should drop packets when drop rate is 1.0 or partitioned")
    void testSimulatedNetworkDrop() throws Exception {
        try (SimulatedNetworkTransport transport = new SimulatedNetworkTransport()) {

            transport.setPartitioned(true);

            WorkloadMetadata meta = WorkloadMetadata.builder("test-svc", "CORR-1").build();
            WorkloadEnvelope<TestMsg> env = WorkloadEnvelope.request(meta, new TestMsg("ping"));

            CompletableFuture<WorkloadEnvelope<TestMsg>> future = transport.requestReply(
                    "test-svc",
                    "test-replies",
                    env,
                    Duration.ofMillis(50)
            );

            assertThatThrownBy(future::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);
        }
    }
}
