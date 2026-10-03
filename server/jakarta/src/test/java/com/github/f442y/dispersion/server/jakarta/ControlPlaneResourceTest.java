package com.github.f442y.dispersion.server.jakarta;

import com.github.f442y.dispersion.control.InspectableMachine;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.event.DynamicTapManager;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.state.TransitionEvaluatedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.serialization.avaje.AvajeJsonSerializer;
import com.github.f442y.dispersion.serialization.json.JsonSerializer;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseBroadcaster;
import jakarta.ws.rs.sse.SseEventSink;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ControlPlaneResourceTest {

    private DefaultControlPlane controlPlane;
    private JsonSerializer jsonSerializer;
    private ControlPlaneResource resource;

    @BeforeEach
    void setUp() {
        controlPlane = new DefaultControlPlane();
        jsonSerializer = new AvajeJsonSerializer();
        resource = new ControlPlaneResource(controlPlane, jsonSerializer);
    }

    @Test
    @DisplayName("GET /machines returns registered machines")
    void listMachines() {
        controlPlane.register(new InspectableMachine() {
            @Override
            public @NonNull MachineDescriptor descriptor() {
                return new MachineDescriptor("OrderWorkflow", MachineType.ORCHESTRATION, "INIT", Set.of("PAID"), List.of("INIT", "PAID"), "stateDiagram-v2");
            }

            @Override
            public @NonNull CompletableFuture<SignalDeliveryResult> sendSignal(@NonNull String correlationKey, @NonNull String signalName, @Nullable Object payload) {
                return CompletableFuture.completedFuture(new SignalDeliveryResult(true, "Signal delivered", "OrderWorkflow", correlationKey, signalName, false, false, "RUNNING", null));
            }

            @Override
            public @NonNull Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.empty();
            }
        });

        Response response = resource.listMachines();
        assertThat(response.getStatus()).isEqualTo(200);
        String body = (String) response.getEntity();
        assertThat(body).contains("OrderWorkflow");
    }

    @Test
    @DisplayName("GET /executions lists summaries and filters by status")
    void listExecutions() {
        UUID executionId = UUID.randomUUID();
        Instant now = Instant.now();

        // Ingest TurnStartedEvent to create an execution summary
        ExecutionEvent startEvent = new TurnStartedEvent(executionId, "OrderWorkflow", "corr-123", now);
        controlPlane.getEventListener().onEvent(startEvent);

        Response response = resource.listExecutions("OrderWorkflow", "RUNNING", 10);
        assertThat(response.getStatus()).isEqualTo(200);
        String body = (String) response.getEntity();
        assertThat(body).contains("OrderWorkflow");
        assertThat(body).contains("corr-123");

        // Filter by COMPLETED should return empty list
        Response completedResponse = resource.listExecutions("OrderWorkflow", "COMPLETED", 10);
        assertThat(completedResponse.getStatus()).isEqualTo(200);
        String completedBody = (String) completedResponse.getEntity();
        assertThat(completedBody).isEqualTo("[]");
    }

    @Test
    @DisplayName("POST /executions/signal delivers signal via JSON payload")
    void sendSignal() {
        controlPlane.register(new InspectableMachine() {
            @Override
            public @NonNull MachineDescriptor descriptor() {
                return new MachineDescriptor("OrderWorkflow", MachineType.ORCHESTRATION, "INIT", Set.of(), List.of(), "stateDiagram-v2");
            }

            @Override
            public @NonNull CompletableFuture<SignalDeliveryResult> sendSignal(@NonNull String correlationKey, @NonNull String signalName, @Nullable Object payload) {
                return CompletableFuture.completedFuture(new SignalDeliveryResult(true, "Signal delivered", "OrderWorkflow", correlationKey, signalName, false, false, "RUNNING", null));
            }

            @Override
            public @NonNull Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.empty();
            }
        });

        String requestJson = """
                {
                    "machineName": "OrderWorkflow",
                    "correlationKey": "ORD-1",
                    "signalName": "PaymentReceived",
                    "payload": "{\\"amount\\": 100}"
                }
                """;

        Response response = resource.sendSignal(requestJson);
        assertThat(response.getStatus()).isEqualTo(200);
        String body = (String) response.getEntity();
        assertThat(body).contains("\"delivered\":true");
        assertThat(body).contains("PaymentReceived");
    }

    @Test
    @DisplayName("POST /executions/signal returns 400 for malformed payload")
    void sendSignalMalformed() {
        Response response = resource.sendSignal("invalid json");
        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getEntity().toString()).contains("Malformed signal request payload");
    }

    @Test
    @DisplayName("POST /machines/{name}/dispatch serializes complex result safely into JSON")
    void dispatchExecutionWithComplexResult() {
        controlPlane.register(new InspectableMachine() {
            @Override
            public @NonNull MachineDescriptor descriptor() {
                return new MachineDescriptor("ComplexWorkflow", MachineType.ORCHESTRATION, "INIT", Set.of(), List.of(), "stateDiagram-v2");
            }

            @Override
            public @NonNull CompletableFuture<SignalDeliveryResult> sendSignal(@NonNull String correlationKey, @NonNull String signalName, @Nullable Object payload) {
                return CompletableFuture.completedFuture(new SignalDeliveryResult(true, "Signal delivered", "ComplexWorkflow", correlationKey, signalName, false, false, "RUNNING", null));
            }

            @Override
            public @NonNull CompletableFuture<Object> dispatchExecution(@Nullable Object input) {
                return CompletableFuture.completedFuture(List.of("step1", "step2 with \"quotes\""));
            }

            @Override
            public @NonNull Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.empty();
            }
        });

        Response response = resource.dispatchExecution("ComplexWorkflow", "{}");
        assertThat(response.getStatus()).isEqualTo(200);
        String body = (String) response.getEntity();
        assertThat(body).contains("\"status\":\"DISPATCHED\"");
        assertThat(body).contains("\"result\":[\"step1\",\"step2 with \\\"quotes\\\"\"]");
    }

    @Test
    @DisplayName("POST /executions/signal returns 500 JSON with root cause when signal future fails")
    void sendSignalWithFailedFuture() {
        controlPlane.register(new InspectableMachine() {
            @Override
            public @NonNull MachineDescriptor descriptor() {
                return new MachineDescriptor("FailingMachine", MachineType.ORCHESTRATION, "INIT", Set.of(), List.of(), "stateDiagram-v2");
            }

            @Override
            public @NonNull CompletableFuture<SignalDeliveryResult> sendSignal(@NonNull String correlationKey, @NonNull String signalName, @Nullable Object payload) {
                return CompletableFuture.failedFuture(new IllegalStateException("Workflow instance locked"));
            }

            @Override
            public @NonNull Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.empty();
            }
        });

        String requestJson = """
                {
                    "machineName": "FailingMachine",
                    "correlationKey": "K1",
                    "signalName": "TestSignal"
                }
                """;

        Response response = resource.sendSignal(requestJson);
        assertThat(response.getStatus()).isEqualTo(500);
        String body = (String) response.getEntity();
        assertThat(body).contains("Failed to deliver signal: Workflow instance locked");
    }

    @Test
    @DisplayName("GET /executions/{machine}/{key}/checkpoint formats plaintext string as valid quoted JSON")
    void inspectCheckpointPlaintextReturnsQuotedJson() {
        controlPlane.register(new InspectableMachine() {
            @Override
            public @NonNull MachineDescriptor descriptor() {
                return new MachineDescriptor("PlainMachine", MachineType.ORCHESTRATION, "INIT", Set.of(), List.of(), "stateDiagram-v2");
            }

            @Override
            public @NonNull CompletableFuture<SignalDeliveryResult> sendSignal(@NonNull String correlationKey, @NonNull String signalName, @Nullable Object payload) {
                return CompletableFuture.completedFuture(new SignalDeliveryResult(true, "Signal delivered", "PlainMachine", correlationKey, signalName, false, false, "RUNNING", null));
            }

            @Override
            public @NonNull Optional<Object> inspectCheckpoint(@NonNull String correlationKey) {
                return Optional.of("PAYMENT_PENDING");
            }
        });

        Response response = resource.inspectCheckpoint("PlainMachine", "ORD-123");
        assertThat(response.getStatus()).isEqualTo(200);
        String body = (String) response.getEntity();
        assertThat(body).isEqualTo("\"PAYMENT_PENDING\"");
    }

    @Test
    @DisplayName("GET /events/stream streams events with dynamic tap lease on Java 25 virtual thread")
    void streamEventsWithDynamicTap() throws Exception {
        UUID executionId = UUID.randomUUID();
        Instant now = Instant.now();

        TestDynamicTapManager tapManager = new TestDynamicTapManager();
        controlPlane.registerTapManager(tapManager);

        TestSseEventSink eventSink = new TestSseEventSink();
        TestSse sse = new TestSse();

        resource.streamEvents(eventSink, sse, "OrderWorkflow", executionId.toString(), "all");

        // Verify tap was registered immediately
        assertThat(tapManager.hasActiveTap(executionId)).isTrue();

        // Ingest one lifecycle event and one granular event
        ExecutionEvent lifecycleEvent = new TurnStartedEvent(executionId, "OrderWorkflow", "corr-1", now);
        ExecutionEvent granularEvent = new TransitionEvaluatedEvent(executionId, "OrderWorkflow", "INIT", "DONE", now.plusMillis(10));

        controlPlane.getEventListener().onEvent(lifecycleEvent);
        controlPlane.getEventListener().onEvent(granularEvent);

        // Await delivery of both events
        eventSink.awaitEvents(2, 2, TimeUnit.SECONDS);

        assertThat(eventSink.events).hasSize(2);
        assertThat(eventSink.events.get(0).getName()).isEqualTo("message");
        assertThat(eventSink.events.get(0).getData().toString()).contains("TurnStartedEvent");
        assertThat(eventSink.events.get(1).getName()).isEqualTo("message");
        assertThat(eventSink.events.get(1).getData().toString()).contains("TransitionEvaluatedEvent");

        // Close event sink and verify tap is unregistered on disconnect
        eventSink.close();
        awaitCondition(() -> !tapManager.hasActiveTap(executionId), 2000);
        assertThat(tapManager.hasActiveTap(executionId)).isFalse();
    }

    @Test
    @DisplayName("Broken pipe / client disconnect during event send terminates SSE virtual thread and unregisters tap")
    void streamEventsDisconnectDetectsBrokenPipeAndReleasesTap() throws Exception {
        UUID executionId = UUID.randomUUID();
        Instant now = Instant.now();

        TestDynamicTapManager tapManager = new TestDynamicTapManager();
        controlPlane.registerTapManager(tapManager);

        TestSseEventSink eventSink = new TestSseEventSink();
        TestSse sse = new TestSse();

        resource.streamEvents(eventSink, sse, "OrderWorkflow", executionId.toString(), "all");
        assertThat(tapManager.hasActiveTap(executionId)).isTrue();

        // Simulate abrupt client disconnect
        eventSink.close();

        // Triggering an event forces a write attempt that fails
        ExecutionEvent event = new TurnStartedEvent(executionId, "OrderWorkflow", "corr-1", now);
        controlPlane.getEventListener().onEvent(event);

        // Tap lease must be cleaned up promptly
        awaitCondition(() -> !tapManager.hasActiveTap(executionId), 2000);
        assertThat(tapManager.hasActiveTap(executionId)).isFalse();
    }

    @Test
    @DisplayName("GET /executions/{id} returns 404 for unknown execution")
    void getUnknownExecution() {
        Response response = resource.getExecution("unknown-uuid");
        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    @DisplayName("OPTIONS returns 204 No Content for CORS preflight")
    void handleOptionsPreflight() {
        Response response = resource.handleOptions();
        assertThat(response.getStatus()).isEqualTo(204);
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
    }

    // =========================================================================
    // Test Doubles
    // =========================================================================

    private static final class TestDynamicTapManager implements DynamicTapManager {
        private final Set<UUID> tapped = ConcurrentHashMap.newKeySet();

        @Override
        public void registerTap(@NonNull UUID executionId) {
            tapped.add(executionId);
        }

        @Override
        public void registerTap(@NonNull UUID executionId, @NonNull Duration leaseDuration) {
            tapped.add(executionId);
        }

        @Override
        public void unregisterTap(@NonNull UUID executionId) {
            tapped.remove(executionId);
        }

        @Override
        public boolean hasActiveTap(@NonNull UUID executionId) {
            return tapped.contains(executionId);
        }

        @Override
        public int activeTapCount() {
            return tapped.size();
        }

        @Override
        public void pruneExpired() {
        }

        @Override
        public void clear() {
            tapped.clear();
        }
    }

    private static final class TestSseEventSink implements SseEventSink {
        private final List<OutboundSseEvent> events = new CopyOnWriteArrayList<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private CountDownLatch latch = new CountDownLatch(0);

        void awaitEvents(int count, long timeout, TimeUnit unit) throws InterruptedException {
            this.latch = new CountDownLatch(count);
            latch.await(timeout, unit);
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public CompletionStage<?> send(OutboundSseEvent event) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(new IOException("Broken pipe"));
            }
            events.add(event);
            latch.countDown();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static final class TestSse implements Sse {
        @Override
        public OutboundSseEvent.Builder newEventBuilder() {
            return new TestOutboundSseEventBuilder();
        }

        @Override
        public SseBroadcaster newBroadcaster() {
            throw new UnsupportedOperationException();
        }
    }

    @SuppressWarnings("rawtypes")
    private static final class TestOutboundSseEventBuilder implements OutboundSseEvent.Builder {
        private String name;
        private String comment;
        private MediaType mediaType;
        private Object data;

        @Override
        public OutboundSseEvent.Builder name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public OutboundSseEvent.Builder id(String id) {
            return this;
        }

        @Override
        public OutboundSseEvent.Builder reconnectDelay(long reconnectDelay) {
            return this;
        }

        @Override
        public OutboundSseEvent.Builder mediaType(MediaType mediaType) {
            this.mediaType = mediaType;
            return this;
        }

        @Override
        public OutboundSseEvent.Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        @Override
        public OutboundSseEvent.Builder data(Class type, Object data) {
            this.data = data;
            return this;
        }

        @Override
        public OutboundSseEvent.Builder data(GenericType type, Object data) {
            this.data = data;
            return this;
        }

        @Override
        public OutboundSseEvent.Builder data(Object data) {
            this.data = data;
            return this;
        }

        @Override
        public OutboundSseEvent build() {
            return new OutboundSseEvent() {
                @Override public String getName() { return name; }
                @Override public String getId() { return null; }
                @Override public String getComment() { return comment; }
                @Override public long getReconnectDelay() { return -1; }
                @Override public boolean isReconnectDelaySet() { return false; }
                @Override public Class<?> getType() { return String.class; }
                @Override public java.lang.reflect.Type getGenericType() { return String.class; }
                @Override public MediaType getMediaType() { return mediaType; }
                @Override public Object getData() { return data; }
            };
        }
    }
}
