package com.github.f442y.dispersion.orchestration.core.messaging;

import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import com.github.f442y.dispersion.orchestration.OrchestrationStatus;
import com.github.f442y.dispersion.orchestration.OrchestrationTurnResult;
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineExecutor;
import com.github.f442y.dispersion.orchestration.messaging.SignalConsumer;
import com.github.f442y.dispersion.orchestration.messaging.SignalMessage;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerAgnosticMessagingTests {

    public enum ShippingState implements StateKey {
        INITIALIZE,
        NOTIFY_WAREHOUSE,
        AWAIT_PICKED_SIGNAL,
        AWAIT_CARRIER_SIGNAL,
        COMPLETED,
        FAILED
    }

    public static class ShippingContext implements StateMachineContext {
        public String shipmentId;
        public boolean warehouseNotified = false;
        public boolean packagePicked = false;
        public boolean carrierDispatched = false;
        public String warehouseId;
        public String trackingNumber;
        public List<String> eventLog = new ArrayList<>();
    }

    public record WarehouseNotificationCommand(String correlationKey, String warehouseCode) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "WarehouseNotificationCommand";
        }
    }

    public record PackagePickedCommand(String correlationKey, String warehouseId) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "PackagePickedCommand";
        }
    }

    public record CarrierDispatchedCommand(String correlationKey, String trackingNumber) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "CarrierDispatchedCommand";
        }
    }

    /**
     * Tests decoupled event-driven orchestration coordinating outbound topic publishing
     * and inbound broker signal consumption via the in-memory broker abstraction.
     */
    @Test
    public void testBrokerAgnosticPubSubWithInMemoryBroker() throws Exception {
        InMemoryCheckpointStore<ShippingContext, ShippingState> store = new InMemoryCheckpointStore<>();
        InMemorySignalBroker broker = new InMemorySignalBroker();
        List<SignalMessage> publishedMessages = new CopyOnWriteArrayList<>();

        // Subscribe an audit tap to verify outbound messages published by orchestration steps
        broker.subscribe("warehouse-notifications", message -> {
            publishedMessages.add(message);
            return CompletableFuture.completedFuture(null);
        });

        OrchestrationStateMachineExecutor<ShippingContext, ShippingState, ShippingContext, String> executor =
                OrchestrationStateMachineBuilder.<ShippingContext, ShippingState, ShippingContext, String>create("ShippingWorkflow", ShippingState.class)
                .context(ShippingContext::new)
                .initialState(ShippingState.INITIALIZE)
                .correlationKey(ctx -> ctx.shipmentId)
                .checkpointStore(store)
                .input((ctx, input) -> {
                    if (input != null) {
                        ctx.shipmentId = input.shipmentId;
                    }
                    return ctx;
                })
                .state(ShippingState.INITIALIZE)
                    .action(ctx -> {
                        ctx.eventLog.add("INITIALIZE");
                        return ctx;
                    })
                    .transition(ShippingState.NOTIFY_WAREHOUSE)
                // Outbound publish to message broker destination "warehouse-notifications"
                .state(ShippingState.NOTIFY_WAREHOUSE)
                    .publish(broker, "warehouse-notifications", ctx -> {
                        ctx.warehouseNotified = true;
                        ctx.eventLog.add("NOTIFY_WAREHOUSE");
                        return new WarehouseNotificationCommand(ctx.shipmentId, "WH-NYC-01");
                    })
                    .transition(ShippingState.AWAIT_PICKED_SIGNAL)
                // Inbound signal 1 from broker
                .state(ShippingState.AWAIT_PICKED_SIGNAL)
                    .waitForCommand(PackagePickedCommand.class, (ctx, cmd) -> {
                        if (cmd != null) {
                            ctx.packagePicked = true;
                            ctx.warehouseId = cmd.warehouseId();
                            ctx.eventLog.add("PICKED_AT_" + cmd.warehouseId());
                        }
                        return ctx;
                    })
                    .transition(ShippingState.AWAIT_CARRIER_SIGNAL)
                // Inbound signal 2 from broker
                .state(ShippingState.AWAIT_CARRIER_SIGNAL)
                    .waitForCommand(CarrierDispatchedCommand.class, (ctx, cmd) -> {
                        if (cmd != null) {
                            ctx.carrierDispatched = true;
                            ctx.trackingNumber = cmd.trackingNumber();
                            ctx.eventLog.add("DISPATCHED_" + cmd.trackingNumber());
                        }
                        return ctx;
                    })
                    .transition(ShippingState.COMPLETED)
                .endStates(ShippingState.COMPLETED, ShippingState.FAILED)
                .output(ctx -> "Shipment " + ctx.shipmentId + " tracking: " + ctx.trackingNumber)
                .buildExecutor();

        // Connect the broker subscriber to the state machine via SignalReceiver
        SignalReceiver receiver = SignalReceiver.forExecutor(executor);
        broker.subscribe("warehouse-events", receiver);
        broker.subscribe("carrier-events", receiver);

        // 1. Dispatch initial turn: Runs INITIALIZE -> publishes to broker -> suspends at AWAIT_PICKED_SIGNAL
        ShippingContext input = new ShippingContext();
        input.shipmentId = "SHIP-8899";

        OrchestrationTurnResult<ShippingContext, ShippingState, String> turn1 = executor.dispatchTurnSync(null, input);
        assertTrue(turn1.isSuspended());
        assertEquals(ShippingState.AWAIT_PICKED_SIGNAL, turn1.currentStateKey());
        assertTrue(turn1.context().warehouseNotified);
        assertFalse(turn1.context().packagePicked);

        // Verify outbound notification message was published onto the broker
        assertEquals(1, publishedMessages.size());
        assertEquals("warehouse-notifications", publishedMessages.getFirst().destination());
        assertEquals("SHIP-8899", publishedMessages.getFirst().correlationKey());

        // 2. External system publishes PackagePicked event to "warehouse-events" topic
        PackagePickedCommand pickedCommand = new PackagePickedCommand("SHIP-8899", "WH-NYC-01");
        broker.publish("warehouse-events", pickedCommand).join();

        // Allow virtual thread to process
        Thread.sleep(100);

        // Verify checkpoint progressed to AWAIT_CARRIER_SIGNAL
        Optional<OrchestrationCheckpoint<ShippingContext, ShippingState>> cp2 = store.findByCorrelationKey("SHIP-8899");
        assertTrue(cp2.isPresent());
        assertEquals(ShippingState.AWAIT_CARRIER_SIGNAL, cp2.get().currentStateKey());
        assertTrue(cp2.get().contextSnapshot().packagePicked);

        // 3. Carrier system publishes CarrierDispatched event with idempotent CommandEnvelope to "carrier-events"
        CommandEnvelope<CarrierDispatchedCommand> carrierEnvelope = CommandEnvelope.of(
                UUID.randomUUID(),
                new CarrierDispatchedCommand("SHIP-8899", "TRACK-FDX-9900")
        );
        broker.publish("carrier-events", carrierEnvelope).join();

        // Allow virtual thread to process
        Thread.sleep(100);

        // Verify completed
        Optional<OrchestrationCheckpoint<ShippingContext, ShippingState>> cpFinal = store.findByCorrelationKey("SHIP-8899");
        assertTrue(cpFinal.isPresent());
        assertEquals(OrchestrationStatus.COMPLETED, cpFinal.get().status());
        assertTrue(cpFinal.get().contextSnapshot().carrierDispatched);
        assertEquals("TRACK-FDX-9900", cpFinal.get().contextSnapshot().trackingNumber);
        assertEquals(
                List.of("INITIALIZE", "NOTIFY_WAREHOUSE", "PICKED_AT_WH-NYC-01", "DISPATCHED_TRACK-FDX-9900"),
                cpFinal.get().contextSnapshot().eventLog
        );

        broker.close();
        executor.close();
    }

    /**
     * Tests simulated Kafka / SQS adapter passing raw broker headers, message IDs, and correlation keys via {@link SignalMessage}.
     */
    @Test
    public void testKafkaOrSqsAdapterSimulation() throws Exception {
        InMemoryCheckpointStore<ShippingContext, ShippingState> store = new InMemoryCheckpointStore<>();

        OrchestrationStateMachineExecutor<ShippingContext, ShippingState, ShippingContext, String> executor =
                OrchestrationStateMachineBuilder.<ShippingContext, ShippingState, ShippingContext, String>create("KafkaWorkflow", ShippingState.class)
                .context(ShippingContext::new)
                .initialState(ShippingState.AWAIT_PICKED_SIGNAL)
                .correlationKey(ctx -> ctx.shipmentId)
                .checkpointStore(store)
                .input((ctx, input) -> {
                    if (input != null) ctx.shipmentId = input.shipmentId;
                    return ctx;
                })
                .state(ShippingState.AWAIT_PICKED_SIGNAL)
                    .waitForCommand(PackagePickedCommand.class, (ctx, cmd) -> {
                        if (cmd != null) {
                            ctx.packagePicked = true;
                            ctx.warehouseId = cmd.warehouseId();
                        }
                        return ctx;
                    })
                    .transition(ShippingState.COMPLETED)
                .endStates(ShippingState.COMPLETED)
                .output(ctx -> "Done " + ctx.shipmentId)
                .buildExecutor();

        ShippingContext input = new ShippingContext();
        input.shipmentId = "KAFKA-ORD-1";

        OrchestrationTurnResult<ShippingContext, ShippingState, String> turn1 = executor.dispatchTurnSync(null, input);
        assertTrue(turn1.isSuspended());

        SignalReceiver receiver = SignalReceiver.forExecutor(executor);

        // Simulate a Kafka consumer record: partition header, trace parent, kafka offset
        SignalMessage kafkaMessage = new SignalMessage(
                "orders.fulfillment.v1",
                "PackagePickedCommand",
                "KAFKA-ORD-1",
                UUID.randomUUID(),
                Instant.now(),
                Map.of("kafka_offset", "10294", "traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
                new PackagePickedCommand("KAFKA-ORD-1", "WAREHOUSE-EU-NORTH")
        );

        CompletableFuture<OrchestrationTurnResult<?, ?, ?>> future = receiver.onMessage(kafkaMessage);
        OrchestrationTurnResult<?, ?, ?> result = future.get();

        assertNotNull(result);
        assertTrue(result.isCompleted());
        assertEquals("Done KAFKA-ORD-1", result.output());

        executor.close();
    }

    @Test
    public void testBrokerSubscriberFaultIsolation() {
        InMemorySignalBroker broker = new InMemorySignalBroker();
        List<String> receivedBySecond = new CopyOnWriteArrayList<>();
        List<String> receivedByGlobal = new CopyOnWriteArrayList<>();

        // Failing subscriber
        broker.subscribe("test-topic", _ -> {
            throw new RuntimeException("Simulated subscriber crash");
        });

        // Healthy subscriber on same topic
        broker.subscribe("test-topic", msg -> {
            receivedBySecond.add(msg.destination());
            return CompletableFuture.completedFuture(null);
        });

        // Healthy global subscriber
        broker.subscribeGlobal(msg -> {
            receivedByGlobal.add(msg.destination());
            return CompletableFuture.completedFuture(null);
        });

        SignalMessage msg = new SignalMessage("test-topic", "TEST_SIGNAL", null, "PAYLOAD");
        CompletableFuture<Void> future = broker.publish(msg);

        // Should complete exceptionally because of the failing subscriber
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, future::join);

        // But healthy topic and global subscribers still processed the message
        assertEquals(List.of("test-topic"), receivedBySecond);
        assertEquals(List.of("test-topic"), receivedByGlobal);

        broker.close();
    }

    @Test
    public void testBrokerUnsubscribeAndAutoCloseableRegistration() throws Exception {
        InMemorySignalBroker broker = new InMemorySignalBroker();
        List<String> received = new CopyOnWriteArrayList<>();

        SignalConsumer consumer = msg -> {
            received.add("topic:" + msg.destination());
            return CompletableFuture.completedFuture(null);
        };

        // 1. AutoCloseable registration on topic
        try (AutoCloseable sub = broker.register("topic-a", consumer)) {
            broker.publish(new SignalMessage("topic-a", "SIG", null, "1")).join();
            assertEquals(1, received.size());
        }

        // After close, consumer should not receive messages
        broker.publish(new SignalMessage("topic-a", "SIG", null, "2")).join();
        assertEquals(1, received.size());

        // 2. Manual unsubscribe on topic
        broker.subscribe("topic-b", consumer);
        broker.publish(new SignalMessage("topic-b", "SIG", null, "3")).join();
        assertEquals(2, received.size());

        boolean removed = broker.unsubscribe("topic-b", consumer);
        assertTrue(removed);

        broker.publish(new SignalMessage("topic-b", "SIG", null, "4")).join();
        assertEquals(2, received.size());

        // 3. Global registration and close
        List<String> globalReceived = new CopyOnWriteArrayList<>();
        SignalConsumer globalConsumer = msg -> {
            globalReceived.add("global:" + msg.destination());
            return CompletableFuture.completedFuture(null);
        };

        try (AutoCloseable sub = broker.registerGlobal(globalConsumer)) {
            broker.publish(new SignalMessage("topic-x", "SIG", null, "5")).join();
            assertEquals(1, globalReceived.size());
        }

        broker.publish(new SignalMessage("topic-y", "SIG", null, "6")).join();
        assertEquals(1, globalReceived.size());

        broker.close();
    }
}


