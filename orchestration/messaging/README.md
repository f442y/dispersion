# Dispersion Orchestration Messaging Module (`dispersion-orchestration-messaging`)

The **Dispersion Orchestration Messaging Module** provides a broker-agnostic messaging and signal dispatching subsystem engineered for **Java 25+ Virtual Threads**.

---

## 1. Capabilities & Abstractions

* **Broker Abstraction**: Publish-subscribe interface decoupling orchestration signals from physical brokers (Kafka, RabbitMQ, SQS).
* **In-Memory Reference Implementation**: `InMemorySignalBroker` delivers virtual-thread backed pub/sub messaging with subscriber fault isolation, destination routing, and dynamic registration.
* **Signal Ingestion Receiver**: `SignalReceiver` deserializes inbound `BrokerMessage` envelopes and dispatches signals into registered `OrchestrationStateMachineExecutor` instances.

---

## 2. Example Usage

```java
import com.github.f442y.dispersion.orchestration.messaging.broker.BrokerMessage;
import com.github.f442y.dispersion.orchestration.messaging.broker.InMemorySignalBroker;
import com.github.f442y.dispersion.orchestration.messaging.broker.SignalReceiver;

try (InMemorySignalBroker broker = new InMemorySignalBroker()) {
    // 1. Subscribe to external topic
    broker.subscribe("order-signals", (BrokerMessage msg) -> {
        System.out.println("Received signal: " + msg.signalName() + " for key: " + msg.correlationKey());
    });

    // 2. Publish message
    broker.publish("order-signals", new BrokerMessage(
        UUID.randomUUID().toString(),
        "PaymentApprovedSignal",
        "ORD-9021",
        "order-signals",
        Instant.now(),
        new byte[0]
    ));
}
```
