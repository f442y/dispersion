# Dispersion 🌀

[![Java 25](https://img.shields.io/badge/Java-25+-orange.svg?style=flat-square&logo=openjdk)](https://openjdk.org/projects/jdk/25/)
[![Virtual Threads](https://img.shields.io/badge/Virtual%20Threads-Project%20Loom-blue.svg?style=flat-square)](https://openjdk.org/jeps/444)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)
[![CI Build](https://img.shields.io/badge/Build-Passing-brightgreen.svg?style=flat-square&logo=githubactions)](https://github.com/f442y/dispersion/actions)
[![Null Safety: JSpecify](https://img.shields.io/badge/Null%20Safety-JSpecify-purple.svg?style=flat-square)](https://jspecify.dev/)

> **High-Throughput Finite State Machine and Distributed Saga Orchestration Engine natively engineered for Java 25+ Virtual Threads.**

---

## 🌟 Overview

**Dispersion** is a lightweight, zero-synchronization workflow and state machine engine engineered from the ground up for modern Java. By uniting **Java 25 Virtual Threads** (`Thread.ofVirtual()`), sealed type hierarchies, records, and pattern matching, Dispersion enables hundreds of thousands of concurrent state machine workflows with sub-millisecond dispatch times, minimal memory footprint, and zero thread-pool exhaustion.

Unlike heavyweight workflow orchestrators that require external database daemons or complex reflection proxies, Dispersion gives you two purpose-built tiers:

1. **Tier 1: Atomic (Micro) FSMs**: Thread-confined, zero-synchronization state pipelines executing on single virtual threads.
2. **Tier 2: Orchestration (Macro) Sagas**: Turn-based, durable, suspendable workflows with pluggable checkpoint persistence, automated LIFO Saga rollbacks, concurrent parallel branches, and broker-agnostic messaging.

```mermaid
graph TD
    subgraph Macro["Tier 2: Macro Orchestration State Machine (Durable / Turn-Based)"]
        START([Start Turn]) --> V1[Validate Order]
        V1 --> PARALLEL{Fork-Join Parallel}

        subgraph "Virtual Thread Concurrent Branches"
            PARALLEL --> B1[Reserve Inventory]
            PARALLEL --> B2[Fraud Analysis]
            PARALLEL --> B3[Calculate Tax]
        end

        B1 & B2 & B3 --> JOIN{Join}
        JOIN --> CHILD_FSM[Atomic Micro-FSM]

        subgraph Micro["Tier 1: Atomic Micro-FSM (Thread-Confined)"]
            CHILD_FSM --> A1[Tokenize Card] --> A2[Authorize] --> A3[Capture]
        end

        CHILD_FSM --> PUB[Publish Outbound Request]
        PUB --> WAIT_SIG{Wait for Inbound Signal}
        WAIT_SIG -.->|Suspend & Persist| STORE[(Checkpoint Store)]
        STORE -.->|Signal Arrives: Rehydrate| FULFILL[Fulfill Order]

        FULFILL -->|Success| ORCH_END([Completed])
        FULFILL -->|Failure| SAGA[Automated LIFO Saga Rollback]
    end
```

---

## 🏛️ Two-Tiered Model: At a Glance

| Dimension | Tier 1: Atomic (Micro) State Machine | Tier 2: Orchestration (Macro) State Machine |
| :--- | :--- | :--- |
| **Execution Scope** | Single virtual thread, thread-confined | Turn-based, asynchronous, durable coordinator |
| **State Mutations** | Direct, zero-synchronization context POJO | Checkpointed snapshots & persistent rehydration |
| **Concurrency & Lifecycle** | High-throughput sequential graph steps | Long-lived, suspendable via external signals (`waitForSignal`), parallel fork-join, **batch streaming & barriers** |
| **Persistence** | In-memory only | Pluggable `CheckpointStore` (SQL, Key-Value, In-Memory) |
| **Messaging & Transport** | In-memory only | **Broker-Agnostic SPI** (Kafka, RabbitMQ, SQS, Redis, In-Memory) |
| **Failure Recovery** | Fast-fail, cycle loop-breakers, fallback states | Virtual-thread retries + **Automated LIFO Saga Rollbacks** across turns |
| **Primary Use Cases** | Low-latency state parsing, protocol decoding, single-unit business rules | Distributed transactions, multi-service Sagas, async approval workflows, batch item pipelines |

---

## ⚡ 60-Second Quick Start

### 1. Build an Atomic (Micro) State Machine
```java
import com.github.f442y.dispersion.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.context.StateMachineContext;
import com.github.f442y.dispersion.state.StateKey;

// Define states and context
enum CoffeeState implements StateKey { GRIND, BREW, SERVED }
class CoffeeContext implements StateMachineContext { String type; String cup; }

// Build and execute on Virtual Threads
try (var executor = AtomicStateMachineBuilder.<CoffeeContext, CoffeeState, String, String>create(CoffeeState.class)
        .context(CoffeeContext::new)
        .initialState(CoffeeState.GRIND)
        .input((ctx, type) -> { ctx.type = type; return ctx; })
        .state(CoffeeState.GRIND)
            .action(ctx -> { System.out.println("Grinding " + ctx.type); return ctx; })
            .transition(CoffeeState.BREW)
        .state(CoffeeState.BREW)
            .action(ctx -> { ctx.cup = "Hot cup of " + ctx.type + "!"; return ctx; })
            .transition(CoffeeState.SERVED)
        .endStates(CoffeeState.SERVED)
        .output(ctx -> ctx.cup)
        .buildExecutor("coffee-machine")) {

    String coffee = executor.dispatchSync("Espresso Roast");
    System.out.println(coffee); // Hot cup of Espresso Roast!
}
```

### 2. Build a Suspendable Orchestration Saga with Rollback
```java
import com.github.f442y.dispersion.orchestration.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineBuilder;
import com.github.f442y.dispersion.orchestration.OrchestrationStateMachineExecutor;

enum OrderState implements StateKey { RESERVE_STOCK, WAIT_PAYMENT, COMPLETE }
class OrderContext implements StateMachineContext { String orderId; boolean reserved; }
record PaymentSignal(String orderId, boolean paid) {}

try (var executor = OrchestrationStateMachineBuilder.<OrderContext, OrderState, String, String>create("OrderSaga", OrderState.class)
        .context(OrderContext::new)
        .initialState(OrderState.RESERVE_STOCK)
        .checkpointStore(new InMemoryCheckpointStore<>())
        .correlationKey(ctx -> ctx.orderId)
        .input((ctx, id) -> { ctx.orderId = id; return ctx; })
        .state(OrderState.RESERVE_STOCK)
            .action(ctx -> { ctx.reserved = true; return ctx; })
            .compensate(ctx -> { ctx.reserved = false; System.out.println("Compensated stock!"); return ctx; })
            .transition(OrderState.WAIT_PAYMENT)
        .state(OrderState.WAIT_PAYMENT)
            .waitForSignal("PaymentReceived", PaymentSignal.class, (ctx, sig) -> ctx)
            .transition(OrderState.COMPLETE)
        .endStates(OrderState.COMPLETE)
        .output(ctx -> "Order " + ctx.orderId + " completed!")
        .buildExecutor()) {

    // Turn 1: Runs to WAIT_PAYMENT, snapshots state, and suspends (frees virtual thread)
    var turn1 = executor.dispatchTurnSync(null, "ORD-101");
    System.out.println("Suspended? " + turn1.isSuspended()); // true

    // Turn 2: Webhook signal arrives hours later -> resumes and completes
    var turn2 = executor.sendSignalByCorrelationKey("ORD-101", "PaymentReceived", new PaymentSignal("ORD-101", true)).get();
    System.out.println("Result: " + turn2.output()); // Order ORD-101 completed!
}
```

---

## 📚 Complete Documentation Guides

Explore our modular documentation guides for detailed explanations, patterns, and reference architectures:

| Guide | Description |
| :--- | :--- |
| 🏗️ **[Architecture & Core Concepts](docs/architecture-and-concepts.md)** | Deep dive into the two-tier model, thread confinement, memory safety, and fundamental building blocks (`StateKey`, `StateMachineContext`, `Action`, `Transition`, `SagaCommand`). |
| ⚡ **[Tier 1: Atomic (Micro) State Machines](docs/tier-1-atomic-machines.md)** | High-throughput micro-pipelines, dynamic conditional branching (`transitionsTo`), state visit limits (`maxVisits`), global circuit breakers (`maxTransitions`), and backpressure (`AdmissionController`). |
| 🔄 **[Tier 2: Orchestration & Distributed Sagas](docs/tier-2-orchestration-sagas.md)** | Turn-based execution, suspension & rehydration (`waitForSignal`), automated LIFO Saga rollbacks, concurrent parallel fork-join branches with fast-fail compensation, and embedded child machines with retries. |
| 📦 **[Distributed Messaging & Batch Orchestration](docs/distributed-messaging-and-batching.md)** | Network deduplication with `CommandEnvelope`, broker-agnostic messaging SPI (Kafka, SQS, RabbitMQ, In-Memory), and batch collections with dynamic synchronization barriers (`BarrierPolicy`). |
| 🔭 **[Observability & Core Control Plane](docs/observability-and-control-plane.md)** | Real-time `ExecutionEventListener` SPI, 12 sealed telemetry records, `AsyncExecutionEventDispatcher`, and the in-memory `DefaultControlPlane` (topology discovery, live summaries, timeline replay, signal routing, and React TanStack Router UI integration). |
| ☕ **[Java 25+ Language Features in Action](docs/java-25-features.md)** | Virtual Threads (Project Loom), compile-time exhaustive switch matching over sealed exceptions and events, nested record pattern deconstruction, and JSpecify null safety. |

---

## 📦 Installation & Dependency Management

Dispersion publishes a centralized Bill of Materials (**BOM**) for streamlined dependency management.

### Maven BOM Setup

Add the BOM to your root `pom.xml`:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.github.f442y.dispersion</groupId>
            <artifactId>dispersion-bom</artifactId>
            <version>DEVELOP-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

### Module Dependencies

Add the required modules to your application:

```xml
<dependencies>
    <!-- Core runtime engine (Virtual Threads, Executors, Builders, Sagas, Control Plane) -->
    <dependency>
        <groupId>com.github.f442y.dispersion</groupId>
        <artifactId>dispersion-core</artifactId>
    </dependency>

    <!-- (Optional) API interfaces only (for domain / contract modules) -->
    <dependency>
        <groupId>com.github.f442y.dispersion</groupId>
        <artifactId>dispersion-api</artifactId>
    </dependency>
</dependencies>
```

### Gradle Setup

```groovy
// build.gradle
dependencies {
    implementation platform('com.github.f442y.dispersion:dispersion-bom:DEVELOP-SNAPSHOT')
    implementation 'com.github.f442y.dispersion:dispersion-core'
}
```

---

## ☕ Java Platform Module System (JPMS) Support

All Dispersion JARs declare automatic module names in their manifests:

| Maven Module | JAR Artifact | JPMS Automatic Module Name | Primary Role |
| :--- | :--- | :--- | :--- |
| `dispersion-api` | `dispersion-api.jar` | `com.github.f442y.dispersion.api` | Contracts, sealed exceptions, records, SPIs (Zero runtime dependencies) |
| `dispersion-core` | `dispersion-core.jar` | `com.github.f442y.dispersion.core` | Virtual-thread execution runtime, builders, sagas, messaging, control plane |
| `dispersion-examples` | `dispersion-examples.jar` | `com.github.f442y.dispersion.examples` | Reference architectures, distributed sagas, high-throughput pipelines |

In your `module-info.java`:

```java
module com.example.myapp {
    requires com.github.f442y.dispersion.core;
}
```

---

## 📁 Module Structure

```
dispersion/
├── docs/                        # Complete in-depth architectural guides & documentation
│   ├── architecture-and-concepts.md
│   ├── tier-1-atomic-machines.md
│   ├── tier-2-orchestration-sagas.md
│   ├── distributed-messaging-and-batching.md
│   ├── observability-and-control-plane.md
│   └── java-25-features.md
├── dispersion-bom/              # Centralized Bill of Materials POM
├── dispersion-api/              # Interfaces, Sealed Exceptions, Records, SPIs
├── dispersion-core/             # Virtual-Thread Executors, Builders, Sagas, Control Plane
└── dispersion-examples/         # Real-world Distributed Saga Reference Implementations
```

---

## 🛠️ Building & Testing

### Prerequisites
- **JDK 25+** (e.g. OpenJDK 25 / Azul Zulu 25 / Liberica JDK 25)
- **Maven 3.9+** (or use the included `./mvnw`)

### Build & Run Complete Test Matrix

```bash
# Build all modules and verify packages
./mvnw clean verify

# Run complete multi-module test suite
./mvnw clean test
```

---

## 📄 License

Dispersion is open-source software licensed under the [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0).
