# Architecture & Hexagonal Design

Dispersion is a high-throughput, zero-synchronization finite state machine and distributed Saga orchestration engine engineered specifically for **Java 25+ Virtual Threads** (Project Loom).

Traditional workflow engines (BPMN frameworks, distributed orchestrators, reflection-heavy state libraries) incur severe performance and operational penalties:
* **Thread Contention:** Fixed OS thread pools choke under burst loads or blocked I/O.
* **Database Ping-Pong:** Persisting state to relational databases on every micro-transition degrades throughput to hundreds of ops/sec.
* **Synchronization Bottlenecks:** Pervasive locking (`synchronized`, `ReentrantLock`, `AtomicReference`) triggers cache thrashing and memory barriers across CPU cores.
* **Tight Architectural Coupling:** Monolithic designs interlock observability, persistence, and business execution.

Dispersion resolves these challenges through a unified design rooted in **thread confinement**, **pre-compiled ordinal array state graphs**, **hexagonal SPI boundaries**, and a **multi-tiered execution model**.

---

## 1. Hexagonal Decoupling & Module Symmetrical Topology

Dispersion enforces strict architectural separation using the **Ports-and-Adapters (Hexagonal)** pattern across 16 Maven modules:

```mermaid
graph TD
    subgraph BOM["dispersion-bom"]
        BOM_DEP["Centralized Dependency Version Alignment"]
    end

    subgraph Event["1. Event Subsystem"]
        E_API["dispersion-event-api<br/>(Ports & Sealed Events)"]
        E_CORE["dispersion-event-core<br/>(Virtual Thread Bus)"]
        E_TEST["dispersion-event-test<br/>(Recording Doubles)"]
        E_CORE --> E_API
        E_TEST --> E_API
    end

    subgraph FSM["2. FSM Subsystem (Tier 1)"]
        F_API["dispersion-fsm-api<br/>(StateKey, Context, Actions)"]
        F_CORE["dispersion-fsm-core<br/>(Atomic Execution Engine)"]
        F_TEST["dispersion-fsm-test<br/>(Test Context Doubles)"]
        F_API --> E_API
        F_CORE --> F_API
        F_CORE --> E_API
        F_TEST --> F_API
    end

    subgraph Orch["3. Orchestration Subsystem (Tier 2/3)"]
        O_API["dispersion-orchestration-api<br/>(Sagas, Checkpoints, Signals)"]
        O_CORE["dispersion-orchestration-core<br/>(Turn Driver & Batch Engine)"]
        O_TEST["dispersion-orchestration-test<br/>(Fake Signal Broker & Store)"]
        O_API --> F_API
        O_API --> E_API
        O_CORE --> O_API
        O_CORE --> F_CORE
        O_TEST --> O_API
    end

    subgraph Control["4. Control Subsystem (Observability)"]
        C_API["dispersion-control-api<br/>(InspectableMachine SPI)"]
        C_CORE["dispersion-control-core<br/>(Registry & Signal Router)"]
        C_TEST["dispersion-control-test<br/>(Fake Inspectable Machine)"]
        C_API --> E_API
        C_CORE --> C_API
        C_TEST --> C_API
    end

    subgraph Testing["5. Testing Facade"]
        TESTING["dispersion-testing<br/>(DispersionTestKit)"]
        TESTING --> E_TEST & F_TEST & O_TEST & C_TEST
        TESTING --> E_API & F_API & O_API & C_API
    end
```

### Decoupling Rules & Architectural Invariants

1. **`*-api` Modules Are Pure Contracts:**
   Contain only interfaces, sealed records, and domain exceptions. They depend exclusively on standard Java and JSpecify annotations—never on third-party frameworks or runtime engines.
2. **`*-core` Modules Contain Runtimes:**
   Implement the corresponding API. A core module **never depends on other core modules** unless architecturally hierarchical (`orchestration-core` relies on `fsm-core` to embed atomic child machines).
3. **`*-test` Modules Are Zero-Dependency Doubles:**
   Contain deterministic fakes and recorders. A test companion module **never depends on `*-core`**, ensuring test doubles cannot accidentally rely on engine internals.
4. **Control Plane Inversion via `InspectableMachine`:**
   `dispersion-control-core` has **zero compile-time dependencies** on `fsm-core` or `orchestration-core`. Executors implement the `InspectableMachine` SPI and adapt themselves via `.asInspectableMachine()`, allowing the control plane to observe any engine generically.

---

## 2. The Multi-Tier Execution Model

Dispersion partitions state machine execution into three complementary tiers:

```mermaid
graph TB
    subgraph Tier1["Tier 1: Atomic State Machine (dispersion-fsm-core)"]
        direction LR
        A_IN["Input"] --> A_S1["Validate"] --> A_S2["Compute"] --> A_S3["Transform"] --> A_OUT["Output"]
    end

    subgraph Tier2["Tier 2: Macro Saga Orchestration (dispersion-orchestration-core)"]
        direction TB
        O_START(["Start Turn 1"]) --> O_STEP1["Reserve Inventory (Compensable)"]
        O_STEP1 --> O_EMBED["Step 2: Run Tier 1 Machine"]
        O_EMBED --> O_SUSPEND{"Step 3: Wait For External Signal"}
        O_SUSPEND -.->|"Persist Checkpoint & Free Thread"| CP[("CheckpointStore")]
        CP -.->|"Inbound Signal Correlated"| O_RESUME(["Start Turn 2"])
        O_RESUME --> O_STEP4["Settle Payment (Compensable)"]
        O_STEP4 --> O_END(["Complete Workflow"])

        O_STEP4 -.->|"Payment Error"| O_ROLLBACK["Automated LIFO Saga Rollback"]
    end

    subgraph Tier3["Tier 3: Turn-Based Batch Processing (dispersion-orchestration-core)"]
        direction LR
        B_ITEMS["Batch Items [1..N]"] --> B_BARRIER{"Barrier Policy: ALL_ITEMS | QUORUM"}
        B_BARRIER --> B_ADVANCE["Advance to Next Batch Step"]
    end

    O_EMBED -.->|"Executes In-Memory"| Tier1
```

### Architectural Dimension Matrix

| Dimension | Tier 1: Atomic (Micro) Machine | Tier 2: Orchestration (Macro) Saga | Tier 3: Turn-Based Batching |
| :--- | :--- | :--- | :--- |
| **Primary Module** | `dispersion-fsm-core` | `dispersion-orchestration-core` | `dispersion-orchestration-core` |
| **Execution Latency** | Sub-microsecond (< 1 μs) | Turn-based (< 50 μs per in-memory step) | Parallel items with barrier sync |
| **Thread Model** | Single Virtual Thread (confined) | Virtual Thread per turn | Virtual Thread per batch item |
| **State Mutability** | Direct POJO mutations (lock-free) | Checkpoint snapshots at turn boundaries | Item-level isolated context |
| **Lifecycle** | Ephemeral, in-memory only | Long-lived, suspendable (`waitForCommand`) | Batch-synchronized turns |
| **Failure Recovery** | Fast-fail terminal diversion | **Automated LIFO Saga rollbacks** | Item-level error isolation |
| **Concurrency** | Single-threaded sequential graph | Parallel fork-join branches (`.parallel()`) | Concurrent item processing |
| **Ideal Use Cases** | Protocol parsing, trading validation, rules | Distributed sagas, multi-service transactions | Bulk ingestion, payroll runs, order batches |

---

## 3. Concurrency Guarantees & Thread Confinement

Traditional multi-threaded frameworks protect shared state through synchronization primitives:
* Mutexes (`synchronized`, `ReentrantLock`)
* Atomic wrappers (`AtomicReference`, `AtomicInteger`)
* Concurrent collections (`ConcurrentHashMap`)

These primitives introduce CPU cache-line bouncing, memory bus lock signals, and context-switch latencies.

Dispersion relies instead on **Thread Confinement**:

```
                       ┌─────────────────────────────────────────────────────────┐
                       │                   Virtual Thread #42                    │
                       │                                                         │
  Inbound Request ────►│  ┌───────────────────────────────────────────────────┐  │
                       │  │      Domain Context (Mutable Lock-Free POJO)      │  │
                       │  │       • orderId = "ORD-101"                       │  │
                       │  │       • balance = 450.00                          │  │
                       │  │       • status  = VALIDATED                       │  │
                       │  └───────────────────────────────────────────────────┘  │
                       │         │                     ▲                         │
                       │         ▼                     │                         │
                       │  ┌──────────────┐      ┌──────────────┐                 │
                       │  │   Action 1   │─────►│   Action 2   │                 │
                       │  └──────────────┘      └──────────────┘                 │
                       └─────────────────────────────────────────────────────────┘
                                   Zero Locks • Zero Volatiles
```

1. **Confined Mutability:** For the duration of a turn, the `StateMachineContext` is accessed exclusively by a single Virtual Thread. Actions mutate context fields directly.
2. **Deterministic Checkpoint Snapshots:** When an orchestration machine suspends at `.waitForCommand(...)`, Dispersion captures a deep/serialized snapshot into an `OrchestrationCheckpoint`. This snapshot is saved to `CheckpointStore`, and the virtual thread terminates.
3. **Structured Parallelism:** When executing `.parallel()` branches, Dispersion creates distinct child contexts for each concurrent branch. The parent virtual thread joins child threads deterministically before reconciling state back into the primary context.

---

## 4. JPMS Module Isolation & Zero Split-Packages

Dispersion is built strictly for the Java Platform Module System (JPMS). Every module owns an isolated package namespace, guaranteeing **zero split-package collisions**:

| Module Artifact | JPMS Automatic Module Name | Package Namespace Root |
| :--- | :--- | :--- |
| `dispersion-bom` | `com.github.f442y.dispersion.bom` | *N/A (BOM POM)* |
| `dispersion-event-api` | `com.github.f442y.dispersion.event.api` | `com.github.f442y.dispersion.event` |
| `dispersion-event-core` | `com.github.f442y.dispersion.event.core` | `com.github.f442y.dispersion.event.bus`, `com.github.f442y.dispersion.event.dispatcher` |
| `dispersion-event-test` | `com.github.f442y.dispersion.event.test` | `com.github.f442y.dispersion.event.test` |
| `dispersion-fsm-api` | `com.github.f442y.dispersion.fsm.api` | `com.github.f442y.dispersion.fsm` |
| `dispersion-fsm-core` | `com.github.f442y.dispersion.fsm.core` | `com.github.f442y.dispersion.fsm.core` |
| `dispersion-fsm-test` | `com.github.f442y.dispersion.fsm.test` | `com.github.f442y.dispersion.fsm.test` |
| `dispersion-orchestration-api` | `com.github.f442y.dispersion.orchestration.api` | `com.github.f442y.dispersion.orchestration` |
| `dispersion-orchestration-core` | `com.github.f442y.dispersion.orchestration.core` | `com.github.f442y.dispersion.orchestration.core` |
| `dispersion-orchestration-test` | `com.github.f442y.dispersion.orchestration.test` | `com.github.f442y.dispersion.orchestration.test` |
| `dispersion-control-api` | `com.github.f442y.dispersion.control.api` | `com.github.f442y.dispersion.control` |
| `dispersion-control-core` | `com.github.f442y.dispersion.control.core` | `com.github.f442y.dispersion.control.core` |
| `dispersion-control-test` | `com.github.f442y.dispersion.control.test` | `com.github.f442y.dispersion.control.test` |
| `dispersion-testing` | `com.github.f442y.dispersion.testing` | `com.github.f442y.dispersion.testing` |
| `dispersion-examples` | `com.github.f442y.dispersion.examples` | `com.github.f442y.dispersion.examples` |

---

## Next Architectural Deep Dives

* ⚡ [**Virtual Threads & Performance Guide**](virtual-threads-and-performance.md) — Carrier thread scheduling, unmounting mechanics, zero-pinning guarantees, and JVM escape analysis.
* 🔄 [**Saga Orchestration & Batch Processing**](saga-orchestration-and-batching.md) — Turn-based lifecycle, LIFO compensation unwind, and batch barrier policies.
* 🔭 [**Observability & Control Plane**](observability-and-control-plane.md) — Telemetry streaming, $O(1)$ dual-pool memory topology, and React UI integration.
