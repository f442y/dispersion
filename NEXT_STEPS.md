# Dispersion — Roadmap & Next Steps Plan

> **Last Updated:** September 14, 2026
> **Current Git State:** `main` branch clean, all tests passing (21 reactor modules, 38/38 tests passing across unit and integration suites).
> **Overall Goal:** Connect the Dispersion distributed state engine to a modern React + TanStack Router Control Panel Web UI for real-time inspection, monitoring, and operator control.

---

## 📍 Where We Are Today

### ✅ Completed Milestones

1. **Multi-Module Decomposition & Reorganization (Hexagonal Boundaries & Nested Domains)**
   - Decomposed monolithic modules into topic-nested directories with zero split-package collisions across 21 modules:
     - `event/api` (`dispersion-event-api`), `event/core` (`dispersion-event-core`), `event/test` (`dispersion-event-test`): Telemetry event hierarchy and high-throughput virtual thread ring buffer dispatcher.
     - `fsm/api` (`dispersion-fsm-api`), `fsm/core` (`dispersion-fsm-core`), `fsm/test` (`dispersion-fsm-test`): Atomic FSM contracts, builders, virtual thread runner, and token-bucket admission control.
     - `routing/api` (`dispersion-routing-api`), `routing/core` (`dispersion-routing-core`), `routing/test` (`dispersion-routing-test`): Location-agnostic workload routing, Canary traffic control, and developer sandboxes.
     - `orchestration/api` (`dispersion-orchestration-api`), `orchestration/core` (`dispersion-orchestration-core`), `orchestration/batch` (`dispersion-orchestration-batch`), `orchestration/messaging` (`dispersion-orchestration-messaging`), `orchestration/test` (`dispersion-orchestration-test`): Macro sagas, automated LIFO rollbacks, turn-based batch barriers, broker-agnostic messaging, and test doubles.
     - `control/api` (`dispersion-control-api`), `control/core` (`dispersion-control-core`), `control/test` (`dispersion-control-test`): Control plane query SPI, registry, and topology discovery.
     - `testing` (`dispersion-testing`): Unified static testing facade.
     - `bom` (`dispersion-bom`): Centralized dependency management.
     - `examples` (`dispersion-examples`): High-throughput virtual-thread pipeline bursts (500 threads) and multi-step distributed Saga rollbacks.

2. **Tier 1: Atomic Finite State Machine Engine (`dispersion-fsm-core`)**
   - Pure thread-confined, lock-free execution model over Java 25 virtual threads.
   - Dynamic conditional transition evaluation (`transitionsTo`).
   - State visit loop detection & fallback thresholds (`maxVisits`).
   - Fast-fail circuit breaker protection (`circuitBreaker`).
   - Direct execution pathway (`executeDirect`) with zero thread allocations when invoked within virtual thread contexts.

3. **Tier 2: Orchestration & Distributed Saga Engine (`dispersion-orchestration-core`)**
   - Turn-based discrete execution lifecycle with thread confinement.
   - Safe execution suspension (`waitForSignal`) and resume on signal delivery.
   - Automated LIFO Saga compensation rollback upon unhandled errors or operator cancellation.
   - Parallel concurrent fork-join execution with fast-fail cancellation and interrupt handling.
   - Child machine composition with automatic retry, recovery, and result mapping.
   - Network idempotency deduplication (`CommandEnvelope`).

4. **Tier 3: Turn-Based Batch Processing (`dispersion-orchestration-batch`)**
   - Item-level virtual thread concurrency with isolated context state.
   - Synchronized stage progression via `ALL_ITEMS` and `QUORUM` barrier policies.
   - Granular item checkpoints for safe workflow resumption.

5. **Observability & Control Plane Subsystem (`dispersion-control-api` & `dispersion-control-core`)**
   - Clean, extensible telemetry event hierarchy in [`ExecutionEvent`](file:///C:/Users/faiza/development/dispersion/event/api/src/main/java/com/github/f442y/dispersion/event/ExecutionEvent.java).
   - Thread-safe functional listener contract [`ExecutionEventListener`](file:///C:/Users/faiza/development/dispersion/event/api/src/main/java/com/github/f442y/dispersion/event/ExecutionEventListener.java).
   - High-throughput, bounded, lock-free ring buffer dispatcher running on dedicated virtual threads.
   - Unified operator control SPI ([`ControlPlane`](file:///C:/Users/faiza/development/dispersion/control/api/src/main/java/com/github/f442y/dispersion/control/ControlPlane.java)) with thread-safe in-memory reference implementation ([`DefaultControlPlane`](file:///C:/Users/faiza/development/dispersion/control/core/src/main/java/com/github/f442y/dispersion/control/core/DefaultControlPlane.java)).
   - Dynamic Mermaid diagram generation and inspection.

---

## 🎯 Next Steps & Phased Execution Roadmap

```mermaid
flowchart TD
    subgraph Done["✅ Phase 1: Core Telemetry, SPI & Decomposition (Complete)"]
        DEC["Multi-Module Decomposition (21 Modules)"]
        CP["ControlPlane SPI"]
        EE["ExecutionEvent Records"]
        DISP["AsyncEventDispatcher"]
        ROUT["Workload Routing & Sandboxes"]
    end

    subgraph Next["🚀 Phase 2: Serialization & API Gateway (Next Up)"]
        SER["JSON Serialization (Records / Polymorphic Events)"]
        EP["HTTP / SSE / WebSocket Gateway Module"]
        CLI["Lightweight Embedded Server (JDK HTTP / Javalin)"]
    end

    subgraph UI["🖥️ Phase 3: Control Panel Web UI (React + TanStack)"]
        ROUTER["TanStack Router (File-based routing)"]
        QUERY["TanStack Query (Cache & Live SSE sync)"]
        GRAPH["Interactive Graph Canvas (React Flow DAG)"]
        CTRL["Operator Controls (Signal Injection & Compensation Triggers)"]
    end

    subgraph Dist["🌐 Phase 4: Cluster & Distributed Stores"]
        KAFKA["Distributed Messaging Adapter (Kafka/RabbitMQ)"]
        REDIS["Persistent Checkpoint Store (Redis/Postgres)"]
    end

    Done --> Next
    Next --> UI
    UI --> Dist
```

---

### 🚀 Phase 2: API Gateway & Serialization (Backend Bridge)
> **Constraint Reminder:** *Keep core modules clean and independent of heavy web frameworks.*
> Follow the hybrid layout (e.g. `serialization/json` with `artifactId`: `dispersion-serialization-json`, and `server/http` with `artifactId`: `dispersion-server-http`).

- [ ] **Step 2.1 — Event & Descriptor JSON Serialization (`serialization/json` / `dispersion-serialization-json`)**
  - Implement zero-dependency or lightweight Jackson / standard JSON serializers for:
    - All [`ExecutionEvent`](file:///C:/Users/faiza/development/dispersion/event/api/src/main/java/com/github/f442y/dispersion/event/ExecutionEvent.java) records using polymorphic type discrimination (`@type` or `eventType`).
    - [`MachineDescriptor`](file:///C:/Users/faiza/development/dispersion/control/api/src/main/java/com/github/f442y/dispersion/control/MachineDescriptor.java) (topology metadata: states, transitions, initial/terminal states).
    - [`ExecutionSummary`](file:///C:/Users/faiza/development/dispersion/control/api/src/main/java/com/github/f442y/dispersion/control/ExecutionSummary.java) and [`SignalDeliveryResult`](file:///C:/Users/faiza/development/dispersion/control/api/src/main/java/com/github/f442y/dispersion/control/SignalDeliveryResult.java).
- [ ] **Step 2.2 — Pluggable Transport Server Module (`server/http` / `dispersion-server-http`)**
  - Create a new Maven sub-module: `server/http`.
  - Use Java 25 virtual-thread-native HTTP/WebSocket server (e.g., lightweight JDK `HttpServer` with virtual thread executor, or Javalin).
  - Implement REST endpoints:
    - `GET /api/v1/machines`: List all registered state machines.
    - `GET /api/v1/machines/:name`: Fetch topology descriptor and state graph schema.
    - `GET /api/v1/executions`: List active and completed executions with pagination.
    - `GET /api/v1/executions/:id`: Fetch detailed execution state, history, and current status.
    - `POST /api/v1/executions/:id/signal`: Deliver external signal payload to suspended workflows.
  - Implement Real-Time Event Streaming:
    - `GET /api/v1/events/stream`: Server-Sent Events (SSE) push for live `ExecutionEvent` streams, filtered by machine name or correlation key.

---

### 🖥️ Phase 3: Control Panel Web UI (React + TanStack Router)
> **Stack:** React 19, TypeScript, TanStack Router, TanStack Query, Tailwind CSS, React Flow / @xyflow/react.

- [ ] **Step 3.1 — UI Project Scaffolding**
  - Initialize `dispersion-ui` (Vite + React + TypeScript + TanStack Router).
  - Configure TanStack Router file-based routes:
    - `routes/__root.tsx`: App layout, global header, connection status badge (SSE indicator).
    - `routes/index.tsx`: Dashboard with running machine counts, throughput counters, and recent activity.
    - `routes/machines/index.tsx`: Catalog of state machines (Atomic vs. Orchestration sagas).
    - `routes/machines/$machineName.tsx`: Machine details, DAG visualization, state transition matrix.
    - `routes/executions/index.tsx`: Searchable execution table (filter by status: `RUNNING`, `SUSPENDED`, `COMPLETED`, `COMPENSATED`).
    - `routes/executions/$executionId.tsx`: Execution inspector with real-time state timeline, turn durations, and signal injection modal.
- [ ] **Step 3.2 — Interactive DAG Graph Visualization**
  - Render machine topology using React Flow:
    - Nodes styled according to state type (`initial`, `transient`, `terminal`, `suspendable`).
    - Animate active transitions in real-time when `StateEnteredEvent` / `TransitionEvaluatedEvent` arrives.
    - Visually highlight compensated states in red/amber during Saga rollbacks.
- [ ] **Step 3.3 — Live State Synchronization**
  - TanStack Query hooks consuming `/api/v1/events/stream` via EventSource.
  - Optimistic UI updates with cache invalidation on signal injection.

---

### 🌐 Phase 4: Production & Distributed Enhancements

- [ ] **Kafka / RabbitMQ Broker Adapters (`dispersion-messaging-kafka`)**:
  - Connect `CommandEnvelope` transport to Kafka topics with partitioned correlation keys.
- [ ] **Durable Checkpoint Stores (`dispersion-store-postgres`, `dispersion-store-redis`)**:
  - Persistent storage for suspended orchestration checkpoints across node restarts.
- [ ] **Cluster Leadership & Lease Management**:
  - Partition assignment for distributed state machine workers.

---

## 📌 Quick Commands Reference

| Action | Command |
| :--- | :--- |
| **Run All Unit & Integration Tests** | `./mvnw clean test` |
| **Fast Compile (No Tests)** | `./mvnw test-compile -DskipTests` |
| **Check Git Status** | `git status` |
| **View Latest Commits** | `git log --oneline -n 5` |
