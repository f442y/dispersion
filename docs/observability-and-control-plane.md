# Observability & Control Plane Guide

Dispersion features a centralized, hexagonal **Observability and Control Plane** designed for zero hot-path overhead, real-time workflow inspection, dynamic Mermaid diagram generation, $O(1)$ dual-pool memory topology, standalone Helidon SE virtual-thread HTTP/SSE hosting, and bidirectional external signal routing.

---

## 1. Architectural Topology & Hexagonal Inversion

Traditional monitoring tools intrude upon execution engines via reflection, bytecode manipulation, or mandatory persistence. Dispersion isolates telemetry and control through the **Ports-and-Adapters (Hexagonal)** pattern:

```mermaid
graph TD
    subgraph UI["Web Client / Dashboard (ui/)"]
        TOP_UI["Topology Diagram (Mermaid / SVG)"]
        EXEC_UI["Execution Explorer & Timelines"]
        SIG_UI["Manual Signal Dispatcher"]
    end

    subgraph ServerHost["dispersion-server-standalone (Helidon SE 4.x Níma)"]
        HTTP["HTTP Routing on Virtual Threads"]
        SSE["Server-Sent Events Stream"]
        JAK["dispersion-server-jakarta (JAX-RS)"]
    end

    subgraph ControlPlane["dispersion-control-plane-core (DefaultControlPlane)"]
        DCP["DefaultControlPlane"]
        APOOL["Active Pool (O(1) Hash Map)"]
        TPOOL["Terminal Pool (Bounded Circular Buffer)"]
        ROUTER["Signal Delivery Router"]
        DCP --- APOOL
        DCP --- TPOOL
        DCP --- ROUTER
    end

    subgraph SPI["dispersion-control-plane-api (Contracts)"]
        IM["InspectableMachine (SPI)"]
        MD["MachineDescriptor"]
        ES["ExecutionSummary"]
    end

    subgraph Engines["Execution Engines"]
        ASME["AtomicStateMachineExecutor"]
        OSME["OrchestrationExecutor"]
        BOSE["BatchOrchestrationExecutor"]
    end

    UI <-->|"REST / SSE (/api/v1)"| ServerHost
    ServerHost <--> DCP
    DCP ..> IM
    ASME -.->|"asInspectableMachine()"| IM
    OSME -.->|"asInspectableMachine()"| IM
    BOSE -.->|"asInspectableMachine()"| IM
```

### Key Architectural Invariants
1. **Zero Core-to-Core Coupling:**
   `dispersion-control-plane-core` has **zero compile-time dependencies** on `fsm-core` or `orchestration-core`. It interacts exclusively through the `InspectableMachine` SPI and the `ExecutionEventListener` stream.
2. **Zero Hot-Path Penalties:**
   When telemetry is not subscribed, event dispatching evaluates to a single branch predictor check—**0** allocations, **0** system clock queries, and **0** thread context switches.
3. **Strict Fault Isolation:**
   Listener invocations are isolated with error boundaries. Telemetry failures or slow monitoring sinks **never** cause workflow state transitions or Saga compensations to abort.
4. **Decoupled Server Hosting:**
   `dispersion-server-standalone` hosts the control plane using Helidon SE 4.x Níma on virtual threads without polluting core domain logic. JSON serialization is offloaded to compile-time reflection-free codecs (`dispersion-serialization-avaje`).

---

## 2. Telemetry Hierarchy (`ExecutionEvent`)

Dispersion emits immutable telemetry records at every execution milestone. The foundational `ExecutionEvent` interface defines core lifecycle events nested directly within modular subpackages (`event.turn`, `event.state`, `event.signal`, `event.compensation`, `event.retry`, `event.child`, `event.parallel`, `event.guard`, `event.control`), while domain-specific events implement `ExecutionEvent` directly from their respective modules:

| Event Record | Module | Emitted When | Payload Highlights |
| :--- | :--- | :--- | :--- |
| `TurnStartedEvent` | `dispersion-event-api` | Orchestration or atomic turn begins | `machineId`, `machineName`, `turnId`, `stateName` |
| `StateEnteredEvent` | `dispersion-event-api` | Workflow transitions into a state node | `machineId`, `stateName` |
| `ActionExecutedEvent` | `dispersion-event-api` | State action logic successfully runs | `machineId`, `stateName`, `duration` |
| `TransitionEvaluatedEvent` | `dispersion-event-api` | Routing evaluation chooses next target state | `machineId`, `sourceState`, `targetState` |
| `StateExitedEvent` | `dispersion-event-api` | Workflow leaves a state node | `machineId`, `stateName`, `duration` |
| `SignalAwaitedEvent` | `dispersion-event-api` | Workflow suspends waiting for an external signal | `machineId`, `stateName`, `expectedSignal`, `correlationKey` |
| `SignalDeliveredEvent` | `dispersion-event-api` | Inbound signal arrives and resumes workflow | `machineId`, `signalName`, `correlationKey` |
| `TurnSuspendedEvent` | `dispersion-event-api` | Workflow snapshots checkpoint and releases thread | `machineId`, `stateName`, `correlationKey` |
| `TurnCompensatedEvent` | `dispersion-event-api` | Downstream error triggers LIFO Saga rollback | `machineId`, `failedStateName`, `cause` |
| `TurnCompletedEvent` | `dispersion-event-api` | Workflow reaches a terminal end state | `machineId`, `duration`, `output` |
| `TurnFailedEvent` | `dispersion-event-api` | Unhandled error terminates workflow | `machineId`, `failedStateName`, `cause` |
| `CommandDeduplicatedEvent` | `dispersion-orchestration-api` | Duplicate command envelope skipped | `machineId`, `commandId`, `correlationKey` |
| `BatchBarrierReachedEvent` | `dispersion-orchestration-batch` | Batch item arrives at barrier policy | `machineId`, `batchId`, `itemKey`, `stateName` |
| `BatchBarrierUnlockedEvent` | `dispersion-orchestration-batch` | Batch barrier condition satisfied; stage advances | `machineId`, `batchId`, `stateName`, `itemCount` |

### Pattern Matching in Java 25

```java
package com.example.observability;

import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.signal.SignalAwaitedEvent;
import com.github.f442y.dispersion.event.signal.SignalDeliveredEvent;
import com.github.f442y.dispersion.event.state.ActionExecutedEvent;
import com.github.f442y.dispersion.event.state.StateEnteredEvent;
import com.github.f442y.dispersion.event.state.StateExitedEvent;
import com.github.f442y.dispersion.event.state.TransitionEvaluatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompensatedEvent;
import com.github.f442y.dispersion.event.turn.TurnCompletedEvent;
import com.github.f442y.dispersion.event.turn.TurnFailedEvent;
import com.github.f442y.dispersion.event.turn.TurnStartedEvent;
import com.github.f442y.dispersion.event.turn.TurnSuspendedEvent;
import com.github.f442y.dispersion.orchestration.batch.BatchBarrierReachedEvent;
import com.github.f442y.dispersion.orchestration.batch.BatchBarrierUnlockedEvent;
import com.github.f442y.dispersion.orchestration.command.CommandDeduplicatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TelemetryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TelemetryDispatcher.class);

    public void onEvent(ExecutionEvent event) {
        switch (event) {
            case TurnStartedEvent started ->
                log.atInfo()
                   .addKeyValue("machine", started.machineName())
                   .addKeyValue("machine_id", started.machineId())
                   .log("Execution started");

            case StateEnteredEvent entered ->
                log.atDebug()
                   .addKeyValue("machine", entered.machineName())
                   .addKeyValue("state", entered.stateName())
                   .log("Entering state");

            case ActionExecutedEvent action ->
                log.atDebug()
                   .addKeyValue("state", action.stateName())
                   .addKeyValue("duration_ms", action.duration().toMillis())
                   .log("Action completed");

            case TransitionEvaluatedEvent trans ->
                log.atDebug()
                   .addKeyValue("source", trans.sourceState())
                   .addKeyValue("target", trans.targetState())
                   .log("Transition evaluated");

            case StateExitedEvent exited ->
                log.atDebug()
                   .addKeyValue("state", exited.stateName())
                   .addKeyValue("duration_ms", exited.duration().toMillis())
                   .log("State exited");

            case SignalAwaitedEvent awaited ->
                log.atInfo()
                   .addKeyValue("state", awaited.stateName())
                   .addKeyValue("expected_signal", awaited.expectedSignal())
                   .log("Workflow suspended waiting for external signal");

            case SignalDeliveredEvent delivered ->
                log.atInfo()
                   .addKeyValue("signal", delivered.signalName())
                   .log("Signal delivered to workflow");

            case TurnSuspendedEvent suspended ->
                log.atInfo()
                   .addKeyValue("state", suspended.stateName())
                   .log("Turn suspended and state checkpointed");

            case TurnCompensatedEvent compensated ->
                log.atWarn()
                   .addKeyValue("failed_state", compensated.failedStateName())
                   .log("Failure triggered LIFO Saga rollback");

            case TurnCompletedEvent completed ->
                log.atInfo()
                   .addKeyValue("duration_ms", completed.duration().toMillis())
                   .log("Execution finished successfully");

            case TurnFailedEvent failed ->
                log.atError()
                   .addKeyValue("failed_state", failed.failedStateName())
                   .setCause(failed.cause())
                   .log("Execution failed with unhandled error");

            case BatchBarrierReachedEvent barrier ->
                log.atDebug()
                   .addKeyValue("item_key", barrier.itemKey())
                   .log("Item arrived at batch barrier");

            case BatchBarrierUnlockedEvent unlocked ->
                log.atInfo()
                   .addKeyValue("state", unlocked.stateName())
                   .log("Batch barrier unlocked");

            case CommandDeduplicatedEvent dedup ->
                log.atWarn()
                   .addKeyValue("command_id", dedup.commandId())
                   .log("Duplicate command discarded");

            default ->
                log.atDebug()
                   .addKeyValue("event_type", event.getClass().getSimpleName())
                   .log("Received custom execution event");
        }
    }
}
```

---

## 3. Dual-Pool $O(1)$ Memory Architecture

To prevent out-of-memory errors and eliminate expensive table scans, `DefaultControlPlane` partitions workflow storage into two specialized pools:

```
                    ┌─────────────────────────────────────────────────────────┐
                    │               DefaultControlPlane Memory                │
                    │                                                         │
                    │   ┌─────────────────────────────────────────────────┐   │
In-Flight           │   │           Active Execution Pool (O(1))          │   │
Executions ────────►│   │   • Running or Suspended workflows              │   │
                    │   │   • Indexed by ExecutionId & CorrelationKey     │   │
                    │   │   • Immune to eviction while active             │   │
                    │   └─────────────────────────────────────────────────┘   │
                    │                            │                            │
                    │                            ▼ (Upon Completion / Failure)│
                    │   ┌─────────────────────────────────────────────────┐   │
Completed /         │   │     Bounded Terminal Ring Buffer Pool (O(1))    │   │
Failed Executions ─►│   │   • Bounded capacity (e.g. 10,000 items)        │   │
                    │   │   • Lock-free FIFO eviction of oldest entries   │   │
                    │   │   • Zero GC pause overhead                      │   │
                    │   └─────────────────────────────────────────────────┘   │
                    └─────────────────────────────────────────────────────────┘
```

1. **Active Pool (`activeExecutions`):**
   Stores workflows that are currently `RUNNING` or `SUSPENDED` (waiting for webhooks, human approvals, or batch sync). These represent live business processes and are **strictly protected from eviction**.
2. **Bounded Terminal Pool (`terminalExecutions`):**
   Stores workflows that reached `COMPLETED` or `FAILED`. When the configured limit (e.g. 10,000 executions) is exceeded, older records are evicted in **strictly $O(1)$ time** without traversing collections.

---

## 4. End-to-End Control Plane Operations

```java
package com.example.observability;

import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.orchestration.OrchestrationCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public final class ControlPlaneOperations {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneOperations.class);

    public void queryAndControl(DefaultControlPlane controlPlane, String machineName, String correlationKey) throws Exception {
        // 1. Topology Discovery & Dynamic Mermaid Diagram
        Optional<MachineDescriptor> descriptorOpt = controlPlane.getMachine(machineName);
        if (descriptorOpt.isPresent()) {
            MachineDescriptor descriptor = descriptorOpt.get();
            log.atInfo()
               .addKeyValue("machine_name", descriptor.name())
               .addKeyValue("machine_type", descriptor.type())
               .addKeyValue("initial_state", descriptor.initialState())
               .addKeyValue("mermaid", descriptor.mermaidDiagram())
               .log("Discovered state machine topology");
        }

        // 2. Query Suspended Executions
        List<ExecutionSummary> suspended = controlPlane.listExecutions(machineName, ExecutionStatus.SUSPENDED, 20);
        log.atInfo().addKeyValue("count", suspended.size()).log("Queried suspended workflows");

        // 3. Inspect Checkpoint for Suspended Saga
        Optional<OrchestrationCheckpoint> checkpoint =
            controlPlane.inspectCheckpoint(machineName, correlationKey, OrchestrationCheckpoint.class);

        checkpoint.ifPresent(cp -> log.atInfo()
            .addKeyValue("correlation_key", cp.correlationKey())
            .addKeyValue("state", cp.currentStateKey())
            .log("Inspected suspended saga checkpoint"));

        // 4. Deliver External Signal via Control Plane
        CompletableFuture<SignalDeliveryResult> deliveryFuture = controlPlane.sendSignal(
            machineName,
            correlationKey,
            "ApprovalSignal",
            "APPROVED_BY_ADMIN"
        );

        SignalDeliveryResult delivery = deliveryFuture.get();
        log.atInfo()
           .addKeyValue("delivered", delivery.delivered())
           .addKeyValue("completed", delivery.completed())
           .addKeyValue("final_state", delivery.resultingState())
           .log("Delivered signal through Control Plane");
    }
}
```

---

## 5. Standalone Server Host & HTTP/SSE Endpoints (`dispersion-server-standalone`)

Dispersion provides built-in HTTP and Server-Sent Events (SSE) servers via `dispersion-server-standalone` (powered by Helidon SE 4.x Níma virtual threads) and `dispersion-server-jakarta` (JAX-RS 3.1).

### Starting the Helidon Standalone Server

```java
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.server.api.ControlPlaneServer;
import com.github.f442y.dispersion.server.api.ControlPlaneServerFactory;
import com.github.f442y.dispersion.server.api.ServerConfig;

DefaultControlPlane controlPlane = new DefaultControlPlane();
// ... register inspectable machines ...

ServerConfig config = ServerConfig.builder()
    .port(8080)
    .basePath("/api/v1")
    .cors(true)
    .build();

ControlPlaneServer server = ControlPlaneServerFactory.create(controlPlane, config);
server.start();

System.out.println("Control Plane Server listening on: " + server.boundAddress());
```

### Standard HTTP & SSE Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/api/v1/node` | Cluster node health, CPU core count, JVM uptime, active machine count. |
| `GET` | `/api/v1/machines` | List all registered state machine descriptors. |
| `GET` | `/api/v1/machines/{name}` | Machine topology, initial/end states, and dynamic Mermaid graph string. |
| `POST` | `/api/v1/machines/{name}/dispatch` | Dispatches an execution input directly into a registered machine. |
| `GET` | `/api/v1/executions` | Query executions filtered by `machine`, `status`, or limit. |
| `GET` | `/api/v1/executions/{id}` | Summary status and current state for a specific execution. |
| `GET` | `/api/v1/executions/{id}/timeline` | Ordered chronological event timeline for an execution. |
| `POST` | `/api/v1/executions/signal` | Deliver external signal (`machineName`, `correlationKey`, `signalName`, `payload`). |
| `GET` | `/api/v1/events/stream` | Real-time Server-Sent Events (SSE) telemetry event stream (`text/event-stream`). Optional filter query parameters: `machine`, `executionId`. |

---

## 6. Web Control Panel UI Integration

A modern browser interface is provided in [`ui/`](../ui/) (built with Vite, React 19, and Tailwind CSS).

The web client interacts directly with the Helidon SE control plane server:
* **Topology Diagrams:** Consumes `/api/v1/machines/{name}` and renders the pre-computed Mermaid graph.
* **Execution Explorer:** Queries `/api/v1/executions` to visualize active, suspended, and terminal turns.
* **Live SSE Telemetry:** Subscribes to `/api/v1/events/stream` to update node highlights and event logs in real time.
* **Signal Injection:** Submits manual signals via `POST /api/v1/executions/signal`.

> [!NOTE]
> The UI in `ui/` is subject to ongoing design refinements and frontend UX iteration. The backend server endpoints and Avaje JSON schemas serve as the stable system contract.
