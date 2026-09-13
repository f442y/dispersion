# Observability & Control Plane Guide

Dispersion features a centralized, hexagonal **Observability and Control Plane** designed for zero hot-path overhead, real-time workflow inspection, dynamic Mermaid diagram generation, $O(1)$ dual-pool memory topology, and bidirectional external signal routing.

---

## 1. Architectural Topology & Hexagonal Inversion

Traditional monitoring tools intrude upon execution engines via reflection, bytecode manipulation, or mandatory persistence. Dispersion isolates telemetry and control through the **Ports-and-Adapters (Hexagonal)** pattern:

```mermaid
graph TD
    subgraph Frontend["Modern Web UI (React + TanStack Router)"]
        TOP_UI["Topology Diagram (Mermaid / SVG)"]
        EXEC_UI["Execution Explorer & Timelines"]
        SIG_UI["Manual Signal Dispatcher"]
    end

    subgraph ControlPlane["dispersion-control-core (DefaultControlPlane)"]
        DCP["DefaultControlPlane"]
        APOOL["Active Pool (O(1) Hash Map)"]
        TPOOL["Terminal Pool (Bounded Circular Buffer)"]
        ROUTER["Signal Delivery Router"]
        DCP --- APOOL
        DCP --- TPOOL
        DCP --- ROUTER
    end

    subgraph SPI["dispersion-control-api (Contracts)"]
        IM["InspectableMachine (SPI)"]
        MD["MachineDescriptor"]
        ES["ExecutionSummary"]
    end

    subgraph Engines["Execution Engines"]
        ASME["AtomicStateMachineExecutor"]
        OSME["OrchestrationStateMachineExecutor"]
        BOSE["BatchOrchestrationExecutor"]
    end

    Frontend <-->|"REST / SSE / WebSockets"| DCP
    DCP ..> IM
    ASME -.->|"asInspectableMachine()"| IM
    OSME -.->|"asInspectableMachine()"| IM
    BOSE -.->|"asInspectableMachine()"| IM
```

### Key Architectural Invariants
1. **Zero Core-to-Core Coupling:**
   `dispersion-control-core` has **zero compile-time dependencies** on `fsm-core` or `orchestration-core`. It interacts exclusively through the `InspectableMachine` SPI and the `ExecutionEventListener` stream.
2. **Zero Hot-Path Penalties:**
   When telemetry is not subscribed, event dispatching evaluates to a single branch predictor check—**0** allocations, **0** system clock queries, and **0** thread context switches.
3. **Strict Fault Isolation:**
   Listener invocations are isolated with error boundaries. Telemetry failures or slow monitoring sinks **never** cause workflow state transitions or Saga compensations to abort.

---

## 2. Sealed Telemetry Hierarchy (`ExecutionEvent`)

Dispersion emits an immutable sealed record at every execution milestone:

| Event Record | Emitted When | Payload Highlights |
| :--- | :--- | :--- |
| `TurnStartedEvent` | Orchestration or atomic turn begins | `machineId`, `machineName`, `turnId`, `stateName` |
| `StateEnteredEvent` | Workflow transitions into a state node | `machineId`, `stateName` |
| `ActionExecutedEvent` | State action logic successfully runs | `machineId`, `stateName`, `duration` |
| `TransitionEvaluatedEvent` | Routing evaluation chooses next target state | `machineId`, `sourceState`, `targetState` |
| `StateExitedEvent` | Workflow leaves a state node | `machineId`, `stateName`, `duration` |
| `SignalAwaitedEvent` | Workflow suspends waiting for an external signal | `machineId`, `stateName`, `expectedSignal`, `correlationKey` |
| `SignalDeliveredEvent` | Inbound signal arrives and resumes workflow | `machineId`, `signalName`, `correlationKey` |
| `TurnSuspendedEvent` | Workflow snapshots checkpoint and releases thread | `machineId`, `stateName`, `correlationKey` |
| `TurnCompensatedEvent` | Downstream error triggers LIFO Saga rollback | `machineId`, `failedStateName`, `cause` |
| `TurnCompletedEvent` | Workflow reaches a terminal end state | `machineId`, `duration`, `output` |
| `TurnFailedEvent` | Unhandled error terminates workflow | `machineId`, `failedStateName`, `cause` |
| `BatchBarrierReachedEvent` | Batch item arrives at barrier policy | `machineId`, `batchId`, `itemKey`, `stateName` |
| `BatchBarrierUnlockedEvent` | Batch barrier condition satisfied; stage advances | `machineId`, `batchId`, `stateName`, `itemCount` |

### Exhaustive Pattern Matching in Java 25

```java
package com.example.observability;

import com.github.f442y.dispersion.event.ExecutionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TelemetryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TelemetryDispatcher.class);

    public void onEvent(ExecutionEvent event) {
        switch (event) {
            case ExecutionEvent.TurnStartedEvent started ->
                log.atInfo()
                   .addKeyValue("machine", started.machineName())
                   .addKeyValue("machine_id", started.machineId())
                   .log("Execution started");

            case ExecutionEvent.StateEnteredEvent entered ->
                log.atDebug()
                   .addKeyValue("machine", entered.machineName())
                   .addKeyValue("state", entered.stateName())
                   .log("Entering state");

            case ExecutionEvent.ActionExecutedEvent action ->
                log.atDebug()
                   .addKeyValue("state", action.stateName())
                   .addKeyValue("duration_ms", action.duration().toMillis())
                   .log("Action completed");

            case ExecutionEvent.TransitionEvaluatedEvent trans ->
                log.atDebug()
                   .addKeyValue("source", trans.sourceState())
                   .addKeyValue("target", trans.targetState())
                   .log("Transition evaluated");

            case ExecutionEvent.StateExitedEvent exited ->
                log.atDebug()
                   .addKeyValue("state", exited.stateName())
                   .addKeyValue("duration_ms", exited.duration().toMillis())
                   .log("State exited");

            case ExecutionEvent.SignalAwaitedEvent awaited ->
                log.atInfo()
                   .addKeyValue("state", awaited.stateName())
                   .addKeyValue("expected_signal", awaited.expectedSignal())
                   .log("Workflow suspended waiting for external signal");

            case ExecutionEvent.SignalDeliveredEvent delivered ->
                log.atInfo()
                   .addKeyValue("signal", delivered.signalName())
                   .log("Signal delivered to workflow");

            case ExecutionEvent.TurnSuspendedEvent suspended ->
                log.atInfo()
                   .addKeyValue("state", suspended.stateName())
                   .log("Turn suspended and state checkpointed");

            case ExecutionEvent.TurnCompensatedEvent compensated ->
                log.atWarn()
                   .addKeyValue("failed_state", compensated.failedStateName())
                   .log("Failure triggered LIFO Saga rollback");

            case ExecutionEvent.TurnCompletedEvent completed ->
                log.atInfo()
                   .addKeyValue("duration_ms", completed.duration().toMillis())
                   .log("Execution finished successfully");

            case ExecutionEvent.TurnFailedEvent failed ->
                log.atError()
                   .addKeyValue("failed_state", failed.failedStateName())
                   .setCause(failed.cause())
                   .log("Execution failed with unhandled error");

            case ExecutionEvent.BatchBarrierReachedEvent barrier ->
                log.atDebug()
                   .addKeyValue("item_key", barrier.itemKey())
                   .log("Item arrived at batch barrier");

            case ExecutionEvent.BatchBarrierUnlockedEvent unlocked ->
                log.atInfo()
                   .addKeyValue("state", unlocked.stateName())
                   .log("Batch barrier unlocked");
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

## 5. Modern Web UI Integration (React + TanStack Router)

Because `DefaultControlPlane` is built with pure Java and zero web framework coupling, exposing its operations over REST, Server-Sent Events (SSE), or WebSockets is trivial.

### Spring Boot / Javalin REST Controller Example

```java
package com.example.web;

import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/control")
public class ControlPlaneRestController {

    private final DefaultControlPlane controlPlane;

    public ControlPlaneRestController(DefaultControlPlane controlPlane) {
        this.controlPlane = controlPlane;
    }

    @GetMapping("/machines/{name}")
    public ResponseEntity<MachineDescriptor> getMachine(@PathVariable String name) {
        return controlPlane.getMachine(name)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/machines/{name}/executions")
    public List<ExecutionSummary> listExecutions(
            @PathVariable String name,
            @RequestParam(required = false, defaultValue = "SUSPENDED") ExecutionStatus status,
            @RequestParam(required = false, defaultValue = "50") int limit) {
        return controlPlane.listExecutions(name, status, limit);
    }
}
```

### React + TanStack Router UI View

In the React frontend, rendering the interactive topology uses the generated Mermaid string directly:

```tsx
import React, { useEffect, useRef } from 'react';
import mermaid from 'mermaid';

interface TopologyViewerProps {
  machineName: string;
  mermaidDiagram: string;
}

export const TopologyViewer: React.FC<TopologyViewerProps> = ({ machineName, mermaidDiagram }) => {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (containerRef.current && mermaidDiagram) {
      mermaid.initialize({ startOnLoad: false, theme: 'dark' });
      mermaid.render(`mermaid-${machineName}`, mermaidDiagram).then(({ svg }) => {
        if (containerRef.current) {
          containerRef.current.innerHTML = svg;
        }
      });
    }
  }, [machineName, mermaidDiagram]);

  return (
    <div className="rounded-xl border border-slate-700 bg-slate-900 p-6 shadow-2xl">
      <h2 className="text-xl font-bold text-white mb-4">Topology: {machineName}</h2>
      <div ref={containerRef} className="overflow-x-auto flex justify-center" />
    </div>
  );
};
```
