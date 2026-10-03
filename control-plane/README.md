# Dispersion Observability & Control Plane Subsystem (`control`)

The **Dispersion Control Subsystem** provides a centralized, hexagonal **Observability and Control Plane** engineered for real-time monitoring, live topology discovery, dynamic Mermaid diagram generation, execution timeline reconstruction, and bi-directional signal routing.

---

## 1. Module Structure & Hexagonal SPI Inversion

The control plane subsystem is strictly decoupled from the execution runtimes. `dispersion-control-plane-core` **has zero dependencies on `fsm-core` or `orchestration-core`**, interacting purely through the `InspectableMachine` SPI, `TraceTimelineProvider` SPI, and `ExecutionEventListener` telemetry stream:

```mermaid
graph TD
    subgraph API["dispersion-control-plane-api (Contract)"]
        CP["ControlPlane (Interface)"]
        IM["InspectableMachine (SPI)"]
        TTP["TraceTimelineProvider (SPI)"]
        MD["MachineDescriptor"]
        ES["ExecutionSummary / ExecutionStatus"]
        SDR["SignalDeliveryResult"]
    end

    subgraph Core["dispersion-control-plane-core (Registry & Router)"]
        DCP["DefaultControlPlane"]
        DCP --> CP
        DCP ..> IM
        DCP ..> TTP
    end

    subgraph Engines["Execution Engines (Adapters)"]
        ASME["AtomicStateMachineExecutor"]
        OSME["OrchestrationExecutor"]
        BOSE["BatchOrchestrationExecutor"]
        ASME -.->|"asInspectableMachine()"| IM
        OSME -.->|"asInspectableMachine()"| IM
        BOSE -.->|"asInspectableMachine()"| IM
    end

    subgraph Test["dispersion-control-plane-test (Test Doubles)"]
        FIM["FakeInspectableMachine"]
        FIM --> IM
    end

    Core --> API
    Test --> API
```

### Module Matrix

| Module | JPMS Module Name | Description |
| :--- | :--- | :--- |
| **`dispersion-control-plane-api`** | `com.github.f442y.dispersion.control.api` | Contracts for the Control Plane, `InspectableMachine` SPI, `TraceTimelineProvider` SPI, descriptors, execution summaries, timelines, and signal delivery results. |
| **`dispersion-control-plane-core`** | `com.github.f442y.dispersion.control.core` | `DefaultControlPlane` in-memory engine featuring an $O(1)$ dual-pool memory topology, telemetry event aggregation, dynamic Mermaid diagram generation, execution cancellation, and signal routing. |
| **`dispersion-control-plane-test`** | `com.github.f442y.dispersion.control.test` | `FakeInspectableMachine` test double for testing control plane endpoints, UI dashboards, and administrative workflows in isolation. |

---

## 2. Architectural Highlights

### 1. Decoupled `InspectableMachine` SPI
Any workflow engine can become inspectable by implementing the `InspectableMachine` SPI. All Dispersion executors provide this out of the box via `.asInspectableMachine()`:
* **Topology Discovery:** Exposes initial state, terminal states, transitions, and machine type (`ATOMIC`, `ORCHESTRATION`, `BATCH`).
* **Dynamic Visualization:** Generates standard Mermaid diagram markup (`graph TD ...`) reflecting the active graph topology.
* **Signal Forwarding:** Delivers external commands directly into running workflow instances.
* **Checkpoint Inspection:** Exposes strongly-typed checkpoints for suspended workflows.

### 2. Dual-Pool $O(1)$ Memory Architecture
To prevent unbounded memory growth and eliminate expensive full-collection linear scans, `DefaultControlPlane` partitions executions into two segregated pools:
* **Active Execution Pool:** A concurrent hash map indexed by execution ID and correlation key tracking in-flight workflows (`RUNNING`, `SUSPENDED`, `PAUSED`). Active executions are critical operational entities awaiting internal turns or external signals and are **never** evicted.
* **Bounded Terminal Ring Buffer:** A thread-safe, bounded circular FIFO pool retaining terminal workflows (`COMPLETED`, `FAILED`, `CANCELLED`, `COMPENSATED`). When terminal executions exceed the configured capacity (e.g. 10,000), the oldest terminal execution is pruned in strictly **$O(1)$** time with zero lock contention and zero GC pauses.

### 3. Decoupled `TraceTimelineProvider` SPI
To keep the control plane's index lean and $O(\text{executions})$ in memory:
* Granular telemetry logs (step transitions, guard evaluations, payload metadata) are kept separate from the summary index.
* When operators or UI drawers request an execution's chronological event timeline, `DefaultControlPlane` queries a registered `TraceTimelineProvider`.
* Backed out-of-the-box by `TraceTimelineProvider.from(eventBus.traceBuffer())` (zero-copy query into the worker's ring buffer), or custom durable storage providers.

### 4. Lifecycle & Cancellation Tracking
Workflows can be cancelled by operators or external commands via `ExecutionCancelledEvent`:
* When an execution is cancelled, the control plane immediately updates its status to `ExecutionStatus.CANCELLED`, records the cancellation timestamp and operator reason, and moves the summary into the bounded terminal ring buffer.
* Cancelling an execution marks the workflow as terminal, ensuring deterministic resource reclamation and immediate UI state reflection.

### 5. Workload Router & Worker Node Topology Inspection
The Control Plane integrates with `dispersion-routing` via `InspectableRouter`:
* **Router Registration:** `controlPlane.registerRouter(inspectableRouter)` connects the active routing subsystem.
* **Live Worker Topology:** Discover all registered monolith and distributed endpoints, grouped by service name.
* **Health & Backpressure Metrics:** Query real-time endpoint health statuses, active concurrency counters, and traffic-split ratios.

---

## 3. End-to-End Control Plane Example

The following example shows how to initialize `DefaultControlPlane`, wire the event bus and trace buffer, register an execution engine, query live summaries, and inspect execution timelines:

```java
package com.example.control;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.TraceTimelineProvider;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.event.EventBus;
import com.github.f442y.dispersion.event.ExecutionEvent;
import com.github.f442y.dispersion.event.bus.VirtualThreadEventBus;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.core.OrchestrationBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationExecutor;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class ControlPlaneExample {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneExample.class);

    public enum ReviewState implements StateKey {
        DRAFT, IN_REVIEW, PUBLISHED
    }

    public static final class ReviewContext implements StateMachineContext {
        public String articleId;
        public String reviewer;
    }

    public record ApproveArticleSignal(@NonNull String correlationKey, @NonNull String reviewer) implements SignalCommand {
        @Override
        @NonNull
        public String signalName() {
            return "ApproveArticleSignal";
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. Initialize Event Bus and Control Plane
        try (EventBus eventBus = new VirtualThreadEventBus();
             DefaultControlPlane controlPlane = new DefaultControlPlane(1_000)) {

            // Automatically wires listener and trace timeline provider
            controlPlane.attachToBus(eventBus);

            InMemoryCheckpointStore<ReviewContext, ReviewState> store = new InMemoryCheckpointStore<>();

            // 2. Build Orchestration Machine and wire Control Plane EventListener
            try (OrchestrationExecutor<ReviewContext, ReviewState, String, String> executor =
                     OrchestrationBuilder.<ReviewContext, ReviewState, String, String>create("ArticleReviewMachine", ReviewState.class)
                         .context(ReviewContext::new)
                         .initialState(ReviewState.DRAFT)
                         .endStates(ReviewState.PUBLISHED)
                         .checkpointStore(store)
                         .correlationKey((ReviewContext ctx) -> ctx.articleId)
                         .eventListener(controlPlane.getEventListener())
                         .input((ReviewContext ctx, String articleId) -> { ctx.articleId = articleId; return ctx; })

                         .state(ReviewState.DRAFT)
                             .action((ReviewContext ctx) -> ctx)
                             .transition(ReviewState.IN_REVIEW)

                         .state(ReviewState.IN_REVIEW)
                             .waitForCommand(ApproveArticleSignal.class, (ReviewContext ctx, ApproveArticleSignal sig) -> {
                                 ctx.reviewer = sig.reviewer();
                                 return ctx;
                             })
                             .transition(ReviewState.PUBLISHED)

                         .output((ReviewContext ctx) -> "Article " + ctx.articleId + " published by " + ctx.reviewer)
                         .buildExecutor()) {

                // 3. Register engine into Control Plane via decoupled SPI
                controlPlane.register(executor.asInspectableMachine());

                // 4. Query Machine Descriptor and Dynamic Mermaid Diagram
                Optional<MachineDescriptor> descOpt = controlPlane.getMachine("ArticleReviewMachine");
                if (descOpt.isPresent()) {
                    MachineDescriptor desc = descOpt.get();
                    log.atInfo()
                       .addKeyValue("name", desc.name())
                       .addKeyValue("type", desc.type())
                       .addKeyValue("mermaid", desc.mermaidDiagram())
                       .log("Discovered registered machine");
                }

                // 5. Trigger Turn 1 -> Suspends at IN_REVIEW
                executor.dispatchTurnSync("ART-501", "ART-501");

                // 6. Inspect Suspended Executions
                List<ExecutionSummary> suspended = controlPlane.listExecutions("ArticleReviewMachine", ExecutionStatus.SUSPENDED, 10);
                log.atInfo().addKeyValue("suspended_count", suspended.size()).log("Queried suspended workflows");

                UUID executionId = suspended.getFirst().executionId();

                // 7. Query Chronological Event Timeline
                List<ExecutionEvent> timeline = controlPlane.getExecutionTimeline(executionId);
                log.atInfo().addKeyValue("event_count", timeline.size()).log("Fetched execution timeline");

                // 8. Route Signal Through Control Plane
                CompletableFuture<SignalDeliveryResult> deliveryFuture = controlPlane.sendSignal(
                    "ArticleReviewMachine",
                    "ART-501",
                    "ApproveArticleSignal",
                    new ApproveArticleSignal("ART-501", "Editor-in-Chief")
                );

                SignalDeliveryResult result = deliveryFuture.get();
                log.atInfo()
                   .addKeyValue("delivered", result.delivered())
                   .addKeyValue("completed", result.completed())
                   .addKeyValue("final_state", result.resultingState())
                   .log("Signal routed through Control Plane successfully");
            }
        }
    }
}
```

---

## 4. Testing Control Plane Integrations (`dispersion-control-plane-test`)

Use `FakeInspectableMachine` to test dashboards, REST controllers, or CLI admin tools without running heavy execution engines:

```java
package com.example.control;

import com.github.f442y.dispersion.control.MachineType;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.control.test.FakeInspectableMachine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ControlPlaneTestingExampleTests {

    @Test
    @DisplayName("Should register FakeInspectableMachine into ControlPlane")
    public void testFakeMachineRegistration() {
        try (DefaultControlPlane controlPlane = new DefaultControlPlane()) {
            FakeInspectableMachine fake = new FakeInspectableMachine("MockWorkflow", MachineType.ORCHESTRATION);
            fake.setInitialState("INIT");
            fake.setEndStates("DONE");

            controlPlane.register(fake);

            assertThat(controlPlane.getMachine("MockWorkflow")).isPresent();
            assertThat(controlPlane.getMachine("MockWorkflow").get().initialState()).isEqualTo("INIT");
        }
    }
}
```

---

## 5. Maven Dependency Setup

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.github.f442y.dispersion</groupId>
            <artifactId>dispersion-bom</artifactId>
            <version>0.1.0-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- Public API Contract -->
    <dependency>
        <groupId>com.github.f442y.dispersion</groupId>
        <artifactId>dispersion-control-plane-api</artifactId>
    </dependency>

    <!-- Runtime Control Plane Registry -->
    <dependency>
        <groupId>com.github.f442y.dispersion</groupId>
        <artifactId>dispersion-control-plane-core</artifactId>
    </dependency>

    <!-- Testing Double (Scope: Test) -->
    <dependency>
        <groupId>com.github.f442y.dispersion</groupId>
        <artifactId>dispersion-control-plane-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

---

## 🔗 Related Subsystems & Guides

* 🏠 [**Project Showcase (`README.md`)**](../README.md) — Landing page, architecture overview, and quickstarts.
* 🌐 [**Server Subsystem (`server/`)**](../server/README.md) — Virtual-thread HTTP/SSE hosting and Jakarta REST resource.
* 📦 [**Serialization Subsystem (`serialization/`)**](../serialization/README.md) — Reflection-free Avaje JSON and Apache Fury codecs.
* ⚡ [**Tier 1 FSM Subsystem (`fsm/`)**](../fsm/README.md) — Adapting atomic state machines via `executor.asInspectableMachine()`.
* 🚦 [**Routing Subsystem (`routing/`)**](../routing/README.md) — Inspectable workload router, Canary traffic splits, and Developer Sandboxes.
* 🔄 [**Orchestration Subsystem (`orchestration/`)**](../orchestration/README.md) — Adapting saga orchestrators and batch engines via `executor.asInspectableMachine()`.
* 📡 [**Event Subsystem (`event/`)**](../event/README.md) — Connecting event buses and telemetry listeners to `DefaultControlPlane`.
* 🧪 [**Testing Framework (`testkit/`)**](../testkit/README.md) — Unit testing control plane workflows with `FakeInspectableMachine`.
* 🔭 [**Observability & UI Deep Dive**](../docs/observability-and-control-plane.md) — Control plane guide and SSE integration.
