# Dispersion Observability & Control Plane Subsystem (`control`)

The **Dispersion Control Subsystem** provides a centralized, hexagonal **Observability and Control Plane** engineered for real-time monitoring, live topology discovery, dynamic Mermaid diagram generation, execution timeline reconstruction, and bi-directional signal routing.

---

## 1. Module Structure & Hexagonal SPI Inversion

The control subsystem is strictly decoupled from the execution runtimes. `dispersion-control-core` **has zero dependencies on `fsm-core` or `orchestration-core`**, interacting purely through the `InspectableMachine` SPI and `ExecutionEventListener` telemetry stream:

```mermaid
graph TD
    subgraph API["dispersion-control-api (Contract)"]
        CP["ControlPlane (Interface)"]
        IM["InspectableMachine (SPI)"]
        MD["MachineDescriptor"]
        ES["ExecutionSummary / ExecutionStatus"]
        SDR["SignalDeliveryResult"]
    end

    subgraph Core["dispersion-control-core (Registry & Router)"]
        DCP["DefaultControlPlane"]
        DCP --> CP
        DCP ..> IM
    end

    subgraph Engines["Execution Engines (Adapters)"]
        ASME["AtomicStateMachineExecutor"]
        OSME["OrchestrationStateMachineExecutor"]
        BOSE["BatchOrchestrationExecutor"]
        ASME -.->|"asInspectableMachine()"| IM
        OSME -.->|"asInspectableMachine()"| IM
        BOSE -.->|"asInspectableMachine()"| IM
    end

    subgraph Test["dispersion-control-test (Test Doubles)"]
        FIM["FakeInspectableMachine"]
        FIM --> IM
    end

    Core --> API
    Test --> API
```

### Module Matrix

| Module | JPMS Module Name | Description |
| :--- | :--- | :--- |
| **`dispersion-control-api`** | `com.github.f442y.dispersion.control.api` | Contracts for the Control Plane, `InspectableMachine` SPI, descriptors, execution summaries, timelines, and signal delivery results. |
| **`dispersion-control-core`** | `com.github.f442y.dispersion.control.core` | `DefaultControlPlane` in-memory engine featuring an $O(1)$ dual-pool memory topology, telemetry event aggregation, dynamic Mermaid diagram generation, and signal routing. |
| **`dispersion-control-test`** | `com.github.f442y.dispersion.control.test` | `FakeInspectableMachine` test double for testing control plane endpoints, UI dashboards, and administrative workflows in isolation. |

---

## 2. Architectural Highlights

### 1. Decoupled `InspectableMachine` SPI
Any workflow engine can become inspectable by implementing the `InspectableMachine` SPI. All Dispersion executors provide this out of the box via `.asInspectableMachine()`:
* **Topology Discovery:** Exposes initial state, terminal states, transitions, and machine type (`ATOMIC`, `ORCHESTRATION`, `BATCH`).
* **Dynamic Visualization:** Generates standard Mermaid diagram markup (`graph TD ...`) reflecting the active graph topology.
* **Signal Forwarding:** Delivers external commands directly into running workflow instances.
* **Checkpoint Inspection:** Exposes strongly-typed checkpoints for suspended workflows.

### 2. Dual-Pool $O(1)$ Memory Architecture
To prevent unbounded memory growth and eliminate expensive full-table scans, `DefaultControlPlane` uses a dual-pool memory architecture:
* **Active Execution Pool:** A concurrent hash map indexed by execution ID and correlation key for fast $O(1)$ lookups of active or suspended workflows.
* **Bounded Terminal Ring Buffer:** A thread-safe, bounded circular pool retaining completed and failed workflows. As new executions terminate, older executions are evicted with zero lock contention and zero GC pauses.

---

## 3. End-to-End Control Plane Example

The following example shows how to initialize `DefaultControlPlane`, register an execution engine, query live summaries, and deliver signals:

```java
package com.example.control;

import com.github.f442y.dispersion.control.ControlPlane;
import com.github.f442y.dispersion.control.ExecutionStatus;
import com.github.f442y.dispersion.control.ExecutionSummary;
import com.github.f442y.dispersion.control.MachineDescriptor;
import com.github.f442y.dispersion.control.SignalDeliveryResult;
import com.github.f442y.dispersion.control.core.DefaultControlPlane;
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.command.SignalCommand;
import com.github.f442y.dispersion.orchestration.core.InMemoryCheckpointStore;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineBuilder;
import com.github.f442y.dispersion.orchestration.core.OrchestrationStateMachineExecutor;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
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
        // 1. Initialize Control Plane (Ring Buffer Capacity: 1,000 executions)
        try (DefaultControlPlane controlPlane = new DefaultControlPlane(1_000)) {

            InMemoryCheckpointStore<ReviewContext, ReviewState> store = new InMemoryCheckpointStore<>();

            // 2. Build Orchestration Machine and wire Control Plane EventListener
            try (OrchestrationStateMachineExecutor<ReviewContext, ReviewState, String, String> executor =
                     OrchestrationStateMachineBuilder.<ReviewContext, ReviewState, String, String>create("ArticleReviewMachine", ReviewState.class)
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

                // 7. Route Signal Through Control Plane
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

## 4. Testing Control Plane Integrations (`dispersion-control-test`)

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
            <version>1.0.0-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- Public API Contract -->
    <dependency>
        <groupId>com.github.f442y.dispersion</groupId>
        <artifactId>dispersion-control-api</artifactId>
    </dependency>

    <!-- Runtime Control Plane Registry -->
    <dependency>
        <groupId>com.github.f442y.dispersion</groupId>
        <artifactId>dispersion-control-core</artifactId>
    </dependency>

    <!-- Testing Double (Scope: Test) -->
    <dependency>
        <groupId>com.github.f442y.dispersion</groupId>
        <artifactId>dispersion-control-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```
