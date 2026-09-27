# Dispersion Orchestration Batch Module (`dispersion-orchestration-batch`)

The **Dispersion Orchestration Batch Module** provides a turn-based synchronized batch execution engine engineered for **Java 25+ Virtual Threads**. It coordinates bulk workflows where collections of domain items advance through coordinated stages with barrier synchronization.

---

## 1. Capabilities & Barrier Synchronization

* **Item-Level Virtual Threads**: Each item in a batch executes concurrently on its own virtual thread with isolated context mutability.
* **Synchronized Barriers**: Items advance through steps based on configurable `BarrierPolicy` rules:
  * **`BarrierPolicy.ALL_ITEMS_ARRIVED`**: Every item in the batch must arrive at the barrier before the batch advances to the next step.
  * **`BarrierPolicy.SIGNAL_TRIGGERED`**: Pauses batch items at the barrier until an explicit external signal triggers continuation.
* **Granular Checkpointing**: `BatchCheckpoint` records the progress and state of every individual item, allowing interrupted batches to resume without reprocessing completed items.
* **InspectableMachine Adapter**: Native `.asInspectableMachine()` support for dynamic Mermaid topologies, checkpoint inspection, and batch/item signal routing in the Control Plane.
* **Extensible Telemetry**: Emits `BatchBarrierReachedEvent` and `BatchBarrierUnlockedEvent` directly to the `ExecutionEvent` bus.

---

## 2. Example Usage

```java
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.batch.BarrierPolicy;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationBuilder;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationExecutor;
import com.github.f442y.dispersion.orchestration.batch.BatchTurnResult;
import java.util.List;

public enum BatchStep implements StateKey {
    INGEST, PROCESS, SETTLE, COMPLETED
}

public static final class BatchRootContext implements StateMachineContext {
    public String batchId;
}

public static final class ItemContext implements StateMachineContext {
    public String batchId;
    public String itemId;
    public boolean processed;
}

try (BatchOrchestrationExecutor<BatchRootContext, ItemContext, BatchStep, String> executor =
         BatchOrchestrationBuilder.<BatchRootContext, ItemContext, BatchStep, String>create("BatchSettlement", BatchStep.class)
             .batchContext(BatchRootContext::new)
             .batchKey((BatchRootContext ctx) -> ctx.batchId)
             .itemKey((ItemContext ctx) -> ctx.itemId)
             .initialState(BatchStep.INGEST)
             .endStates(BatchStep.COMPLETED)
             .itemState(BatchStep.INGEST)
                 .action((ItemContext ctx) -> ctx)
                 .transition(BatchStep.PROCESS)
             .itemState(BatchStep.PROCESS)
                 .action((ItemContext ctx) -> { ctx.processed = true; return ctx; })
                 .barrier(BarrierPolicy.ALL_ITEMS_ARRIVED)
                 .transition(BatchStep.SETTLE)
             .itemState(BatchStep.SETTLE)
                 .action((ItemContext ctx) -> ctx)
                 .transition(BatchStep.COMPLETED)
             .output((BatchRootContext ctx) -> "Batch completed")
             .buildExecutor()) {

    // Process a collection of items with barrier synchronization
    ItemContext item1 = new ItemContext();
    item1.batchId = "BATCH-01";
    item1.itemId = "ITEM-1";

    BatchTurnResult<BatchRootContext, ItemContext, BatchStep, String> result =
            executor.dispatchBatchSync(List.of(item1));
}
```
