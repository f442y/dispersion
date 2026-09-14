# Dispersion Orchestration Batch Module (`dispersion-orchestration-batch`)

The **Dispersion Orchestration Batch Module** provides a turn-based synchronized batch execution engine engineered for **Java 25+ Virtual Threads**. It coordinates bulk workflows where collections of domain items advance through coordinated stages with barrier synchronization.

---

## 1. Capabilities & Barrier Synchronization

* **Item-Level Virtual Threads**: Each item in a batch executes concurrently on its own virtual thread with isolated context mutability.
* **Synchronized Barriers**: Items advance through steps in lockstep based on configurable `BarrierPolicy` rules:
  * **`ALL_ITEMS`**: Every item in the batch must arrive at the barrier before the batch advances to the next step.
  * **`QUORUM`**: The batch advances as soon as a defined quorum threshold ratio (e.g., 80% or 0.8) arrives at the barrier.
* **Granular Checkpointing**: `BatchOrchestrationCheckpoint` records the progress and state of every individual item, allowing interrupted batches to resume without reprocessing completed items.
* **Extensible Telemetry**: Emits `BatchBarrierReachedEvent` and `BatchBarrierUnlockedEvent` directly to the `ExecutionEvent` bus.

---

## 2. Example Usage

```java
import com.github.f442y.dispersion.fsm.context.StateMachineContext;
import com.github.f442y.dispersion.fsm.state.StateKey;
import com.github.f442y.dispersion.orchestration.batch.BarrierPolicy;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationExecutor;
import com.github.f442y.dispersion.orchestration.batch.BatchOrchestrationStateMachineBuilder;

public enum BatchStep implements StateKey {
    INGEST, PROCESS, SETTLE, COMPLETED
}

public static final class ItemContext implements StateMachineContext {
    public String itemId;
    public boolean processed;
}

try (BatchOrchestrationExecutor<ItemContext, BatchStep, String, Boolean> executor =
         BatchOrchestrationStateMachineBuilder.<ItemContext, BatchStep, String, Boolean>create("BatchSettlement", BatchStep.class)
             .context(ItemContext::new)
             .initialState(BatchStep.INGEST)
             .endStates(BatchStep.COMPLETED)
             .barrierPolicy(BarrierPolicy.ALL_ITEMS)
             .state(BatchStep.INGEST)
                 .action((ItemContext ctx) -> ctx)
                 .transition(BatchStep.PROCESS)
             .state(BatchStep.PROCESS)
                 .action((ItemContext ctx) -> { ctx.processed = true; return ctx; })
                 .transition(BatchStep.SETTLE)
             .state(BatchStep.SETTLE)
                 .action((ItemContext ctx) -> ctx)
                 .transition(BatchStep.COMPLETED)
             .output((ItemContext ctx) -> ctx.processed)
             .buildExecutor()) {

    // Process a collection of items with barrier synchronization
    var results = executor.dispatchBatch(List.of("item-1", "item-2", "item-3"));
}
```
