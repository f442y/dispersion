# Virtual Threads & Performance Guide

Dispersion is engineered from the ground up for **Java 25+ Virtual Threads** (Project Loom). This guide explains how Dispersion achieves sub-microsecond state transitions, avoids carrier-thread pinning, optimizes JVM escape analysis, and manages backpressure during high-concurrency bursts.

---

## 1. Project Loom & Virtual Thread Mechanics

### Platform Threads vs. Virtual Threads

Traditional Java workflow frameworks rely on OS-level **Platform Threads**:
* **1:1 Mapping:** Each platform thread maps directly to an operating system kernel thread.
* **Heavyweight:** Consumes ~1 MB of RAM per stack, with expensive kernel-space context switching.
* **Pool Starvation:** When a workflow waits for I/O (network call, database query, external webhook), the OS thread remains blocked and idle, exhausting the thread pool.

Java 25 Virtual Threads introduce **M:N User-Space Scheduling**:
* **M Virtual Threads on N Carrier Threads:** Hundreds of thousands of virtual threads run on a small pool of carrier platform threads (typically matching the number of CPU cores).
* **Lightweight:** Consumes only a few hundred bytes of heap memory per thread.
* **Continuation Suspension:** When a virtual thread encounters blocking operations, the JVM captures its continuation, unmounts it from the carrier thread, and allows the carrier thread to execute another virtual thread immediately.

```
Platform Threads (Traditional):
  [Request 1] ──► [OS Thread 1 (Blocked on I/O)] ──► CPU Stalled
  [Request 2] ──► [OS Thread 2 (Blocked on I/O)] ──► CPU Stalled
  [Request 3] ──► [Thread Pool Exhausted / Queue Backlog]

Virtual Threads in Dispersion:
  [Workflow 1] ──► (Virtual Thread 1) ─┐
  [Workflow 2] ──► (Virtual Thread 2) ─┼──► [Carrier Thread 1 (ForkJoinPool)] ──► 100% CPU Efficiency
  [Workflow N] ──► (Virtual Thread N) ─┘        │
                                                ▼ (When Workflow 1 waits for I/O or signal)
                                     [Workflow 1 Unmounted; Carrier executes Workflow 2]
```

---

## 2. Zero Carrier-Thread Pinning

### The Pinning Problem
A virtual thread is **pinned** to its carrier thread if it performs a blocking operation while holding a monitor lock (`synchronized` block or method) or inside native code (`JNI`). When pinned:
* The carrier thread cannot be released.
* Other virtual threads waiting in the `ForkJoinPool` work queue stall.
* System throughput collapses back to platform thread limitations.

### Dispersion's Pinning Prevention Strategy
1. **Zero `synchronized` on Execution Hot Paths:**
   Dispersion never uses `synchronized` blocks inside its state execution drivers, action runners, or event dispatchers.
2. **Confined Context Mutability:**
   Because a `StateMachineContext` is confined to a single Virtual Thread during each turn, state mutations require **no locking at all**.
3. **`ReentrantLock` & Virtual Thread-Aware Primitives:**
   Where cross-thread synchronization is essential (such as ring-buffer queues or admission controllers), Dispersion relies exclusively on `java.util.concurrent.locks.ReentrantLock` and `Semaphore`. Unlike `synchronized`, these primitives support non-pinning continuation unmounting in Java 25.

---

## 3. Zero-Allocation Hot Paths & JVM C2 Optimization

Dispersion eliminates heap allocations on the state transition hot path through deliberate low-level design:

### 1. Pre-Compiled Ordinal Lookups (`StateMap`)
Traditional state engines look up transitions using `Map<String, State>` or reflection. This causes hash table bucket traversals, object allocations, and cache misses.

Dispersion compiles the state graph during builder configuration into a dense, flat array indexed directly by the `StateKey.ordinal()` integer:

```java
// Fast O(1) array dereference with zero allocation and zero hash computation
TransitionAction action = transitionTable[currentState.ordinal()];
```

### 2. Bitmask Terminal State Checks
Terminal states are encoded into compact primitive bitmasks. Checking if an execution has finished requires a single bitwise operation (`(terminalMask & (1L << state.ordinal())) != 0`), executing in a single CPU clock cycle.

### 3. Escape Analysis & Scalar Replacement
In Java 25, the HotSpot C2 compiler performs aggressive escape analysis:
* Short-lived records (such as `TurnStartedEvent`, `CommandEnvelope`, or small DTO inputs) that do not escape the local calling thread are **scalar-replaced**.
* Their fields are allocated directly to CPU registers or the thread stack rather than the JVM heap.
* This drastically reduces young-generation GC churn, enabling hundreds of thousands of state transitions per second without trigger GC pauses.

---

## 4. Adaptive Admission Control & High-Throughput Bursts

While Virtual Threads are cheap, unbounded thread spawning under massive traffic spikes can overwhelm downstream services (databases, payment gateways, microservices).

Dispersion provides an adaptive `AdmissionController` to govern concurrency:

```java
package com.example.performance;

import com.github.f442y.dispersion.fsm.config.StateMachineConfiguration;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineBuilder;
import com.github.f442y.dispersion.fsm.core.atomic.AtomicStateMachineExecutor;
import com.github.f442y.dispersion.fsm.test.TestStateContext;
import com.github.f442y.dispersion.fsm.test.TestStateKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AdmissionControlExample {

    private static final Logger log = LoggerFactory.getLogger(AdmissionControlExample.class);

    public void configureAdmission() {
        StateMachineConfiguration<TestStateContext, TestStateKey, Integer, Integer> config =
            AtomicStateMachineBuilder.<TestStateContext, TestStateKey, Integer, Integer>create("TunedEngine", TestStateKey.class)
                .context(TestStateContext::new)
                .initialState(TestStateKey.STATE_A)
                .endStates(TestStateKey.STATE_B)
                .input((TestStateContext ctx, Integer val) -> { ctx.put("v", val); return ctx; })
                .state(TestStateKey.STATE_A)
                    .action((TestStateContext ctx) -> ctx)
                    .transition(TestStateKey.STATE_B)
                .output((TestStateContext ctx) -> ctx.<Integer>get("v"))
                .build();

        // Configure executor with max 1,000 concurrent Virtual Threads
        try (AtomicStateMachineExecutor<TestStateContext, TestStateKey, Integer, Integer> executor =
                 new AtomicStateMachineExecutor<>("tuned-executor", config, 1_000)) {

            log.atInfo()
               .addKeyValue("capacity", 1_000)
               .log("Executor initialized with virtual thread admission control");
        }
    }
}
```

### Admission Policies Under Load

| Policy | Behavior | Best Used For |
| :--- | :--- | :--- |
| **`BLOCK`** | Automatically pauses the requesting virtual thread until execution capacity is released. | High-throughput ingestion pipelines where backpressure must push back to the caller. |
| **`REJECT_IMMEDIATELY`** | Instantly throws `CapacityExceededException` without blocking. | Low-latency APIs with strict SLA timeouts (e.g. trading, real-time bidding). |
| **`WAIT_WITH_TIMEOUT`** | Waits up to a designated timeout duration; throws if capacity is not released within the window. | Resilient microservices balancing burst tolerance with timeout SLAs. |

---

## 5. Performance Tuning Best Practices

1. **Keep Contexts Thread-Confined:**
   Never share a `StateMachineContext` instance across multiple concurrent workflow executions. Always supply a factory (`context(MyContext::new)`).
2. **Offload Heavy I/O to Carrier-Friendly Clients:**
   Use non-blocking or virtual-thread-compatible HTTP clients (e.g., `java.net.http.HttpClient`) and JDBC/R2DBC drivers that avoid monitor lock pinning.
3. **Tune Event Bus Buffer Capacity:**
   For telemetry-heavy systems, configure `VirtualThreadEventBus` with a buffer sized for expected burst volumes (e.g. 50,000) and choose an appropriate `OverflowPolicy` (`DROP_OLDEST` for metrics, `BLOCK` for strict auditing).
