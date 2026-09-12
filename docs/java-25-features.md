# Java 25+ Language Features in Action

Dispersion is built specifically for **Java 25+**, taking full advantage of modern language capabilities to provide maximum performance, strict type safety, and clean API ergonomics.

---

## 1. Virtual Threads (Project Loom)

In traditional thread-per-request systems, blocking an OS thread during network I/O or wait states consumes 1–2 MB of stack memory and rapidly exhausts thread pools.

Dispersion leverages Java Virtual Threads (`Thread.ofVirtual()`):
- **Cheap Concurrency**: Spin up hundreds of thousands of concurrent state machine workflows with sub-millisecond dispatch overhead.
- **Thread Confinement**: Context mutations remain lock-free and thread-safe because each turn is confined to a single virtual thread.
- **Synchronous Simplicity**: Write straightforward imperative code (e.g. blocking database calls or message publishing) without nested reactive callback chains (`CompletableFuture`, `Mono`, `Flux`).

---

## 2. Exhaustive Pattern Matching on Sealed Hierarchies

Dispersion uses `sealed` interfaces and classes to strictly control subtype hierarchies. This empowers the Java compiler to guarantee **exhaustive pattern matching** at compile time without requiring fallback `default:` branches.

### Sealed Exceptions
`StateMachineException` is a sealed class that permits only specific error types:

```mermaid
classDiagram
    class StateMachineException {
        <<sealed class>>
    }
    class ActionException
    class TransitionException
    class BackpressureException
    class MaxTransitionsExceededException
    class MaxStateVisitsExceededException
    class CompensationException

    StateMachineException <|-- ActionException
    StateMachineException <|-- TransitionException
    StateMachineException <|-- BackpressureException
    StateMachineException <|-- MaxTransitionsExceededException
    StateMachineException <|-- MaxStateVisitsExceededException
    StateMachineException <|-- CompensationException
```

```java
import com.github.f442y.dispersion.fsm.exception.*;

try {
    executor.dispatchSync(input);
} catch (StateMachineException ex) {
    // Compile-time exhaustive switch: no default branch required!
    String diagnostic = switch (ex) {
        case ActionException ae ->
            "Action failed in state [" + ae.getStateName() + "]: " + ae.getCause().getMessage();
        case TransitionException te ->
            "Invalid transition from [" + te.getSourceStateName() + "] to [" + te.getTargetStateName() + "]";
        case BackpressureException be ->
            "Admission backpressure: " + be.getMessage();
        case MaxTransitionsExceededException mte ->
            "Global transition limit reached: " + mte.getMaxTransitions();
        case MaxStateVisitsExceededException msve ->
            "Max visits exceeded for state [" + msve.getStateName() + "]";
        case CompensationException ce ->
            "Saga rollback error in state [" + ce.getStateName() + "]";
    };
    logger.error(diagnostic, ex);
}
```

If a new exception subtype is added to `StateMachineException` in future releases, the compiler will flag any non-exhaustive switch statements at build time.

---

## 3. Sealed Records & Telemetry Matching

Dispersion's `ExecutionEvent` sealed interface defines 12 immutable record variants. You can consume lifecycle telemetry with type-safe pattern matching:

```java
import com.github.f442y.dispersion.event.ExecutionEvent;

public void processTelemetry(ExecutionEvent event) {
    switch (event) {
        case ExecutionEvent.TurnStartedEvent started ->
            System.out.println("Started turn on " + started.machineName());

        case ExecutionEvent.TurnSuspendedEvent suspended ->
            System.out.printf("Suspended at %s awaiting signal %s (key: %s)%n",
                    suspended.stateName(), suspended.expectedSignal(), suspended.correlationKey());

        case ExecutionEvent.TurnCompletedEvent completed ->
            System.out.println("Completed turn at " + completed.finalStateName());

        case ExecutionEvent.TurnCompensatedEvent compensated ->
            System.err.println("Compensated turn after failure in " + compensated.failedStateName() + ": " + compensated.cause().getMessage());

        case ExecutionEvent.TurnFailedEvent failed ->
            System.err.println("Turn failed at " + failed.failedStateName() + ": " + failed.cause().getMessage());

        case ExecutionEvent.StateEnteredEvent entered ->
            System.out.println("Entered state: " + entered.stateName());

        case ExecutionEvent.StateExitedEvent exited ->
            System.out.println("Exited state: " + exited.stateName() + " in " + exited.duration().toMillis() + "ms");

        case ExecutionEvent.TransitionEvaluatedEvent transition ->
            System.out.println("Transition: " + transition.sourceState() + " -> " + transition.targetState());

        case ExecutionEvent.SignalAwaitedEvent awaited ->
            System.out.println("Awaiting signal: " + awaited.expectedSignal());

        case ExecutionEvent.SignalDeliveredEvent delivered ->
            System.out.println("Delivered signal: " + delivered.signalName());

        case ExecutionEvent.CommandDeduplicatedEvent dedup ->
            System.out.println("Deduplicated command: " + dedup.commandId());

        case ExecutionEvent.BatchBarrierReachedEvent barrier ->
            System.out.println("Item " + barrier.itemKey() + " reached barrier at " + barrier.stateName());

        case ExecutionEvent.BatchBarrierUnlockedEvent unlocked ->
            System.out.println("Barrier unlocked at " + unlocked.stateName() + " for " + unlocked.itemCount() + " items");
    }
}
```

---

## 4. Nested Record Pattern Deconstruction

Dispersion makes heavy use of records (`CommandEnvelope`, `SignalDeliveryResult`, `ExecutionSummary`, `MachineDescriptor`). With record patterns, you can deconstruct nested data structures directly in conditional statements:

```java
import com.github.f442y.dispersion.orchestration.command.CommandEnvelope;
import java.time.Instant;
import java.util.UUID;

if (envelope instanceof CommandEnvelope(UUID id, Instant ts, PaymentSignal(String orderId, double amount))) {
    System.out.printf("Command %s processed payment for order %s: $%.2f%n", id, orderId, amount);
}
```

---

## 5. Null Safety with JSpecify Annotations

Dispersion applies `@NullMarked` and `@Nullable` annotations across all public API modules. This ensures:
- Strict IDE and compiler static analysis checks against `NullPointerException`.
- Seamless interoperability with Kotlin and modern Java static analyzers (e.g. NullAway, SpotBugs).

---

## 6. JPMS Modular Encapsulation

Dispersion declares explicit `Automatic-Module-Name` headers across all jars in its `pom.xml` manifests, enabling clean `module-info.java` consumption in modular Java 25 applications with zero package overlap:

```java
module com.example.myapp {
    requires com.github.f442y.dispersion.fsm.core;
    requires com.github.f442y.dispersion.orchestration.core;
    requires com.github.f442y.dispersion.control.core;
}
```
