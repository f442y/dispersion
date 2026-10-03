# Dispersion Interactive Demo Application (`DispersionDemoApp`)

> **Location:** [`examples/src/main/java/com/github/f442y/dispersion/examples/DispersionDemoApp.java`](file:///C:/Users/faiza/development/dispersion/examples/src/main/java/com/github/f442y/dispersion/examples/DispersionDemoApp.java)
> **Server Runtime:** Spring Boot 4.1 + Jakarta REST 3.1 (Jersey) on Java 25 Virtual Threads
> **Default Port:** `8080` (API Base Path: `/api/v1`, Web Console: `/`)
> **Java Version:** OpenJDK 25+ (Loom Virtual Threads)

The **Dispersion Demo Application** is a self-contained, interactive environment designed for local development, API exploration, and end-to-end integration testing. It launches an embedded virtual-thread HTTP and Server-Sent Events (SSE) server, embeds and serves the compiled React 19 UI dashboard directly at root (`/`), and registers three state machine engines directly into the [DefaultControlPlane](file:///C:/Users/faiza/development/dispersion/control/core/src/main/java/com/github/f442y/dispersion/control/core/DefaultControlPlane.java):

1. **`OrderWorkflow` (Tier 2 Orchestration Saga)**:
   - Validates orders, reserves inventory with registered compensating rollbacks, and suspends turn execution at `AWAIT_PAYMENT_SIGNAL`.
   - Automatically pre-seeds a suspended order (`ORDER-DEMO-99`) awaiting external payment signal injection.
2. **`MetricsPipeline` (Tier 1 Atomic FSM)**:
   - High-throughput atomic pipeline processing metric sensor bursts (`INGEST` &rarr; `ENRICH` &rarr; `TRANSFORM` &rarr; `EXPORT` &rarr; `FINISHED`).
   - Generates background virtual-thread bursts every 500ms to drive live Server-Sent Events (SSE).
3. **`BatchProcessor` (Tier 3 Batch Orchestration)**:
   - Turn-based item processing with barrier synchronization (`ALL_ITEMS_ARRIVED` and `SIGNAL_TRIGGERED`).

---

## 🚀 How to Run the Demo

### Option 1: Run via Spring Boot Maven Plugin (Recommended)
From the repository root:

- **Windows (PowerShell / CMD):**
  ```powershell
  .\mvnw spring-boot:run -pl examples
  ```

- **Linux / macOS (Bash / Zsh):**
  ```bash
  ./mvnw spring-boot:run -pl examples
  ```

### Option 2: Run in IntelliJ IDEA
1. Open the project in IntelliJ IDEA (ensure Java 25 SDK is configured in Project Structure).
2. Navigate to [`examples/src/main/java/com/github/f442y/dispersion/examples/DispersionDemoApp.java`](file:///C:/Users/faiza/development/dispersion/examples/src/main/java/com/github/f442y/dispersion/examples/DispersionDemoApp.java).
3. Right-click the file or click the green run arrow next to `public static void main(String[] args)`.
4. Click **Run 'DispersionDemoApp.main()'**.

When started, the application serves the Web Dashboard SPA at `http://localhost:8080/` and displays an interactive banner confirming the server is listening:

```text
========================================================================================
🚀 DISPERSION CONTROL PLANE & SPRING BOOT 4.1 DEMO IS RUNNING
========================================================================================
Base HTTP URL: http://localhost:8080/api/v1
Web Landing:   http://localhost:8080

✨ Pre-registered Machines:
   • OrderWorkflow    (Orchestration Saga: Suspended order 'ORDER-DEMO-99' awaiting signal)
   • MetricsPipeline  (Atomic FSM: Emitting continuous background events)
   • BatchProcessor   (Turn-based item batch with quorum barrier)
...
```

---

## 📡 Universal HTTP Commands (Bash, Zsh, PowerShell, Windows CMD)

Below are cross-platform commands formatted to work consistently across shells (Bash, Zsh, Windows PowerShell, PowerShell Core, and Windows Command Prompt).

> [!NOTE]
> On Windows PowerShell, the command `curl` is often an alias for `Invoke-WebRequest`. To invoke the real `curl` binary on Windows, use `curl.exe` or call the native PowerShell command `Invoke-RestMethod` shown below.

---

### 1. Node Health & Diagnostics
Retrieve operational node health, CPU cores, JVM version, uptime, and active machine counts:

#### Universal `curl`:
```bash
curl -s "http://localhost:8080/api/v1/node"
```

#### Windows PowerShell:
```powershell
curl.exe -s "http://localhost:8080/api/v1/node"
```

---

### 2. List Registered State Machines
Retrieve descriptors for all state machines registered in the control plane:

#### Universal `curl`:
```bash
curl -s "http://localhost:8080/api/v1/machines"
```

#### Windows PowerShell:
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/api/v1/machines" | ConvertTo-Json -Depth 3
```

**Example JSON Response:**
```json
[
  {
    "name": "OrderWorkflow",
    "type": "ORCHESTRATION",
    "initialState": "VALIDATE_ORDER",
    "endStates": ["COMPLETED", "CANCELLED"],
    "allStates": ["VALIDATE_ORDER", "RESERVE_INVENTORY", "AWAIT_PAYMENT_SIGNAL", "DISPATCH_SHIPMENT", "COMPLETED", "CANCELLED"]
  },
  {
    "name": "MetricsPipeline",
    "type": "ATOMIC",
    "initialState": "INGEST",
    "endStates": ["FINISHED"],
    "allStates": ["INGEST", "ENRICH", "TRANSFORM", "EXPORT", "FINISHED"]
  },
  {
    "name": "BatchProcessor",
    "type": "BATCH",
    "initialState": "IMPORT",
    "endStates": ["COMPLETE"],
    "allStates": ["IMPORT", "RUN_CHECKS", "AWAIT_TRIGGER", "COMPLETE"]
  }
]
```

---

### 3. Inspect Machine Topology & Mermaid Diagram
Fetch detailed state transitions and generated Mermaid state diagram graph for `OrderWorkflow`:

#### Universal `curl`:
```bash
curl -s "http://localhost:8080/api/v1/machines/OrderWorkflow"
```

#### Windows PowerShell:
```powershell
(Invoke-RestMethod -Uri "http://localhost:8080/api/v1/machines/OrderWorkflow").mermaidGraph
```

*Or with `jq` in Bash:*
```bash
curl -s "http://localhost:8080/api/v1/machines/OrderWorkflow" | jq -r '.mermaidGraph'
```

---

### 4. Dispatch State Machine Execution Over HTTP
Synchronously dispatch an execution of `MetricsPipeline` or `OrderWorkflow`:

#### Universal `curl`:
```bash
# Dispatch MetricsPipeline with input value 42.0 (calculates 42.0 * 1.25 = 52.5)
curl -X POST "http://localhost:8080/api/v1/machines/MetricsPipeline/dispatch" \
  -H "Content-Type: application/json" \
  -d "42.0"
```

**Example Response:**
```json
{"status":"DISPATCHED","machine":"MetricsPipeline","result":52.5}
```

---

### 5. Query Suspended Executions
List workflows currently paused at a signal wait step (such as pre-seeded `ORDER-DEMO-99`):

#### Universal `curl`:
```bash
curl -s "http://localhost:8080/api/v1/executions?status=SUSPENDED"
```

#### Windows PowerShell:
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/api/v1/executions?status=SUSPENDED" | ConvertTo-Json
```

---

### 6. Inspect Suspended Workflow Checkpoint
Inspect the durable checkpoint state snapshot for a suspended execution:

#### Universal `curl`:
```bash
curl -s "http://localhost:8080/api/v1/executions/OrderWorkflow/ORDER-DEMO-99/checkpoint"
```

#### Windows PowerShell:
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/api/v1/executions/OrderWorkflow/ORDER-DEMO-99/checkpoint" | ConvertTo-Json
```

**Example Response:**
```json
{
  "machineId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "machineName": "OrderWorkflow",
  "status": "SUSPENDED",
  "currentStateKey": "AWAIT_PAYMENT_SIGNAL",
  "correlationKey": "ORDER-DEMO-99",
  "expectedSignal": "PaymentSignal",
  "timestamp": "2026-10-03T20:00:00.000Z"
}
```

---

### 7. Resume Suspended Workflow via Signal Delivery
Deliver a `PaymentSignal` to resume the suspended order `ORDER-DEMO-99`. The orchestration engine wakes up on a virtual thread, transitions through `DISPATCH_SHIPMENT`, and completes:

#### Universal `curl`:
```bash
curl -X POST "http://localhost:8080/api/v1/executions/signal" \
  -H "Content-Type: application/json" \
  -d "{\"machineName\":\"OrderWorkflow\",\"correlationKey\":\"ORDER-DEMO-99\",\"signalName\":\"PaymentSignal\",\"payload\":{\"correlationKey\":\"ORDER-DEMO-99\",\"paymentMethod\":\"APPLE_PAY\",\"amountCents\":9995}}"
```

#### Windows PowerShell:
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/v1/executions/signal" `
  -ContentType "application/json" `
  -Body '{"machineName":"OrderWorkflow","correlationKey":"ORDER-DEMO-99","signalName":"PaymentSignal","payload":{"correlationKey":"ORDER-DEMO-99","paymentMethod":"APPLE_PAY","amountCents":9995}}'
```

---

### 8. Stream Real-Time Telemetry Events (SSE)
Stream live Server-Sent Events over HTTP. Supports two-tier telemetry filtering:

#### Stream Default Lifecycle Events (`tier=lifecycle`):
```bash
curl -N "http://localhost:8080/api/v1/events/stream"
```

#### Stream All Granular Events with Dynamic Tap (`tier=all`):
```bash
curl -N "http://localhost:8080/api/v1/events/stream?machine=MetricsPipeline&tier=all"
```

---

## 🧪 Automated End-to-End Tests

The demo application's behavior and HTTP endpoints are tested end-to-end in [`DispersionDemoAppIntegrationTests.java`](file:///C:/Users/faiza/development/dispersion/examples/src/test/java/com/github/f442y/dispersion/examples/DispersionDemoAppIntegrationTests.java).

Run the tests at any time via:

- **Windows PowerShell:**
  ```powershell
  .\mvnw test -pl examples -am
  ```

- **Linux / macOS:**
  ```bash
  ./mvnw test -pl examples -am
  ```
