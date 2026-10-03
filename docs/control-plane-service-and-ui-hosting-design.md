# Architectural Design: Dedicated Environment Control Plane Service & Embedded UI Hosting

> **Document Status:** Active Architectural Specification & Implementation Reference
> **Author:** Dispersion Architecture Working Group
> **Target Release:** Dispersion 0.1.0+ (Phase 4 Milestone)
> **Primary Module Impacts:** `dispersion-server-jakarta`, `dispersion-server-api`, `dispersion-control-plane-core`, `dispersion-control-plane-api`, `ui/`, `examples/`
> **Repository Baseline:** Java 25 Virtual Threads, Jakarta REST 3.1 (Spring Boot 4.1 / Jersey), Node.js 24, Vite 8, React 19, Avaje JSON, Fury Binary.

---

## 1. Executive Summary & Problem Formulation

### 1.1 The Operational Challenge
In distributed state engine and saga orchestration architectures, there are two distinct traffic profiles:
1. **Data Plane:** Ultra-high throughput, sub-microsecond or turn-based command execution across worker nodes running state machines, sagas, and parallel batch collections.
2. **Control Plane:** Human operators, platform engineers, and automated SRE tooling monitoring live workflows, inspecting audit timelines, visualizing state machine graphs, investigating suspended checkpoints, and dispatching manual or webhook signals.

A critical design question emerges: **How should the Web Dashboard ([`ui/`](../ui/README.md)) connect to and interact with the orchestration fleet in production environments?**

### 1.2 Evaluation of Candidate Topologies

```
Topology A: Direct Browser-to-Worker (P2P) [REJECTED]
[Browser / UI] ──❌──> [Worker Pod 1 (IP: 10.0.1.24)]
               ──❌──> [Worker Pod 2 (IP: 10.0.2.89)]
               ──❌──> [Worker Pod N (...)]

Topology B: External CDN + Split Backend Ingress [COMPLEX / OPTIONAL]
[Browser / UI] ──> [Cloudflare CDN / S3 Bucket]
       │
       └── CORS ──> [API Gateway] ──> [Control Plane Service]

Topology C: Dedicated Control Plane Service with Embedded UI Hosting [IMPLEMENTED]
[Browser / UI] ── Same-Origin (Port 8080/443) ──> [Dispersion Control Plane (Jakarta REST 3.1)]
                                                         │
                                    ┌────────────────────┴────────────────────┐
                                    ▼                                         ▼
                        [Shared CheckpointStore]                  [Telemetry Bus / Workers]
```

### 1.3 Why Direct Browser-to-Worker Fails in Production
* **VPC Ingress & Ephemeral IP Addresses:** In containerized Kubernetes/ECS environments, worker pods run in private VPC subnets with dynamically changing pod IPs and no public ingress. A browser cannot route to internal pod IPs.
* **Split-Brain Audit Timelines:** Sagas move across nodes. Turn 1 may execute on Worker A, suspend waiting for external payment, and resume hours later on Worker B. No single worker node holds the global timeline.
* **Cold Checkpoint Invisibility:** When a saga suspends at `waitForSignal`, it unmounts from virtual threads and is saved into the durable `CheckpointStore`. It is **not in memory on any worker**. A browser polling worker nodes will fail to find suspended executions.
* **SSE Connection Thrashing & CORS Sprawl:** Establishing and maintaining persistent Server-Sent Events (SSE) connections across $N$ dynamically autoscaling worker nodes leads to connection storms, complex pre-flight (`OPTIONS`) handshakes, and CORS security risks.

### 1.4 The Implemented Solution
Deploy a **Dedicated Dispersion Control Plane Service** per environment (`control-plane-staging`, `control-plane-prod`) powered by `dispersion-server-jakarta` (portable Jakarta REST 3.1 and `WebDashboardResource` on Java 25 virtual threads). The server **natively embeds and hosts the compiled React 19 / Vite UI bundle**, providing a turnkey, zero-CORS, single-container operational console for each deployment environment.

---

## 2. End-to-End System Architecture

```mermaid
graph TD
    subgraph BrowserClient["Operator Workplace / Browser"]
        BROWSER["Web Dashboard<br/>(React 19, TanStack Router, Mermaid.js)"]
    end

    subgraph EnvPerimeter["Environment Perimeter (e.g. Staging / Production VPC)"]
        INGRESS["TLS Ingress / Load Balancer<br/><code>https://control-plane.prod.internal</code>"]

        subgraph ControlPlaneService["Dedicated Control Plane Service (dispersion-server-jakarta)"]
            STATIC_ROUTER["WebDashboardResource<br/>Virtual-Thread Static & SPA Handler"]
            UI_ASSETS["Embedded UI Assets (web/ or static/)<br/>MIME Resolution + SPA Fallback + Traversal Defense"]
            REST_API["ControlPlaneResource<br/>REST Endpoints (/api/v1/*)"]
            CONFIG_EP["Node Diagnostics Endpoint<br/>GET /api/v1/node"]
            SSE_HUB["SseEventStreamHandler<br/>Two-Tier Telemetry (Lifecycle / Granular)"]
            QUERY_ENG["Store-Backed Query Engine<br/>(inspectCheckpoint / CheckpointStore SPI)"]
            SIGNAL_INGRESS["External Signal Ingress Router<br/>(Idempotent Command Dispatcher)"]
            MEM_POOL["Dual-Pool Ring Buffers<br/>(Active Hash Map + Bounded Terminal Pool)"]
        end

        subgraph SharedInfra["Shared Environmental Infrastructure"]
            STORE[("Shared Durable CheckpointStore<br/>(PostgreSQL / JDBC / Redis)")]
            BROKER["Telemetry & Signal Broker<br/>(Kafka / NATS / RabbitMQ / RingBuffer)"]
        end

        subgraph WorkerFleet["Distributed Worker Fleet (Data Plane)"]
            W1["Worker Node 1<br/>(OrderSaga Turn 1)"]
            W2["Worker Node 2<br/>(PaymentFSM Worker)"]
            W3["Worker Node 3<br/>(InventoryBatch Engine)"]
        end
    end

    BROWSER -->|HTTPS GET / (HTML/JS/CSS)| INGRESS
    BROWSER -->|HTTPS GET /api/v1/* (REST)| INGRESS
    BROWSER -->|HTTPS GET /api/v1/events/stream (SSE)| INGRESS
    BROWSER -->|HTTPS POST /api/v1/executions/signal| INGRESS

    INGRESS --> STATIC_ROUTER
    STATIC_ROUTER --> UI_ASSETS
    INGRESS --> REST_API
    REST_API --> CONFIG_EP
    REST_API --> SSE_HUB
    REST_API --> QUERY_ENG
    REST_API --> SIGNAL_INGRESS

    QUERY_ENG <-->|Inspect Suspended Workflows| STORE
    BROKER -.->|Ingest Events| MEM_POOL
    SSE_HUB --> MEM_POOL

    W1 & W2 & W3 -.->|Persist Checkpoints on Suspension| STORE
    W1 & W2 & W3 -.->|Publish Polymorphic ExecutionEvents| BROKER
    BROKER -.->|Deliver Resume Commands| W1 & W2 & W3
```

---

## 3. Detailed Component Designs

### 3.1 Component 1: Embedded UI Hosting via `WebDashboardResource`

#### 3.1.1 Portable Virtual-Thread Static Routing Architecture
`dispersion-server-jakarta` provides the framework-agnostic `WebDashboardResource` implementing JAX-RS 3.1 file serving directly on virtual threads. It streams static assets (`.js`, `.css`, `.svg`, `.woff2`) with immutable cache headers and provides seamless SPA client-side routing fallback without requiring external web servers like Nginx.

```
Incoming HTTP Request
        │
        ▼
Is path prefixed with /api/v1/* ?
 ├─ YES ──> Dispatch to ControlPlaneResource (/api/v1/*)
 └─ NO  ──> Check path traversal guard (reject .., \, null bytes with 400 Bad Request)
              │
              ▼
            Check if static resource exists in classpath (web/{path} or static/{path})
              ├─ YES ──> Stream file with content-type and Cache-Control (immutable)
              └─ NO  ──> Path contains '.' (missing asset)?
                          ├─ YES ──> Return 404 Not Found JSON
                          └─ NO  ──> SPA Navigation Fallback: Stream index.html (200 OK, no-cache)
```

#### 3.1.2 Production Static Routing Handler & Traversal Defense
In `dispersion-server-jakarta`, `WebDashboardResource` incorporates automated MIME resolution, cache headers, and single-page routing:

```java
package com.github.f442y.dispersion.server.jakarta;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jspecify.annotations.NonNull;

import java.io.InputStream;
import java.nio.file.Paths;

@Path("/")
public class WebDashboardResource {

    private static final String CLASSPATH_PREFIX = "web/";
    private static final String FALLBACK_PREFIX = "static/";
    private static final String INDEX_HTML = "index.html";

    @GET
    @Produces(MediaType.WILDCARD)
    public Response getRoot() {
        return serveResource(INDEX_HTML);
    }

    @GET
    @Path("{path: [^?#]+}")
    @Produces(MediaType.WILDCARD)
    public Response getPath(@PathParam("path") @NonNull String path) {
        if (path.startsWith("api/") || path.equals("api")) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        if (isPathTraversal(path)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\": \"Invalid path traversal attempt\"}")
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

        return serveResource(path);
    }

    private static boolean isPathTraversal(@NonNull String path) {
        if (path.contains("..") || path.contains("\\") || path.indexOf('\0') != -1 || path.startsWith("/")) {
            return true;
        }
        try {
            java.nio.file.Path normalized = Paths.get(path).normalize();
            if (normalized.startsWith("..") || normalized.isAbsolute()) {
                return true;
            }
        } catch (Exception e) {
            return true;
        }
        return false;
    }
}
```

---

### 3.2 Component 2: Dedicated Control Plane Service Functions

The dedicated control plane service provides four foundational capabilities that worker nodes cannot satisfy individually:

```
┌────────────────────────────────────────────────────────────────────────┐
│            Dispersion Dedicated Control Plane Microservice             │
│                                                                        │
│   ┌───────────────────────────┐     ┌──────────────────────────────┐   │
│   │ 1. Checkpoint Discovery   │     │ 2. Telemetry Aggregation     │   │
│   │ • Queries CheckpointStore │     │ • RingBuffer + Broker Ingest │   │
│   │ • Finds Cold Suspended    │     │ • SSE Streaming Fanout       │   │
│   │ • Timeline Reconstruction │     │ • Dynamic Ephemeral Taps     │   │
│   └───────────────────────────┘     └──────────────────────────────┘   │
│   ┌───────────────────────────┐     ┌──────────────────────────────┐   │
│   │ 3. Signal Router & Ingress│     │ 4. Node & Topology Catalog   │   │
│   │ • POST /executions/signal │     │ • Aggregates Machine Graphs  │   │
│   │ • Worker Resolution       │     │ • Dynamic Mermaid Generation │   │
│   │ • Idempotent Dispatch     │     │ • Health & Uptime Metrics    │   │
│   └───────────────────────────┘     └──────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────┘
```

#### 3.2.1 Cluster-Wide Checkpoint Discovery (`inspectCheckpoint` SPI)
When sagas suspend on worker nodes, they save their state snapshot to the `CheckpointStore` and free their virtual threads.
* **Cold Inspection:** When the operator navigates to `/executions?status=SUSPENDED`, the Control Plane does **not** scan worker JVMs. Instead, it queries the shared `CheckpointStore` directly.
* **Timeline Reconstruction:** When viewing an execution's detail drawer, the control plane stitches together in-memory active turn events with persisted checkpoint audit logs via `TraceTimelineProvider` to present a seamless timeline from inception to suspension.

#### 3.2.2 Global Telemetry Aggregation & Two-Tier SSE Streaming
Worker nodes emit polymorphic `ExecutionEvent` records over a fast transport (in-process ring buffer for monoliths, or distributed messaging such as Kafka/NATS for clustered setups).
* **Two-Tier Event Model:**
  * `tier=lifecycle` (Default): Streams state changes, starts, completions, suspensions, cancellations, and failures. Consumes negligible network bandwidth.
  * `tier=all`: Automatically registers a dynamic tap with `DynamicTapManager` for fine-grained internal actions and transition evaluations. Unsubscribing immediately disarms the tap.
* **Non-Blocking SSE Streaming:** The `/api/v1/events/stream` handler registers connected browser clients. When events arrive, they are serialized using compile-time reflection-free Avaje JSON codecs and broadcasted concurrently across virtual threads.
* **Heartbeat Keep-Alive:** Emits an SSE heartbeat comment frame (`: ping\n\n`) every 15 seconds to prevent corporate firewalls and ingress proxies (ALB/Nginx) from terminating idle SSE connections, while renewing client tap leases.

#### 3.2.3 Centralized External Signal Ingress & Dispatch Sequence

```mermaid
sequenceDiagram
    autonumber
    actor Operator as Operator (Web Dashboard)
    participant CP as Control Plane Service
    participant Store as CheckpointStore (DB)
    participant Broker as Message Broker / Router
    participant Worker as Worker Node (Data Plane)

    Operator->>CP: POST /api/v1/executions/signal<br/>{executionId, signalName, payload, idempotencyKey}
    CP->>Store: SELECT snapshot WHERE id = executionId
    Store-->>CP: ExecutionSnapshot (Status: SUSPENDED)

    alt Saga is not suspended
        CP-->>Operator: HTTP 409 Conflict ("Execution is not suspended")
    else Saga is suspended
        CP->>Broker: Publish CommandEnvelope(ResumeSignal, executionId, idempotencyKey)
        CP-->>Operator: HTTP 200 OK ({ "delivered": true, "status": "RESUMED" })
        Broker->>Worker: Deliver ResumeSignal
        Worker->>Store: Acquire Exclusive Execution Lease
        Worker->>Worker: Mount Saga Turn on Virtual Thread
        Worker->>Store: Commit Next Turn State Snapshot
        Worker->>Broker: Publish TurnCompleted ExecutionEvent
        Broker->>CP: Ingest TurnCompleted Event
        CP->>Operator: SSE Broadcast (Execution updated -> TanStack Query invalidates cache)
    end
```

---

## 4. Build, Packaging & Container Pipeline (Node 24 + Java 25)

To deliver a turnkey single-binary deployment without requiring developers to manually build frontend and backend projects separately, the build lifecycle is unified.

### 4.1 Maven Reactor Integration
In `examples/pom.xml`, the `frontend-maven-plugin` builds the React UI during packaging:

```xml
<plugin>
    <groupId>com.github.eirslett</groupId>
    <artifactId>frontend-maven-plugin</artifactId>
    <version>1.15.1</version>
    <configuration>
        <workingDirectory>${project.basedir}/../ui</workingDirectory>
        <nodeVersion>v24.13.0</nodeVersion>
        <npmVersion>11.6.2</npmVersion>
    </configuration>
    <executions>
        <execution>
            <id>install-node-and-npm</id>
            <goals><goal>install-node-and-npm</goal></goals>
            <phase>generate-resources</phase>
        </execution>
        <execution>
            <id>npm-install</id>
            <goals><goal>npm</goal></goals>
            <phase>generate-resources</phase>
            <configuration><arguments>ci</arguments></configuration>
        </execution>
        <execution>
            <id>npm-build</id>
            <goals><goal>npm</goal></goals>
            <phase>generate-resources</phase>
            <configuration><arguments>run build</arguments></configuration>
        </execution>
    </executions>
</plugin>
```

### 4.2 Production Multi-Stage Dockerfile Specification
For containerized deployments, a clean 3-stage Dockerfile produces an ultra-lean (< 120MB) runtime image:

```dockerfile
# ==============================================================================
# Stage 1: Build Frontend Dashboard with Node 24
# ==============================================================================
FROM node:24-alpine AS ui-builder
WORKDIR /build/ui
COPY ui/package*.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# ==============================================================================
# Stage 2: Build Spring Boot 4.1 / Jakarta Server with Java 25 & Maven
# ==============================================================================
FROM maven:3.9-eclipse-temurin-25-alpine AS backend-builder
WORKDIR /build
COPY pom.xml ./
COPY bom/ ./bom/
COPY event/ ./event/
COPY fsm/ ./fsm/
COPY routing/ ./routing/
COPY orchestration/ ./orchestration/
COPY control-plane/ ./control-plane/
COPY serialization/ ./serialization/
COPY server/ ./server/
COPY testkit/ ./testkit/
COPY examples/ ./examples/

# Inject compiled UI bundle from Stage 1 into classpath resources
COPY --from=ui-builder /build/ui/dist ./examples/src/main/resources/web/

# Compile and package executable server JAR
RUN mvn clean package -pl examples -am -DskipTests

# ==============================================================================
# Stage 3: Minimal Production Container
# ==============================================================================
FROM eclipse-temurin:25-jre-alpine AS runtime
WORKDIR /app

# Non-root security user
RUN addgroup -S dispersion && adduser -S dispersion -G dispersion
USER dispersion

COPY --from=backend-builder /build/examples/target/dispersion-examples-*.jar ./dispersion-control-plane.jar

EXPOSE 8080

ENTRYPOINT ["java", \
  "--enable-preview", \
  "-XX:+UseZGC", \
  "-XX:+ZGenerational", \
  "-jar", "dispersion-control-plane.jar"]
```

---

## 5. Architectural Features & Operational Invariants

### 1. Two-Tier Telemetry & Dynamic Tap Manager
High-frequency events (`TransitionEvaluatedEvent`, `ActionExecutedEvent`) are stored in a local circular buffer (`LocalExecutionTraceBuffer`). Only coarse lifecycle events are pushed to the global event stream unless a client requests `tier=all`, which dynamically activates a tap with automatic reference-counted cleanup on disconnect.

### 2. Path Traversal & Static Asset Security
The embedded UI handler validates all path parameters against directory climbing, backslashes, null bytes, and non-canonical relative paths, returning **HTTP 400 Bad Request** if detected.

### 3. Server-Sent Events Keep-Alive Heartbeat
Every 15 seconds, the server writes an SSE comment frame (`: ping\n\n`) to active client sinks, probing connection liveness and preventing reverse proxies from terminating idle connections.

### 4. Transport Protocol Trade-Off Analysis

| Criteria | Server-Sent Events (SSE) + REST [SELECTED] | WebSockets (WS) | gRPC-Web / Connect-Web |
| :--- | :--- | :--- | :--- |
| **Connection Topology** | Simplex stream (server-to-client) + HTTP/2 REST calls. | Full-duplex persistent TCP/WS connection. | HTTP/2 or HTTP/3 Protobuf over Fetch/XHR. |
| **Proxy / Corporate Ingress** | **Flawless.** Passes through all enterprise firewalls, ALBs, Cloudflare, and Nginx. | Frequent issues with corporate proxies, timeout drops, and proxy buffering. | Requires HTTP/2 or Envoy proxy translation. |
| **Virtual Thread Efficiency** | **Native.** Virtual threads handle blocking HTTP/2 streams effortlessly. | Virtual-thread friendly, but requires socket state maintenance per client. | High efficiency, but requires gRPC runtime dependencies. |
| **Automatic Reconnection** | **Built-in.** Browsers natively retry with `Last-Event-ID` header. | Custom client-side reconnection and backoff logic required. | Custom reconnection logic required. |
| **Browser Tooling & Curl** | Trivial debugging via `curl -N http://localhost:8080/api/v1/events/stream`. | Requires specialized WebSocket debugging tools (`wscat`). | Requires `grpcurl` or Protobuf decoders. |
| **Verdict** | **Standard Default.** Cleanest operational profile and zero third-party dependencies. | Consider only if real-time bidirectional typing is required (e.g. collaborative multi-operator terminal). | Valuable if the organization is already standardized on Protobuf schemas. |
