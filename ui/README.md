# Dispersion Web Dashboard (`ui`)

The **Dispersion Web Dashboard** is a lightweight, responsive developer console for visualizing state machine topologies, monitoring real-time telemetry event streams, inspecting suspended saga executions, and dispatching resumption signals.

> [!NOTE]
> The frontend interface and visual components are subject to ongoing design iterations. The backend HTTP REST and Server-Sent Events (SSE) contracts on `/api/v1/*` represent the stable, decoupled interface.

---

## 🚀 Tech Stack

* **Runtime & Framework:** [Node.js 24+](https://nodejs.org/) + [React 19](https://react.dev/) + [TypeScript](https://www.typescriptlang.org/)
* **Build Tooling:** [Vite 8](https://vite.dev/)
* **Routing:** [TanStack Router](https://tanstack.com/router) (type-safe file-based routing)
* **Data Fetching & Cache:** [TanStack Query v5](https://tanstack.com/query)
* **Styling & Components:** [Tailwind CSS v4](https://tailwindcss.com/) + [@base-ui/react](https://base-ui.com/)
* **Diagrams & Visuals:** [Mermaid.js](https://mermaid.js.org/) (dynamic state topology rendering) + [Lucide Icons](https://lucide.dev/)

---

## 🛠️ Getting Started

### Prerequisites
1. **Node.js 24+** and **npm**
2. A running Dispersion backend server on port `8080` (e.g., [`DispersionDemoApp`](../examples/README.md)):
   ```bash
   # From repository root
   ./mvnw compile exec:java -pl examples -Dexec.mainClass="com.github.f442y.dispersion.examples.DispersionDemoApp"
   ```

### Development
```bash
# 1. Install dependencies
npm install

# 2. Start Vite development server (runs on http://localhost:5173)
npm run dev
```

The Vite dev server automatically proxies all `/api/*` requests to the local backend at `http://127.0.0.1:8080`.

### Production Build
```bash
# Type check and build static production bundle to dist/
npm run build

# Preview production build locally
npm run preview
```

---

## 📡 Backend Integration Contract

The UI consumes standard REST and SSE endpoints exposed by `dispersion-server-standalone` or `dispersion-server-jakarta`:

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/api/v1/node` | `GET` | Node health, uptime, and engine capabilities. |
| `/api/v1/machines` | `GET` | Registered state machine descriptors and Mermaid graphs. |
| `/api/v1/machines/{name}` | `GET` | Topology and state configuration for a specific workflow. |
| `/api/v1/executions` | `GET` | Filter active, completed, failed, or suspended executions. |
| `/api/v1/executions/{id}/timeline` | `GET` | Chronological transition history and audit trail. |
| `/api/v1/events/stream` | `GET` | Real-time Server-Sent Events (SSE) telemetry stream. |
| `/api/v1/executions/signal` | `POST` | Dispatch external webhook/approval signals to suspended sagas. |

---

## 🔗 Related Documentation

* 🏠 [**Project Showcase (`README.md`)**](../README.md) — Main repository landing page.
* 🔭 [**Observability & Control Plane Guide**](../docs/observability-and-control-plane.md) — Complete REST/SSE specification and backend architecture.
* 🌐 [**Server Subsystem Guide**](../server/README.md) — Standalone Helidon SE Níma virtual-thread server.
* 💡 [**Interactive Demo Application**](../examples/README.md) — Running the backend demo server.
