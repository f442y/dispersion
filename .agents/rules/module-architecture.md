---
trigger: always_on
---

# Module Architecture & Dependency Invariants

Dispersion enforces hexagonal multi-module boundaries with zero inter-engine coupling.

### 1. Module Taxonomy & Hierarchy
* **Foundational Domain Triad (Mandatory):**
  * `<domain>-api`: Pure contract (interfaces, sealed records/events, exceptions, builders). Zero runtime or 3rd-party dependencies.
  * `<domain>-core`: Primary runtime engine implementing `<domain>-api`.
  * `<domain>-test`: Deterministic in-memory test doubles (fakes, spies). **Never couples to `*-core`**.
  * `dispersion-testing`: Aggregates all `*-test` companion modules into a unified testkit.
* **Feature Submodules (Decomposition):**
  * Complex domains may partition execution into focused submodules (e.g. `orchestration-batch`, `orchestration-messaging`, `orchestration-command`).
  * Submodules depend on `<domain>-api` and must form a strict DAG (no circular dependencies). Core engines integrate with submodules via SPIs.
* **Permissible Extension Modules:**
  * `<domain>-adapter-<vendor>`: External infrastructure adapters (e.g. Kafka, JDBC). Depends on `<domain>-api` + vendor SDK. **Never on `*-core`**.
  * `<domain>-tck`: Contract test suites. Depends on `<domain>-api` + JUnit/AssertJ. Consumed by adapters via `<scope>test</scope>`.
  * `<domain>-benchmarks`: JMH performance harnesses. Depends on `<domain>-core` + JMH.
  * `*-starter`: Framework auto-configurations (Spring Boot, Quarkus).

### 2. Dependency Scope Matrix
| Module Type | Allowed Dependencies (Compile Scope) | Forbidden Dependencies (Compile / Runtime) |
| :--- | :--- | :--- |
| **`*-api`** | Java Standard Lib, JSpecify, sibling `*-api` | Any `*-core`, `*-test`, `*-adapter-*`, or runtime engines |
| **`*-test`** | `*-api`, AssertJ, JSpecify | Any `*-core` or `*-adapter-*` (fakes must remain pure to API) |
| **`*-core` / Submodules** | Own `*-api`, external `*-api`, SLF4J, JSpecify | External `*-core` modules (only `<scope>test</scope>` allowed) |
| **`*-adapter-*`** | `*-api`, vendor SDKs | Any `*-core` or sibling adapters |

### 3. Package & Encapsulation Invariants
* **Coordinates:** Package structure strictly mirrors module coordinates: `com.github.f442y.dispersion.<domain>.<module_or_feature>.*`.
* **Zero Cross-Core Imports:** Production code in `*-api`, `*-test`, or external `*-core` must **never** import classes from `*.core.*`.
* **Package-Private by Default:** All engine internals, concrete state representations, and helpers must be package-private (`default`). Only public factories, builders, or SPI implementations satisfying `*-api` contracts are `public`.

### 4. System Governance
* **Control Plane Inversion:** `control-core` has zero compile-time dependencies on engine runtimes; engines adapt via `InspectableMachine` SPI.
* **Centralized BOM Alignment:** All modules inherit from `dispersion-parent` and are registered in root `pom.xml` and `bom/pom.xml`. Child POMs must never specify `<version>` for internal artifacts.
* **Virtual-Thread Safety:** Execution hot paths must be thread-confined or lock-free. Never use `synchronized` monitors on hot paths.
