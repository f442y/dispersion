---
trigger: always_on
---

# Java Coding Guidelines

### 1. Imports & Formatting
* **No Wildcards:** Declare every class import explicitly (e.g., `import java.util.List;`). Never use wildcard (`*`) imports.
* **No Inline FQCN:** Import classes at the top of the file; avoid inline FQCNs (e.g. avoid `java.time.Instant.now()`).

### 2. Typing, Variables & Generics
* **No `var`:** Explicitly declare all variable and parameter types (e.g., `List<String> items = new ArrayList<>();`).
* **Descriptive Generics:** Generic parameters must be full-word `SCREAMING_SNAKE_CASE` (e.g., `<EVENT_TYPE>`, `<CONTEXT>`, `<STATE_KEY>`). Single-letter generics (`<T>`, `<E>`, `<K, V>`) are strictly forbidden.

### 3. Warnings & Exceptions
* **Fix Root Causes:** Avoid `@SuppressWarnings` in production code; parameterize raw types and update deprecated calls.
* **Narrow Exceptions:** Catch specific checked/declared exceptions in core domain logic. Never swallow exceptions; preserve cause or log structured error keys.

### 4. Logging & Modern Idioms
* **Structured Fluent Logging:** Use the SLF4J fluent API with key-value pairs:
  ```java
  log.atInfo()
     .addKeyValue("order_id", orderId)
     .addKeyValue("account_id", accountId)
     .log("Order processed successfully");
  ```
  Use `.setCause(throwable)` when logging exceptions.
* **Modern Idioms:** Prefer `record`s for immutable data/events, pattern-matching switch expressions with arrow (`->`) syntax without `break`, and immutable collections (`List.of()`, `Set.of()`).

### 5. Testing & Compiler Conventions
* **Package-Private Tests:** Test classes, `@Test` methods, lifecycle callbacks (`@BeforeEach`, `@AfterEach`), and inner fixture types must be package-private (`default`). Never declare them `public`. In modular Java (JPMS), public test classes in exported packages pollute the exported API surface and cause `in module is not exported` warnings.
* **Test Logging Quietness & Parameterization:**
  * Tests must run quiet by default in CI and CLI builds (`WARN` root level), logging only warnings, errors, and intentional benchmark outputs.
  * Every module utilizing `logback-classic` in tests must provide `src/test/resources/logback-test.xml` defaulting to `${dispersion.log.level:-WARN}` and suppressing 3rd-party logger noise (`io.helidon`, `org.apache.fory`).
  * Never allow Logback to fall back to `BasicConfigurator` (which sets root level to `DEBUG` and spams tens of thousands of lines into test output).
  * Modules running embedded servers (e.g., Helidon) must provide a test-scoped `logging.properties` referenced via Surefire `systemPropertyVariables` to suppress server socket/channel startup noise.
  * Engineers can dynamically elevate log levels during local debugging via `-Ddispersion.log.level=DEBUG` without modifying codebase files.
* **Compiler & Build Hygiene:** Builds enforce `-proc:full`, `-parameters`, `-Xlint:all`, `-Xdiags:verbose`, and debug symbols `lines,vars,source`. Test compilation specifically suppresses `-Xlint:-exports` and `-Xlint:-transitive` for classpath-loaded test libraries.
