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