---
trigger: always_on
---

# Java Coding & Architectural Guidelines

Adhere strictly to these conventions when generating, refactoring, or reviewing Java code.

---

### 1. Imports & Formatting
* **No Wildcards:** Never use wildcard (`*`) imports. Specify every class explicitly (e.g., `import java.util.List;`).
* **No Inline FQCN:** Do not reference fully-qualified class names in method bodies or signatures. Place all imports at the top of the file.

---

### 2. Typing, Variables & Generics
* **No `var`:** Do NOT use the `var` keyword. Explicitly declare all variable and parameter types.
* **Full-Word Generics:** Single-letter generic identifiers (like `<T>`, `<E>`, `<K, V>`) are strictly forbidden.
* **Generic Casing:** Generic parameters must be descriptive and formatted in `SCREAMING_SNAKE_CASE` (e.g., `<ENTITY_TYPE>`, `<EVENT_PAYLOAD>`, `<COMMAND_OBJECT>`).

---

### 3. Modern Java Idioms
* **Data Modeling:** Default to `record` types for all DTOs, value objects, Kafka event payloads, and command envelopes.
* **Domain Hierarchies:** Use `sealed interface` or `sealed class` with explicit `permits` for closed sets of events, states, or commands.
* **Pattern Matching:** Use modern pattern matching for `instanceof` and modern `switch` expressions with arrow (`->`) syntax. Avoid `switch` statements with `break`.
* **Collections:** Use immutable factory methods (`List.of()`, `Set.of()`, `Map.of()`) and `Stream.toList()`. Never return `null` for collections.
* **Text Blocks:** Use text blocks (`""" ... """`) for multi-line strings, SQL queries, or JSON templates.
* **Time API:** Use `java.time` types (`Instant`, `OffsetDateTime`, `LocalDate`). Never use `Date` or `Calendar`.

---

### 4. Structured Logging
* **SLF4J Fluent API:** Use the SLF4J fluent logging API (`log.atInfo()`, `log.atError()`, etc.) for all log statements.
* **Key-Value Attributes:** Attach contextual metadata using `.addKeyValue("key_name", value)` using snake_case keys. Do not bury structured contextual data in the free-text message.
* **Example:**
  ```java
  log.atInfo()
     .addKeyValue("transaction_id", transactionId)
     .addKeyValue("account_id", accountId)
     .addKeyValue("event_type", "FUNDS_TRANSFERRED")
     .log("Funds transfer executed successfully");