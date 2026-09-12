---
trigger: always_on
---

# Java Coding & Architectural Guidelines

Adhere strictly to these conventions when generating, refactoring, or proposing Java code.

---

### 1. Import Rules
* **No Wildcards:** Never use wildcard (`*`) imports under any circumstance. Declare every class import explicitly (e.g., `import java.util.List;`, not `import java.util.*;`).
* **No Inline FQCN:** Do not use fully qualified class names inside method bodies, field declarations, or signatures (e.g., avoid `java.time.Instant.now()`). Add explicit imports at the top of the file.

---

### 2. Typing, Variables & Generics
* **No `var`:** Never use the `var` keyword. Explicitly declare all variable types (e.g., `List<String> items = new ArrayList<>();`).
* **Full-Word Generics:** Single-letter generic identifiers (`<T>`, `<E>`, `<K, V>`) are strictly forbidden.
* **SCREAMING_SNAKE_CASE Generics:** All generic type parameters must be descriptive, multi-word names formatted in uppercase snake case (e.g., `<ENTITY_TYPE>`, `<EVENT_PAYLOAD>`, `<COMMAND_OBJECT>`).

---

### 3. Warning Remediation (Zero Suppressions)
* **Never Suppress Warnings:** Never use `@SuppressWarnings` annotations (e.g., `"unchecked"`, `"rawtypes"`, `"deprecation"`, `"unused"`, `"all"`), and never add suppression comments (e.g., `// NOSONAR`, `// CHECKSTYLE:OFF`).
* **Resolve Root Causes:** Always resolve underlying compiler issues directly:
  * Parameterize raw types properly.
  * Update deprecated calls to current replacements.
  * Remove dead/unused variables or refactor signatures.
  * Implement explicit null checks or use `try-with-resources`.

---

### 4. Exception Handling
* **Never Catch Generic Exceptions:** Never catch `Throwable`, `Exception`, or `RuntimeException`.
* **Catch Specific Exceptions:** Inspect the operations inside the `try` block and catch only the narrow exceptions declared or thrown (e.g., `catch (IOException ex)`).
* **Multi-Catch Syntax:** Use Java multi-catch (`catch (IOException | TimeoutException ex)`) when multiple concrete exceptions share identical remediation.
* **Preserve Stack Traces:** Never swallow exceptions. Wrap in a domain exception preserving the cause (`throw new OrderProcessingException("...", ex)`) or log structured error keys.

---

### 5. Logging & Modern Conventions
* **Structured Fluent Logging:** Use the SLF4J fluent API (`log.atInfo()`, `log.atError()`) with key-value pairs:
  ```java
  log.atInfo()
     .addKeyValue("order_id", orderId)
     .addKeyValue("account_id", accountId)
     .log("Order processed successfully");