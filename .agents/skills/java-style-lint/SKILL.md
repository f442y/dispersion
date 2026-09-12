---
name: java-style-lint
description: "Audit Java files or git diffs against repository coding standards. Use when the user explicitly asks to audit, lint, or verify Java style compliance."
---

# Java Style Lint & Audit Skill

Use this skill to audit Java files, recent changes, or git diffs against the repository's strict coding guidelines defined in `.agents/rules/java-style.md`.

## Audit Checklist

1. **Import Rules:**
   - [ ] No wildcard imports (`import ...*;`).
   - [ ] No inline fully-qualified class names (`java.util.*`, `java.time.*`) in signatures or method bodies.

2. **Typing & Variables:**
   - [ ] No `var` keyword anywhere in production or test code.
   - [ ] All generics must be full-word `SCREAMING_SNAKE_CASE` (e.g. `<EVENT_TYPE>`, `<CONTEXT>`, `<STATE_KEY>`). No single-letter generics like `<T>`, `<E>`, `<K, V>`.

3. **Warning Remediation (Zero Suppressions):**
   - [ ] No `@SuppressWarnings` annotations (e.g. `"unchecked"`, `"rawtypes"`, `"deprecation"`, `"unused"`, `"all"`).
   - [ ] No suppression comments (e.g. `// NOSONAR`, `// CHECKSTYLE:OFF`).
   - [ ] Fix compiler root causes directly (proper type parameterization, replacing deprecated methods, removing dead variables).

4. **Exception Handling:**
   - [ ] Never catch `Throwable`, `Exception`, or `RuntimeException`.
   - [ ] Catch only narrow, concrete declared exceptions.
   - [ ] Use Java multi-catch (`catch (IOException | TimeoutException ex)`) for shared remediation.
   - [ ] Preserve original stack traces via cause or structured logging.

5. **Structured Fluent Logging:**
   - [ ] All log calls must use the SLF4J fluent API (`log.atInfo()`, `log.atDebug()`, `log.atWarn()`, `log.atError()`).
   - [ ] Contextual metadata must use `.addKeyValue("snake_case_key", value)`.
   - [ ] Exceptions must be attached via `.setCause(throwable)`.
   - [ ] No contextual variables buried in free-text messages.

## Audit Commands (PowerShell)

Run these checks to inspect any code changes:

```powershell
# 1. Check for var
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "\bvar\b"

# 2. Check for wildcard imports
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "import\s+(static\s+)?[\w\.]+\.\*;"

# 3. Check for single-letter generics
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "<[A-Z]>|<[A-Z]\s*,|,\s*[A-Z]>"

# 4. Check for legacy log calls
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "log\.(info|debug|warn|error|trace)\("

# 5. Check for @SuppressWarnings
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "@SuppressWarnings"
```