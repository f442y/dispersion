---
name: java-style-lint
description: "Audit Java files or git diffs against repository coding standards. Use when the user explicitly asks to audit, lint, or verify Java style compliance."
---

# Java Style Lint & Audit Skill

Use this skill to audit Java files, recent changes, or git diffs against the coding standards in `.agents/rules/java-style.md`.

## Verification Checklist

- [ ] **Imports:** No wildcard imports (`*`); no inline fully-qualified class names.
- [ ] **Typing & Generics:** No `var`; generics must use full-word `SCREAMING_SNAKE_CASE` (e.g. `<STATE_KEY>`, never `<T>`).
- [ ] **Warnings & Exceptions:** Zero `@SuppressWarnings` or suppression comments; catch specific checked/declared exceptions (never `Exception` or `Throwable`).
- [ ] **Structured Logging & Idioms:** SLF4J fluent logging with `.addKeyValue("key", val)` and `.setCause(ex)`; prefer immutable `record`s and arrow `switch` expressions.

## Automated Audit Commands (PowerShell)

Run these checks across modified files or the entire repository:

```powershell
# 1. Check for 'var' usage
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "\bvar\b"

# 2. Check for wildcard imports
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "import\s+(static\s+)?[\w\.]+\.\*;"

# 3. Check for single-letter generics (<T>, <E>, etc.)
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "<[A-Z]>|<[A-Z]\s*,|,\s*[A-Z]>"

# 4. Check for legacy non-fluent logger calls
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "log\.(info|debug|warn|error|trace)\("

# 5. Check for @SuppressWarnings annotations
Get-ChildItem -Recurse -Filter "*.java" | Where-Object { $_.FullName -notmatch "\\target\\" } | Select-String "@SuppressWarnings"
```