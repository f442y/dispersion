---
name: module-architecture-lint
description: "Audit Maven module structure, dependency scopes, and package imports against Dispersion's hexagonal architecture invariants."
---

# Module Architecture Lint & Audit Skill

Use this skill to audit module structures, Maven dependency scopes, and package imports against the architecture invariants in `.agents/rules/module-architecture.md`.

## Verification Checklist

- [ ] **Triad Foundation:** Domain contains `<domain>/api`, `<domain>/core`, and `<domain>/test` registered in root `pom.xml` and `bom/pom.xml`.
- [ ] **Dependency Boundaries:**
  - `*-api` and `*-test` POMs have zero dependencies on `dispersion-*-core`.
  - `*-core` and submodules have no compile-scoped dependencies on external `*-core` artifacts.
  - `*-adapter-*` POMs depend only on `*-api` + vendor SDKs, never on `*-core`.
  - Child POMs contain no hardcoded `<version>` tags for internal artifacts.
- [ ] **Package Isolation:** Zero `import ...core.` statements inside `api` or `test` source code, and no cross-subsystem `*.core.*` imports in production code.
- [ ] **Encapsulation:** Internal engine classes are package-private (`default`).

## Automated Audit Commands (PowerShell)

Run these checks from the workspace root to audit compliance:

```powershell
# 1. Check for forbidden *-core dependencies in *-api and *-test POMs
Get-ChildItem -Path "*\api\pom.xml", "*\test\pom.xml" -Recurse | Where-Object { $_.FullName -notmatch "\\target\\" } | ForEach-Object {
    $filePath = $_.FullName
    Select-String -Path $filePath -Pattern "<artifactId>dispersion-.*-core</artifactId>" | ForEach-Object {
        "VIOLATION: Forbidden dispersion *-core dependency in ${filePath} - $($_.Line.Trim())"
    }
}

# 2. Check for non-test scoped *-core dependencies in *-core and submodule POMs
Get-ChildItem -Path "*\core\pom.xml", "*\batch\pom.xml", "*\messaging\pom.xml" -Recurse -ErrorAction SilentlyContinue | Where-Object { $_.FullName -notmatch "\\target\\" } | ForEach-Object {
    $filePath = $_.FullName
    $content = [IO.File]::ReadAllText($filePath)
    [regex]::Matches($content, '(?s)<dependency>\s*<groupId>com\.github\.f442y\.dispersion</groupId>\s*<artifactId>([a-z0-9-]+-core)</artifactId>(.*?)</dependency>') | ForEach-Object {
        $dep = $_.Groups[1].Value
        if ($_.Groups[2].Value -notmatch '<scope>test</scope>') {
            "VIOLATION: Non-test scoped core dependency '$dep' in $filePath"
        }
    }
}

# 3. Check for forbidden *.core.* imports in API and Test source trees
Get-ChildItem -Path "*\api\src\main\java", "*\test\src\main\java" -Recurse -Filter "*.java" -ErrorAction SilentlyContinue | 
    Select-String "import\s+com\.github\.f442y\.dispersion\.[a-z0-9_\.]+\.core\." | 
    ForEach-Object { "VIOLATION: Forbidden core import in $($_.Path):$($_.LineNumber)" }

# 4. Check for hardcoded versions of internal dependencies in child POMs
$rootPom = (Resolve-Path "pom.xml").Path
Get-ChildItem -Path "*\pom.xml" -Recurse | Where-Object { $_.FullName -notmatch "\\(target|bom)\\pom.xml$" -and $_.FullName -ne $rootPom } | ForEach-Object {
    $filePath = $_.FullName
    $content = [IO.File]::ReadAllText($filePath)
    [regex]::Matches($content, '(?s)<dependency>\s*<groupId>com\.github\.f442y\.dispersion</groupId>\s*<artifactId>([a-z0-9-]+)</artifactId>\s*<version>') | ForEach-Object {
        "VIOLATION: Hardcoded version for $($_.Groups[1].Value) in $filePath"
    }
}
```

## Scaffolding Guide

1. **Foundational Triad:** Scaffold `<domain>/{api, core, test}`, register in root `pom.xml` `<modules>` and `bom/pom.xml` `<dependencyManagement>`. Export test fakes in `testing/pom.xml`.
2. **Feature Submodule:** Scaffold `<domain>/<feature>`, depend on `<domain>-api` (compile) and other submodules in a strict DAG.
3. **Infrastructure Adapter:** Scaffold `<domain>/adapter-<vendor>`, depend on `<domain>-api` + vendor client SDK. Never depend on `*-core`.
