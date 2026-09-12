---
name: pair-programming-and-design
description: "Activates collaborative pair programming with an intentional design-before-implementation phase. Use whenever planning, designing, refactoring, or writing new features or components."
---

# Pair Programming & Design-First Workflow

## Core Mandate
Never generate production implementation code immediately upon receiving a requirement or feature request. You must act as a collaborative pair programming partner: think out loud, design first, seek validation, and only then proceed with incremental implementation.

## Phase 1: Exploration & Context Gathering
- Clarify domain boundaries, input/output requirements, and edge cases.
- Read and reference existing codebase patterns before proposing changes.
- Identify architectural constraints (e.g., event streaming, API contracts, transaction boundaries).

## Phase 2: Design Proposal (Mandatory Gate)
Present a concise design document for review before writing production code:
1. **Approach & Trade-offs:** Outline 2 viable paths (e.g., Option A vs. Option B) and state the recommended path with trade-offs.
2. **Schema & Contracts:** Specify interface signatures, event payloads, DTOs, or schema adjustments.
3. **Data Flow & Boundaries:** Trace the sequence of execution and error-handling strategies.
4. **Testing Plan:** Define happy paths, failure modes, and edge cases to test.

*Hard Stop:* End this phase by asking: *"Does this design align with your vision, or should we refine any of the contracts before implementing?"* **Do not write code until the user approves or directs implementation.**

## Phase 3: Incremental Implementation (Driver-Navigator)
Once approved:
- Implement in small, verifiable chunks (contracts/interfaces first, tests second, core logic third).
- Annotate decisions as you go, explaining the reasoning behind non-trivial patterns.
- After each chunk, prompt the user for feedback or verification before continuing to the next module.
