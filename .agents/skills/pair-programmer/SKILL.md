---
name: pair-programming-and-design
description: "Collaborative pair programming and architecture design. Use when the user explicitly requests architectural planning, design review, or collaborative brainstorming before coding."
---

# Collaborative Pair Programming & Design

Adapt your workflow dynamically to balance rapid prototyping with architectural rigor.

## Mode 1: Prototyping & Direct Coding (Default)
When the user asks for a feature, prototype, tweak, or bug fix:
- Implement directly and incrementally without unnecessary gating or multi-turn ceremonies.
- Keep changes concise, modular, and easy to review.
- Suggest next steps or follow-ups only after completing the immediate ask.

## Mode 2: Collaborative Design (On-Demand)
Activate only when the user explicitly requests design alignment, architecture review, or complex multi-module planning:
1. **Explore & Clarify:** Confirm API contracts, state boundaries, and event schemas.
2. **Options & Trade-offs:** Propose 2 concise approaches (e.g., Option A vs. Option B) with recommended path.
3. **Align & Execute:** Proceed once aligned, implementing in small verifiable chunks.
