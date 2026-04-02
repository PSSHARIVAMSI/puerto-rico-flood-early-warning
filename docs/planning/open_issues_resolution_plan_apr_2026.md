# Open Issues Resolution Plan (April 2026)

## Purpose

This document collates the current open GitHub issues into decision-ready workstreams so the team can close or consolidate them cleanly without fragmenting the roadmap.

## Current Collation

### 1. Architecture / Delivery Epic

Combine:

- Issue `#4` — Snowflake for the project
- Architecture and delivery-scope portions of issue `#7`

**Decision status**

- Resolved for the Spring 2026 semester: the implementation path is DuckDB + local Streamlit workbench + existing GitHub Pages public dashboard flow.
- Deferred for future scaling review: Snowflake, AWS-hosted patterns, and other managed-platform alternatives.

**Recommended action**

- Close issue `#4` as a current-semester architecture decision.
- Close the architecture portion of issue `#7` by pointing to the same decision and leaving only unresolved model questions, if any, to the risk-model epic.

**Suggested closing statement for issue `#4`**

> Closing this issue as a current-semester architecture decision. For Spring 2026, the repository standard is DuckDB for the local analytical store, Streamlit for the local/internal workbench, and the existing GitHub Pages flow for the public-facing dashboard. Snowflake remains a valid follow-on alternative, but it is intentionally deferred until post-semester scaling discussions so a future team can evaluate it against validated needs, cost, and operational ownership. The decision record is now documented in the repo for reuse.

**Suggested architecture closure text for issue `#7`**

> The architecture and delivery-path portion of this issue is now resolved for the current semester. We are proceeding with the local-first DuckDB + Streamlit workbench approach, while keeping managed-platform alternatives for a later scaling review. Any remaining metric or modeling questions from this issue should be handled under the risk-model hardening workstream rather than as a separate architecture thread.

### 2. Risk Model v1.1 Epic

Combine:

- Issue `#5` — clear risk index metrics
- Issue `#6` — terrain and geographic indicators
- Metric-definition and model-hardening parts of issue `#7`

**Current repo position**

The repository already has real groundwork in place:

- executable index configuration
- terrain feature pack specification and implementation path
- staged notebook scoring pipeline
- age-based social adjustment overlay
- local workbench exposure of some adjustment outputs

**Recommendation**

- Combine these into one `Risk Model v1.1` workstream rather than scattering changes across separate issues.
- Prefer real implemented capability where the repo already has supporting inputs, config, or tested notebook paths.
- Do not add fake placeholders in notebooks or GUI just to claim feature coverage.
- Use explicit backlog/spec language only for items that are not yet backed by stable inputs or validated formulas.

**Advice on README TODO vs real implementation**

Use real implementation when all three conditions are true:

1. The input data exists in a stable, repeatable form.
2. The formula or rule is documented in spec/config.
3. The output can be validated and explained.

Use backlog/spec-only tracking when any of those are still missing.

That means:

- age-sensitive adjustment is already in real implementation territory
- terrain is in real sidecar implementation territory, with selective promotion into scoring still pending
- transport/no-vehicle, housing fragility, and related social refinements should be promoted only where the inputs and formulas are ready for defensible scoring

**Recommended next implementation order inside this epic**

1. Finalize the metric contract in spec/config for transport, housing fragility, income/poverty, and terrain-sidecar promotion rules.
2. Promote only the metrics that already have stable data and explainable formulas.
3. Add validation and comparison outputs so each metric change can be justified.
4. Surface high-value, already-landed factors in the local workbench before expanding further.

**Suggested consolidation statement for issues `#5`, `#6`, and metric portions of `#7`**

> Consolidating this work into a single Risk Model v1.1 hardening stream. The repo already contains the core scoring pipeline, executable index config, terrain sidecar capability, and age-based adjustment work. Going forward, we will promote additional metrics only where data inputs, formulas, and validation are mature enough to support explainable scoring. This avoids fragmented issue handling and keeps model changes aligned with the documented index contract.

### 3. AI-Assisted UX Epic

Combine:

- Issue `#9` — chatbot integration in Streamlit dashboard
- User-facing chatbot/assistant portion of issue `#10`

**Decision status**

- Valid future direction
- Not the current MVP critical path ahead of architecture closure and model hardening

**Recommended scope**

- Near-term target: constrained natural-language or prompt-driven query support over current DuckDB-backed views
- Near-term non-goal: broad AI/ML claims around automated ingest or model training without validated datasets and evaluation

**Suggested closing statement for issue `#9`**

> Closing this as a standalone issue and folding it into the AI-Assisted UX workstream. The immediate path is a constrained query assistant over the current DuckDB-backed decision-support views, not a broad chatbot platform. This keeps the work aligned with the current local workbench architecture and prevents overlap with broader AI/ML discussions.

### 4. Future ML / Automation Backlog

Retain from issue `#10`:

- AI-assisted data collection
- ML-enhanced risk calculations
- broader automation ideas that go beyond current validated scoring

**Decision status**

- Deferred backlog, not current MVP critical path

**Suggested closing statement for issue `#10`**

> Closing this issue as written because it combines three separate workstreams: data-collection automation, ML-enhanced scoring, and user-facing assistant UX. For the current MVP, we are separating those concerns. User-facing assistant work belongs in the AI-Assisted UX stream. ML-based scoring and automated ingest remain future backlog items until the rule-based model, validation framework, and operational data contracts are more mature.

## Recommended Execution Order

1. Resolve architecture and semester-end target first: `#4` plus architecture parts of `#7`
2. Resolve the model contract next: `#5`, `#6`, plus metric parts of `#7`
3. Only then address AI-assisted UX: `#9` plus the chatbot/user-facing part of `#10`
4. Keep future ML and automation as deferred backlog until the validated MVP is stable

## Local Validation Rule Before Push

Before any of these changes are shipped to `main`, validate locally that:

- architecture decision docs and README references are consistent
- any promoted metric has a backing input, config/spec reference, and testable output
- AI/UX wording does not imply current capability that the repo does not actually provide
