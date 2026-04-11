# Platform Alternatives and Scaling Follow-On

## Current Semester Decision

For the Spring 2026 semester delivery, the repository standard is:

- DuckDB as the local analytical store
- Streamlit as the local/internal workbench layer
- GitHub Pages as the current public-facing dashboard surface

This decision closes the current-semester platform question for implementation purposes. It keeps the stack low-friction, low-cost, easy to reproduce locally, and aligned with the notebook-first workflow already established in the repository.

## Why This Is the Right Current-State Decision

- The current project already produces stable curated notebook outputs that are well-suited for local analytical loading.
- DuckDB requires no separate server or paid platform to begin working productively.
- Streamlit provides a practical local UI/workbench layer for QA, exploration, and explanation.
- The team can validate data products and user interaction patterns now without prematurely committing to a heavier cloud architecture.

## Deferred Alternatives for Future Scaling Reviews

The following alternatives remain valid for future review, but they are intentionally deferred until a later scaling decision cycle:

- Snowflake-centered notebook, storage, and Streamlit workflow
- AWS-oriented scheduled ingestion and hosted application architecture
- Additional managed database or warehouse options if future funding, governance, and operational ownership justify them

These alternatives are not rejected. They are deferred. The decision point is expected after the current semester, when a later team or scaling-focused review can evaluate:

- validated user needs
- expected data volume and refresh cadence
- hosting and security requirements
- budget and operational ownership
- deployment and maintenance expectations

## Guidance for Future Teams

When the scaling discussion resumes, compare alternatives against these criteria:

1. Reproducibility for new contributors
2. Total cost and operational burden
3. Fit with the notebook and pipeline workflow already in place
4. Ease of integrating public data feeds and curated outputs
5. Long-term support for decision-support dashboards and explainability

## Positioning Statement

DuckDB + Streamlit is the current implementation decision. Platform alternatives such as Snowflake or AWS-hosted patterns are documented for future follow-up and should be revisited during formal scaling-up decision meetings rather than introduced mid-semester.
