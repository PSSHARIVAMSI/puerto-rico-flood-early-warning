# PR #14 Dashboard Review and Split Recommendation

## Purpose

This note supports the review of PR #14 by separating the changes that appear most relevant to the current capstone deliverable from items that should be handled through smaller follow-on PRs or preserved as future work.

PR #14 reflects valuable initiative and a strong dashboard direction. However, the PR is broad in scope and combines dashboard updates, data-layer work, notebooks, SQL, tests, and documentation. For a late-semester capstone closeout, a narrower review path would help the team retain the strongest contributions while reducing merge risk.

## Recommended for Current Scope

- Dashboard changes that directly support the final demonstration and stakeholder-facing narrative.
- Data-layer changes that are necessary for the dashboard to run consistently and reproducibly.
- Focused usability improvements that make the dashboard easier to review, explain, or present.
- Supporting documentation that helps reviewers understand the dashboard outputs, assumptions, and purpose.

## Split Into Smaller PRs

- Notebook reruns or regenerated outputs that are not required for the final dashboard demonstration.
- DuckDB builder or schema changes that require separate validation before they are merged.
- SQL explorer functionality that should be reviewed independently from dashboard user-interface changes.
- Larger feature additions that combine app behavior, analysis outputs, tests, and documentation in one PR.

## Future Enhancements

- Additional map layers or deeper geospatial views.
- More detailed municipio-level filtering and comparison tools.
- Expanded hazard, terrain, earthquake, and sensor-data granularity.
- AI/ML or explainability features that require more design and validation time.
- Additional validation against historical events or public reporting.

## Recommendation

The team should not merge PR #14 in its current form. Instead, the dashboard and data-layer changes that are essential to the capstone should be extracted first, reviewed in smaller PRs, and tested against the final demonstration path.

Items that are valuable but not required for the current deliverable should be documented as future work. This approach preserves the contribution from PR #14 while keeping the final capstone scope clear, reviewable, and defensible.

## PR Impact

This is a docs-only contribution. It does not change the dashboard, data files, notebooks, tests, or application behavior.
