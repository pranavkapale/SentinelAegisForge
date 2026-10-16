# ADR-034: Offline feature and prediction distribution monitoring

## Status

Accepted

## Context

ADR-033 freezes one label-free prediction ledger and evaluates delayed outcomes separately. We need to describe changes in the inputs and frozen scores across chronological populations without confusing changing label availability, a newly scored model, or synthetic evaluation with production degradation. The Phase 12/16 contracts fix the corrected statistical Delta source and exact 11 model inputs.

## Decision

Partition one verified Phase 19 ledger by explicit, disjoint, half-open UTC ingestion-time windows. Both cohorts therefore share candidate, Registry model version, input contract and fixed Delta lineage. Reuse `inspect_ledger` to reconcile Delta table ID/version/schema, event identities, ordered input and source-row fingerprints. Read probabilities from the ledger, never rescore. Measure all 11 candidate inputs from the pinned Delta rows; do not read labels or risk dispositions.

Use `offline-distribution-monitoring-v1` and `offline-distribution-metrics-v2`. Numeric cohorts retain null counts and use finite float conversion only for descriptive distribution calculations, type-7 `(n-1)` linearly interpolated percentiles, exact empirical-CDF two-sample KS D without p-values, and reference-fitted quantile-bin PSI. Target ten bins; duplicate edges collapse, extreme edges are omitted, and fewer than two effective bins make PSI `UNAVAILABLE`. Add a fixed `0.5` pseudocount to every effective bin in both populations, normalize by the respective smoothed total, then sum `(comparison-reference)*ln(comparison/reference)`. Empty/insufficient support is unavailable with reason, never zero by fiat. Default aggregate support is 30 rows per cohort and five non-null observations per numeric feature, configurable and included in snapshot identity.

Categorical distributions use the sorted union of categories, counts/proportions, TVD and base-2 Jensen–Shannon **divergence** (not its square-root distance); null is the reserved `<MISSING>` category. Report new/disappearing categories and null-rate changes. Preserve decimal(18,4)/decimal(38,4) source values unchanged. Overall raw amount magnitudes are not currency-normalized financial totals; add same-currency descriptive diagnostics where supported. The fixed 0.5 score rate is not observed fraud prevalence.

SHA-256 over canonical ledger/source identity, cohort membership, windows, feature fingerprint and metric/support configuration identifies an immutable local snapshot. Metrics/report byte hashes and source reinspection protect integrity; an existing identical snapshot is verified, never overwritten. An incomplete/conflicting exclusive directory fails closed. This is earlier post-training synthetic historical replay, not an established production reference, alerting policy or causal analysis.

## Alternatives considered

- A monitoring SaaS now could provide managed charts and alerts, but introduces operations and governance before a trustworthy production baseline exists.
- Rescoring history with a new model can answer a separate model-comparison question, but confounds model and population changes in this contract.
- Label-dependent drift alone observes only matured outcomes and can select a biased reference; ADR-033 owns outcome quality separately.
- Automatically retraining on a PSI threshold gives a simple trigger, but no representative reference, alert calibration or promotion authority exists.
- Using only labeled training rows as reference is convenient but selects on delayed-label availability.
- Mixing Registry versions in one comparison could be useful for shadow evaluation, but violates same-model comparability here.

## Trade-offs

The local implementation materializes verified Delta and ledger rows in memory and retains exact source files for later inspection. KS/PSI/TVD/JS are descriptive distances, not significance tests or root-cause evidence. Null patterns and new categories may reflect changing population composition. Smoothing and binning choices affect PSI; algorithm and configuration are versioned. Hashes are integrity checks, not signatures, and local exclusive publication is not a distributed concurrency guarantee.

## Consequences

Feature and prediction distribution changes can be inspected without Docker, a model load, or outcomes. They cannot establish concept drift, worsening fraud quality, production monitoring readiness or a retraining decision. Phase 17 Registry and Phase 18 production-ineligible dossier remain unchanged.
