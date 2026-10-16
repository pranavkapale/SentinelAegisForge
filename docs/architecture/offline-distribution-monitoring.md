# Offline feature and prediction distribution monitoring

Phase 20 compares two chronological, disjoint populations from one immutable Phase 19 `offline-prediction-ledger-v1`:

```text
verified prediction ledger -- frozen probabilities
       | -- event IDs / source-row fingerprints
       v
fixed-version corrected statistical Delta source
       v
integrity reconciliation and half-open UTC ingestion-time windows
       v
reference + comparison populations
       v
pure descriptive statistics
       v
immutable local drift snapshot
       X alerts / retraining (not implemented)
```

The full Phase 12 25-column Delta schema is checked by `StatisticalFeatureSource`; `inspect_ledger` verifies table ID/version/schema, candidate contract, ordered event IDs and every source-row fingerprint. The drift builder validates the 11 Phase 16 candidate inputs and reads the already frozen `fraud_probability_estimate`. It never loads an estimator, scores rows, reads labels or uses Phase 11 risk decisions. Both cohorts must be within one ledger window, ordered and non-overlapping. A boundary event belongs to the cohort beginning at that instant, never the one ending there. Observed cohort sizes are recorded rather than inferred from dates.

Seven numeric features and frozen scores report rows, non-null/null counts, missing fraction, minimum, maximum, mean, median, p05 and p95. Exact source monetary decimals remain unchanged; finite float conversion is for distribution calculations only. Percentiles linearly interpolate at `(n-1)*p` on sorted non-null values. KS D is the maximum empirical-CDF separation at every observed distinct value, including ties; it has no p-value or automatic severity. PSI fits up to ten quantile bins on reference values only, collapses duplicate internal boundaries, excludes reference extremes, and uses `0.5` finite pseudocount per effective bin. Empty/degenerate/undersupported metrics are explicitly unavailable. Minimum support defaults to 30 rows per cohort and five non-null values per numeric feature.

Four categorical features report sorted counts/proportions, new and disappeared categories, total variation distance and base-2 Jensen–Shannon **divergence**. Null is the reserved `<MISSING>` category. `OneHotEncoder(handle_unknown="ignore")` does not crash on a new category, but the occurrence remains monitoring evidence. Statistical mean/stddev/z-score null-rate differences are preserved rather than imputed. A fixed score threshold rate at 0.5 is descriptive, not observed fraud prevalence or calibrated probability quality.

Raw `amount` and `prior_amount_sum_10m` are not summed across currencies as a financial total. Overall numeric magnitude distributions carry this caveat; same-currency strata provide amount, rolling sum, prior mean/stddev and z-score descriptions where supported. No FX normalization occurs.

Each `.local/ml/drift-snapshots/<drift_snapshot_id>/` contains canonical `drift_manifest.json`, `drift_metrics.json` and `drift_report.md`. The deterministic ID covers ledger/candidate/source/contract identity, both windows, cohort-membership fingerprints and metric/support configuration. Artifact SHA-256 values protect bytes; repeat construction revalidates without overwrite. Inspection rechecks the retained ledger and fixed Delta source. Incomplete or conflicting output is not automatically repaired. `inspect-drift-snapshot` and `compare-drift-snapshots` are read-only; incompatible identities receive warnings, never a winner ranking.

The Phase 14/19 reference `[2030-01-04, 2030-01-07)` is an earlier post-training synthetic replay cohort, not a production baseline. The comparison is `[2030-01-07, 2030-01-10)`. Phase 19 T1/T2 instead compare evolving label maturity for the *same* frozen predictions; those quality changes cannot be attributed to Phase 20 distribution differences without separate causal evidence. Historical ingestion timestamps do not independently prove online feature/model availability. No production performance, drift alert, retraining, Registry change, approval or deployment is established.
