# Offline model candidate artifact

```text
Verified Phase 12 snapshot
          ↓
Verified Phase 13 training run (optional read-only Phase 15 tracking check)
          ↓
Reconstruct original ingestion-time cohorts and fit-label cutoff
          ↓
Fit unchanged sklearn Pipeline on eligible training rows only
          ↓
Compare all archived validation/test probabilities by split + event_id
          ↓
Save local MLflow sklearn Model in skops format
          ↓
Hash files, reload from trusted local bytes, rescore, publish immutably
          ↓
TECHNICALLY_VERIFIED offline candidate
```

The candidate is a **verified reconstruction**, not the original in-memory Phase 13 estimator. Its candidate ID is a deterministic SHA-256 of `offline-logistic-candidate-v1`, canonical training/snapshot identities, input schema fingerprint, preprocessing/estimator configuration, serialization format and pinned Python/sklearn/pandas/MLflow/skops versions. It does not depend on output root, wall clock or model bytes. A separate sorted relative-path/content-hash tree identifies the actual MLflow Model files. Rebuilds can have the same semantic ID without a byte-for-byte serialization promise.

The exact ordered input is `amount`, `currency`, `country`, `transaction_type`, `prior_transaction_count_5m`, `prior_amount_sum_10m`, `prior_amount_observation_count`, `prior_amount_mean`, `prior_amount_stddev`, `amount_zscore`, `statistical_feature_status`. Source amounts remain exact Decimal (`decimal(18,4)` and `decimal(38,4)`) until the existing `feature_frame` conversion to model floats; those floats are approximations, not ledger values. Statistics are nullable only in their established NO_HISTORY, INSUFFICIENT_VARIANCE_HISTORY and ZERO_VARIANCE states. READY requires coherent finite mean/stddev/z-score context. Unknown but well-formed categorical strings are handled by the fitted `OneHotEncoder(handle_unknown="ignore")`. Identities, timestamps, Kafka coordinates, labels and risk decisions are not accepted as model inputs.

The native sklearn flavor preserves ColumnTransformer, encoder, imputer, scaler and LogisticRegression; offline scoring explicitly checks class order `[0,1]` and returns the class-1 `fraud_probability_estimate` in `[0,1]`. It is not calibrated or an enforcement decision. Candidate inspection checks source identities, local runtime, model bytes and archived probability equivalence before accepting the artifact. The tolerance is `math.isclose(rel_tol=1e-12, abs_tol=1e-12)`; the manifest records comparison count, maximum absolute/relative difference, mismatch count and positive class. No metrics are recalculated or improved.

`skops` is selected explicitly. The pinned Pipeline needs a static `numpy.dtype` trust entry; unknown types are rejected. Integrity is checked before loading, but hashes are not signatures. Do not load external or user-supplied artifacts simply because they carry a matching self-reported manifest. The ignored `.local/ml/model-candidates/<candidate_id>/` directory contains only the MLflow Model, manifest, input contract and equivalence summary; it does not copy the dataset, private labels or predictions. Incomplete or conflicting candidate directories fail closed. A candidate is technically verified for local offline inference only; no registration, approval, serving or production fraud-quality claim follows. The reference evaluation remains **Synthetic Scenario Evaluation — Not Production Fraud Performance**.
