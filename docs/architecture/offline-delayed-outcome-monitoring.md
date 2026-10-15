# Offline prediction replay and delayed-outcome monitoring

```text
Verified corrected statistical Delta version
                 ↓
Exact research-only Registry model version
                 ↓
Trusted Phase 16 scorer, 11 fixed features
                 ↓
Label-free immutable prediction ledger
                 │
Independent verified synthetic labels ──┘
                 ↓
AS_OF revision resolution and causal join
                 ↓
Immutable coverage, quality and segment snapshots
```

This is **Offline Synthetic Historical Replay & Delayed-Outcome Evaluation**, not real-time serving, historical proof that the later-reconstructed model existed at transaction ingestion, shadow deployment, or production monitoring. Stored `ingestion_time` is the observation chronology for this experiment, not an independently measured feature-materialization time.

## Frozen ledger

The replay requires a numeric Registry version, verified research-only candidate, explicit corrected statistical Delta path/version and UTC half-open `[score_start,score_end)` window. The start cannot precede the candidate's original `train_end`. The source must match the training snapshot's Delta path, table ID, version and 25-column schema fingerprint. All source rows pass existing mathematical/status/unique-event validation. Only the established 11 feature columns reach the trusted estimator. Source exact Decimals are converted only by its existing model projection. No labels or risk decisions are scoring inputs.

The `offline-prediction-ledger-v1` row contains event ID, ingestion time, candidate ID, FRAUD-class probability estimate, currency, country, transaction type and a full-source-row logical fingerprint. It excludes labels, customer/device/IP identifiers and Phase 11 dispositions. Segmentation fields are metadata, not model features. Rows are sorted by `(ingestion_time,event_id)` and have unique event IDs. Identity covers model artifact tree, input contract, Delta lineage, scoring window, ordered logical input and runtime/implementation versions. Parquet byte SHA-256 is separate. The complete selected source ID set and row fingerprints must match the persisted ledger. Archived Phase 13 validation/test overlap is compared at `1e-12` relative and absolute tolerance. A repeated run verifies the existing ledger; incomplete or conflicting artifacts fail closed.

## Point-in-time outcomes

Only the separate evaluator reads the verified Phase 14 private scenario label JSONL. It validates the corpus/label hashes and revision history, reconciles label IDs against the *full* feature source (so labels outside the scoring window are expected, but unknown IDs fail), and requires every observation to follow its transaction ingestion time. `evaluation_as_of` is explicit UTC and at or after `score_end`. The highest revision observed by that cutoff is selected; future revisions cannot enter earlier snapshots. A missing eligible label is `UNLABELED`, never LEGIT. Prediction probabilities and ledger bytes do not change when labels mature.

`delayed-outcome-monitoring-v1` snapshots contain canonical manifest, coverage, aggregate quality, segment quality, selected local outcome revisions and a human report. The selected-outcomes file is a local evaluation artifact, not uploaded to MLflow or served. Snapshot ID includes ledger ID, candidate ID, label-byte hash, cutoff, resolution/metrics/segment versions and minimum-support settings. Output hashes detect corruption; repeated evaluation verifies existing bytes and logical rows. T2 does not alter T1. A comparison distinguishes newly labeled events, changed revisions, outcome flips, unchanged labels, support changes and metric movement.

## Denominators and interpretation

Coverage denominator is all scored ledger rows. Quality denominator is only scored rows with an eligible observed outcome. Reports include FRAUD/LEGIT support, first/later revision selections, label-delay distribution and outside-window/future label counts. Average Precision, ROC AUC and fixed-0.5 confusion/precision/recall/F1 reuse Phase 13 evaluation conventions; default minimum support is 20 labeled rows and 2 per class. When insufficient, ranking and threshold metrics are `UNAVAILABLE` with a reason, not zero. Currency, country and transaction-type segments report scored/labeled coverage and only supported quality metrics.

Delayed labels may be selected non-randomly: known-outcome quality can differ from quality on the entire scored population. Metric movement may reflect new/corrected truth instead of changed model behavior. Synthetic segments are not demographic fairness or production robustness evidence. No drift thresholds, alert, model-selection, Registry promotion, retraining or serving action is attached to a snapshot.
