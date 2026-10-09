# Local MLflow experiment tracking (Phase 15)

## Provenance boundary

```text
Phase 14 scenario corpus (explicitly reconciled when supplied)
    → statistical-feature Delta version
    → Phase 12 immutable snapshot ID and logical fingerprint
    → Phase 13 immutable training-run ID
    → MLflow experiment ID / provider-generated run ID
```

The Phase 12/13 files remain authoritative. `track-baseline-run` reads an **existing** run; it calls the Phase 13 inspector, the Phase 12 snapshot inspector, and optionally Phase 14 corpus reconciliation before opening the tracking store. It neither fits the estimator nor recalculates metrics or predictions. A corpus ID is never guessed from a directory name. To include it, supply the corpus plus validated, deduplicated, rolling and statistical Delta paths; the statistical Delta version and labels must match the verified snapshot.

## Local storage and identity

Default experiment: `sentinelaegisforge-synthetic-fraud-baseline`. Metadata lives in the repository's ignored `.local/mlflow/tracking.db` via an absolute `sqlite:////...` URI; artifacts live in ignored `.local/mlflow/artifacts/`. Paths and experiment name are configurable. No HTTP server is required. The optional UI binds to `127.0.0.1:5000` by default and must not be exposed as a production tracking service.

The SHA-256 `training_run_id` is the canonical engineering identity. MLflow's `run_id` is a separate provider address. Tags carry training/dataset contract versions, snapshot ID, logical fingerprint, source Delta version/schema fingerprint/AS_OF, source hashes, Python/pandas/sklearn/MLflow versions, and (only when explicitly reconciled) scenario corpus ID. Synthetic scope is mandatory: `data_origin=synthetic`, `evaluation_scope=integration_verification`, `production_fraud_quality=unproven`, `model_family=logistic_regression`.

Parameters come from the verified manifest: estimator class/solver/class weight/C/tolerance/max iterations, threshold, preprocessing and candidate-feature contracts, split boundaries and minimum cohort support. Validation and test ranking/threshold metrics, class support, prevalence and confusion counts are copied from authoritative `metrics.json` under separate namespaces. A genuinely absent ROC AUC is omitted, never made zero. The original manifest, metrics JSON and a compact provenance summary are uploaded and then downloaded/hash-checked. Predictions stay in their original immutable run directory; their SHA-256 is indexed. No labels, private synthetic truth, row-level predictions or model binary are uploaded.

## Retry, failure and comparison

The adapter holds a cooperating local single-writer lock during canonical-ID lookup and creation. A repeat returns the same finished run only after exact parameter/tag/metric and artifact-hash read-back. Conflicting or multiple claims fail; incomplete/failed claims remain visible and are not overwritten or silently repaired. This is **sequential idempotence**, not a globally unique MLflow constraint or concurrent exactly-once guarantee. External writers that ignore the lock can still race.

`inspect-tracked-run` and `compare-tracked-runs` are read-only. Comparison shows identities, split periods, estimator configuration, cohort support and validation/test metrics, warning when datasets or periods differ. It neither ranks a production winner nor promotes a model. The Phase 14 metrics are explicitly **Synthetic Scenario Evaluation — Not Production Fraud Performance**; below-baseline synthetic ranking results are retained as factual evidence.

## Commands

```sh
make track-baseline-run BASELINE_RUN_PATH=/absolute/path/to/<training_run_id>
make inspect-tracked-run MLFLOW_RUN_ID=<provider-run-id>
make compare-tracked-runs
make mlflow-ui
```

For corpus-linked tracking, additionally supply `MLFLOW_CORPUS_PATH`, `MLFLOW_VALIDATED_PATH`, `MLFLOW_DEDUPLICATED_PATH`, `MLFLOW_ROLLING_PATH` and `MLFLOW_STATISTICAL_PATH`. All five are required together. The original training-run directory is never modified by tracking. Ordinary `make verify` uses isolated SQLite test backends, requires no Docker/server and never launches the UI.

Repeat a corpus-linked tracking call with the same five explicit paths. Omitting that verified provenance on a later retry changes the expected evidence and fails closed rather than silently weakening the existing record.

Model registration, candidate/champion governance, promotion, serving, drift and retraining remain deferred.
