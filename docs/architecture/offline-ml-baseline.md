# Temporal offline ML baseline

## Current flow

```text
verified immutable transaction-fraud-v1 snapshot
    ↓ ingestion-time chronological split
training labels known by train_end only
    ↓ training-only categorical/numeric preprocessing
LogisticRegression in memory
    ↓ separate validation/test predictions and metrics
immutable training manifest + metrics.json + predictions.parquet
```

Training calls Phase 12 `inspect_snapshot` before reading `dataset.parquet`. It never reads Delta, JSONL labels, Kafka or deterministic risk decisions directly. The source snapshot remains unchanged. Its contract version, snapshot ID, logical fingerprint, Delta version/schema fingerprint and AS_OF_TIME are carried into the run manifest.

## Causal chronology and cohort gates

Use explicit UTC `train_end < validation_end < test_end <= AS_OF_TIME`. Training is `ingestion_time < train_end`; validation is `[train_end, validation_end)`; test is `[validation_end, test_end)`. Rows at exactly `test_end` are outside the run. The same event ID may appear in only one split. `event_time` describes the business event; `ingestion_time` reflects the current online observation order used by the prior-observed Phase 10.1 features. No random split or cross-validation exists.

An ingested training-period row fits only if its selected Phase 12 `label_observed_at <= train_end`. Later-known labels are counted as `excluded_from_fit_due_to_label_delay`, never converted to negatives. Validation/test labels may be known later, up to the selected snapshot AS_OF_TIME; they serve retrospective evaluation only. Require both outcomes and configurable minimum 20 training, 10 validation and 10 test rows by default. Insufficient support fails before a run directory is created. These are engineering minima, not evidence of statistical power.

## Feature and preprocessing boundary

The Phase 12 manifest must match the package's declared eleven candidate columns. Categorical columns are `currency`, `country`, `transaction_type`, `statistical_feature_status`; numeric columns are `amount`, `prior_transaction_count_5m`, `prior_amount_sum_10m`, `prior_amount_observation_count`, `prior_amount_mean`, `prior_amount_stddev`, `amount_zscore`. Identity, merchant/device/IP, event/ingestion timestamps, Kafka lineage, labels/provenance and risk-rule outputs do not reach the estimator.

Training-only `OneHotEncoder(handle_unknown="ignore")` handles later unknown categories. Numeric `SimpleImputer(strategy="constant", fill_value=0.0, keep_empty_features=True)` retains immature statistical columns; status distinguishes their semantic absence. `StandardScaler` is fitted only on eligible training rows. The `ColumnTransformer` is sparse-compatible; the sklearn `Pipeline` binds it to the estimator. Money remains exact Decimal in the Phase 12 Parquet file and becomes float only in this model-input projection. No source values are rewritten.

## Model and evaluation

The sole model is logistic regression with `solver="lbfgs"`, `class_weight="balanced"`, `C=1.0`, `tol=1e-4`, `max_iter=1000` and `fit_intercept=True`; all effective estimator parameters are persisted. `lbfgs` is deterministic for fixed data and ignores `random_state`. Balanced class weights do not change dataset row counts. No hyperparameter search or threshold selection occurs. A fixed 0.5 threshold is used only for classification statistics.

`predict_proba` supplies baseline probability estimates, not calibrated fraud probabilities. Validation and test each record rows, FRAUD/LEGIT support, prevalence, Average Precision (primary), ROC AUC, threshold precision/recall/F1 and `tn/fp/fn/tp`. Undefined precision when there are no positive predictions is `null`. No metric target or combined validation/test metric is used. The test set has no role in fit, preprocessing fit, parameter selection or threshold choice.

## Reproducible run and limits

The SHA-256 `training_run_id` hashes canonical training contract/dataset identity, normalized snapshot path, split bounds/minima, ordered feature contract, preprocessing, effective estimator parameters, fixed threshold and Python/pandas/scikit-learn/pyarrow/deltalake versions. The run directory contains `manifest.json`, `metrics.json` and `predictions.parquet`; predictions include only validation/test event ID, split, ingestion timestamp, observed label, probability estimate and fixed-threshold prediction. Existing artifacts are verified, never overwritten. Manifest hashes detect changed metric/prediction bytes. No model artifact is persisted.

The existing live Phase 12 five-event source yields only four labeled T2 rows and intentionally fails minimum support, chronological-window and/or class gates. Controlled fixture results demonstrate implementation mechanics, not useful fraud-model performance. Representative fraud labels, temporal walk-forward analysis, label maturity/selection bias, calibration, tuning, artifact registry, MLflow, serving, drift and retraining remain deferred.

## Local commands

```sh
make train-baseline-model \
  BASELINE_DATASET_SNAPSHOT_PATH=/absolute/path/to/<snapshot_id> \
  BASELINE_TRAIN_END=2030-01-03T00:00:00Z \
  BASELINE_VALIDATION_END=2030-01-05T00:00:00Z \
  BASELINE_TEST_END=2030-01-07T00:00:00Z
make inspect-baseline-run BASELINE_RUN_PATH=/absolute/path/to/<training_run_id>
```

The CLI is `python -m sentinelaegisforge_control_plane.training.cli train|inspect`. The output defaults to ignored `.local/ml/runs/fraud-logistic-baseline-v1/`. These commands neither create Phase 12 snapshots nor start infrastructure.
