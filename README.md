# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. It provides deterministic advisory risk decisions, reproducible offline feature/label snapshots, a local temporal logistic-regression baseline, a controlled synthetic corpus for full-path validation, local MLflow tracking, a technically verified offline model candidate, research-only Registry cataloging, validation-evidence dossiers, delayed-outcome monitoring, label-free offline distribution comparison and evidence-linked manual investigation packets. No production-approved model, serving or enforcement behavior is implemented.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module with a validated transaction contract, deterministic simulator, bounded Kafka producer, Spark ingestion, local audit and deduplicated Delta tables, an independent customer activity lifecycle, rolling and statistical feature queries, and stateless explainable CLEAR/REVIEW decisions.
- **Module B — `model-control-plane`:** an independently buildable Python module with fixed-version Delta/Arrow feature reads, immutable delayed-label snapshots, an offline baseline trainer, local MLflow experiment indexing, offline candidate reconstruction/validation, research-only Registry governance, validation-evidence dossiers, immutable offline prediction/outcome snapshots, descriptive feature/prediction distribution reports and manual investigation packets. Promotion, serving and automated drift alerts remain deferred.

## Current status

The repository is in **Phase 21 — Offline Operational Monitoring Triage**. Verified Phase 20 distribution evidence can be combined with compatible Phase 19 delayed-outcome snapshots and the Phase 18 dossier in an immutable, evidence-linked investigation packet and manual runbook. These are descriptive synthetic historical-replay diagnostics, not production drift alerts or proof of changed fraud quality. The Phase 18 outcome remains `NOT_ELIGIBLE_FOR_PROMOTION`; no model is approved or served.

## Local development

Prerequisites:

- JDK 21 LTS
- sbt 2.0.9 (selected by the build)
- Python 3.13
- [uv](https://docs.astral.sh/uv/)
- GNU Make
- Docker with Docker Compose (only for local Kafka infrastructure)

Run all foundation checks from the repository root:

```sh
make verify
```

Run a module's checks independently with `make verify-scala` or `make verify-python`.

Start and verify the local Kafka environment separately:

```sh
make infra-up
make infra-status
make verify-infra
make infra-down
```

Publish a deterministic sample while infrastructure is running:

```sh
make produce-sample COUNT=10 SEED=42 BASE_TIME=2026-09-21T00:00:00Z
```

Consume all records available to a fresh local checkpoint:

```sh
make consume-sample \
  CHECKPOINT_DIR=/tmp/sentinel-phase5-checkpoint \
  STARTING_OFFSETS=earliest
```

Reusing the checkpoint resumes from Spark's saved source progress. Delete or replace a checkpoint only as an intentional local test reset; checkpoint progress is not an exactly-once sink guarantee.

Run bounded durable ingestion and inspect the path-based table:

```sh
make ingest-delta
make inspect-delta
```

The durable query defaults to `.local/checkpoints/delta-transaction-ingestion`, `.local/delta/transactions_validated`, and transaction application ID `sentinel-transactions-validated-v1`. A new checkpoint lineage must use a new `DELTA_TXN_APP_ID`. `make reset-delta` deliberately removes only the default table and its coupled checkpoint.

Run the bounded semantic stage and inspect its separate output:

```sh
make deduplicate-transactions WATERMARK_DELAY="10 minutes"
make inspect-deduplicated
```

The deduplication query defaults to `.local/checkpoints/transaction-deduplication`, `.local/delta/transactions_deduplicated`, and transaction application ID `sentinel-transactions-deduplicated-v1`. Changing the watermark or state semantics requires checkpoint compatibility review and normally a new checkpoint/application-ID lineage. `make reset-deduplication` removes only this default semantic table and checkpoint.

Run and inspect the bounded customer-state stage:

```sh
make process-customer-state \
  WATERMARK_DELAY="10 minutes" \
  CUSTOMER_STATE_INACTIVITY="24 hours"
make inspect-customer-state
```

The state query defaults to `.local/checkpoints/customer-activity-state`, `.local/delta/customer_activity_snapshots`, and transaction application ID `sentinel-customer-activity-snapshots-v1`. A new state/checkpoint lineage requires a new transaction application ID. `make reset-customer-state` deliberately removes only these default Phase 8 paths.

Run and inspect the independent rolling feature stage:

```sh
make process-rolling-features WATERMARK_DELAY="10 minutes"
make inspect-rolling-features
```

The feature query defaults to `.local/checkpoints/customer-rolling-features-v2`, `.local/delta/transaction_customer_features_v2`, and transaction application ID `sentinel-transaction-customer-features-v2`. The five- and ten-minute feature windows are versioned definitions, while the ten-minute default watermark is a local development policy. A new state/checkpoint lineage requires a new transaction application ID. `make reset-rolling-features` removes only these v2 defaults, leaving old v1 tables/checkpoints untouched. Do not resume v1 state or append v2 features to v1 paths.

Run and inspect the downstream statistical feature stage:

```sh
make process-statistical-features \
  WATERMARK_DELAY="10 minutes" \
  STATISTICAL_FEATURE_INACTIVITY="24 hours"
make inspect-statistical-features
```

The default target is `.local/delta/transaction_statistical_features_v2`, checkpoint `.local/checkpoints/customer-statistical-features-v2`, and transaction application ID `sentinel-transaction-statistical-features-v2`. The inactivity timer resets each customer-currency baseline independently after watermark-driven expiry; ten-minute watermark delay is a local development policy. A new state/checkpoint lineage requires a new transaction application ID. `make reset-statistical-features` removes only these v2 defaults, leaving v1 history untouched. See [ADR-020](docs/adr/ADR-020-currency-safe-monetary-feature-semantics.md) for the feature semantic version change.

Run the stateless risk stage with an explicit **non-production verification policy**:

```sh
make process-risk-decisions \
  POLICY_VERSION=phase11-verification-v1 \
  HIGH_VELOCITY_THRESHOLD_5M=3 HIGH_AMOUNT_ZSCORE_THRESHOLD=2.0 \
  COMBINED_VELOCITY_THRESHOLD_5M=3 COMBINED_AMOUNT_ZSCORE_THRESHOLD=1.0
make inspect-risk-decisions
```

No policy defaults exist. Policy changes need explicit version/fingerprint and checkpoint/output-lineage review. See the [deterministic risk engine](docs/architecture/deterministic-risk-engine.md) for configuration, explainability and retry boundaries.

Build an offline snapshot from already materialized corrected features and independent labels:

```sh
make build-ml-dataset \
  ML_LABELS_PATH=/absolute/path/labels.jsonl \
  ML_AS_OF_TIME=2030-01-10T00:00:00Z
make inspect-ml-dataset ML_DATASET_SNAPSHOT_PATH=/absolute/path/<snapshot_id>
```

These commands do not start infrastructure. See the [offline dataset contract](docs/architecture/offline-ml-dataset.md) for source-version selection, label revisions, fingerprints and candidate feature boundaries.

Run the local training baseline only on an eligible immutable snapshot:

```sh
make train-baseline-model \
  BASELINE_DATASET_SNAPSHOT_PATH=/absolute/path/to/<snapshot_id> \
  BASELINE_TRAIN_END=2030-01-03T00:00:00Z \
  BASELINE_VALIDATION_END=2030-01-05T00:00:00Z \
  BASELINE_TEST_END=2030-01-07T00:00:00Z
make inspect-baseline-run BASELINE_RUN_PATH=/absolute/path/to/<training_run_id>
```

See the [temporal baseline contract](docs/architecture/offline-ml-baseline.md). Fixture metrics verify implementation mechanics only; no production model-quality claim is made.

Generate and inspect an isolated synthetic corpus before publishing any events:

```sh
make generate-synthetic-corpus SCENARIO_SEED=14027 SCENARIO_BASE_TIME=2030-01-01T00:00:00Z
make inspect-synthetic-corpus SCENARIO_CORPUS_PATH=/absolute/path/.local/ml/scenarios/<corpus_id>
```

Publishing requires the local Kafka/Schema Registry runtime and a fresh, isolated ingestion checkpoint initialized at `latest` before the first wave. Complete validated, deduplicated, rolling and statistical stages for each wave before advancing. The [synthetic corpus guide](docs/architecture/synthetic-fraud-corpus.md) describes the lineage, reconciliation and synthetic-only evaluation boundaries. Generated plans, private truth, labels, Delta tables and model-run outputs remain under ignored `.local/` storage.

Index an existing immutable training run without retraining:

```sh
make track-baseline-run BASELINE_RUN_PATH=/absolute/path/to/<training_run_id>
make inspect-tracked-run MLFLOW_RUN_ID=<provider-run-id>
make compare-tracked-runs
```

The optional `make mlflow-ui` binds to localhost; headless tracking needs no server. For verified Phase 14 corpus linkage, provide all five explicit corpus/Delta paths described in [MLflow experiment tracking](docs/architecture/mlflow-experiment-tracking.md). No model artifact or promotion is created.
Use the same lineage arguments on repeat tracking calls; mismatched or omitted provenance fails closed.

Build and inspect a local-only candidate from an already verified training run (optionally check its existing MLflow run read-only):

```sh
make build-model-candidate BASELINE_RUN_PATH=/absolute/path/to/<training_run_id> MLFLOW_RUN_ID=<provider-run-id>
make inspect-model-candidate MODEL_CANDIDATE_PATH=/absolute/path/to/<candidate_id>
```

`make score-model-candidate` accepts `MODEL_INPUT_JSON` containing exactly the 11 model features; monetary fields are decimal strings. It returns an uncalibrated offline FRAUD-class probability estimate, not an enforcement decision. See the [candidate artifact contract](docs/architecture/model-candidate-artifact.md).

Catalog a verified local candidate without retraining or promotion:

```sh
make register-model-candidate MODEL_CANDIDATE_PATH=/absolute/path/to/<candidate_id>
make inspect-registered-candidate REGISTERED_MODEL_VERSION=<explicit-version>
make verify-registered-model-artifact REGISTERED_MODEL_VERSION=<explicit-version>
```

`make compare-registered-candidates REGISTERED_MODEL_VERSIONS="1 2"` is read-only and does not select a winner. The [Registry governance contract](docs/architecture/model-registry-governance.md) explains local source retention, research-only eligibility and incomplete-registration handling.

Assess an exact registered version without retraining or Registry mutation:

```sh
make assess-registered-model REGISTERED_MODEL_VERSION=1
make inspect-model-assessment MODEL_ASSESSMENT_PATH=/absolute/path/to/<assessment_id>
```

`make compare-model-assessments MODEL_ASSESSMENT_PATHS="<path1> <path2>"` is read-only and never selects a model. See [model validation governance](docs/architecture/model-validation-governance.md) for the immutable dossier and synthetic-only limitations.

Replay a fixed corrected feature version, then evaluate independent outcomes at two explicit UTC cutoffs:

```sh
make build-prediction-ledger REGISTERED_MODEL_VERSION=1 \
  MONITORING_FEATURE_DELTA_PATH=/absolute/path/to/statistical \
  MONITORING_SOURCE_VERSION=2 \
  MONITORING_SCORE_START=2030-01-04T00:00:00Z \
  MONITORING_SCORE_END=2030-01-10T00:00:00Z
make evaluate-delayed-outcomes \
  MONITORING_LEDGER_PATH=/absolute/path/to/<ledger_id> \
  MONITORING_CORPUS_PATH=/absolute/path/to/<verified_corpus_id> \
  MONITORING_EVALUATION_AS_OF=2030-01-20T00:00:00Z
```

`make inspect-prediction-ledger`, `make inspect-monitoring-snapshot` and `make compare-monitoring-snapshots` are read-only. See [offline delayed-outcome monitoring](docs/architecture/offline-delayed-outcome-monitoring.md) for scope and denominator semantics. These operations do not train, serve or promote a model.

Compare two label-free windows from one existing ledger with `make build-drift-snapshot DRIFT_LEDGER_PATH=<absolute-ledger-path> DRIFT_REFERENCE_START=<UTC> DRIFT_REFERENCE_END=<UTC> DRIFT_COMPARISON_START=<UTC> DRIFT_COMPARISON_END=<UTC>`. `make inspect-drift-snapshot` and `make compare-drift-snapshots` are read-only. See [offline distribution monitoring](docs/architecture/offline-distribution-monitoring.md); no alerts or actions are generated.

Build a manual evidence packet with `make build-monitoring-investigation INVESTIGATION_DRIFT_PATH=<absolute-snapshot>`, optionally adding both Phase 19 T1/T2 paths and the compatible Phase 18 dossier. Inspect it with `make inspect-monitoring-investigation INVESTIGATION_PACKET_PATH=<absolute-packet>`. See [offline monitoring investigation](docs/architecture/offline-monitoring-investigation.md); neither operation rescores a model or triggers an action.

`make infra-down` preserves local Kafka data. `make infra-reset` deliberately removes it. Infrastructure is not started by `make verify`.

See the [transaction event v1 contract](docs/architecture/transaction-event-v1.md), [transaction wire contract](docs/architecture/transaction-wire-contract.md), [transaction producer](docs/architecture/transaction-producer.md), [minimal streaming ingestion](docs/architecture/streaming-ingestion.md), [durable Delta ingestion](docs/architecture/delta-ingestion.md), [event-time deduplication](docs/architecture/event-time-deduplication.md), [customer stateful processing](docs/architecture/customer-stateful-processing.md), [customer rolling features](docs/architecture/customer-rolling-features.md), [customer statistical features](docs/architecture/customer-statistical-features.md), [local Kafka foundation](docs/architecture/local-kafka.md), [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
