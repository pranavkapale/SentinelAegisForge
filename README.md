# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. It provides deterministic advisory risk decisions and, in Phase 12, reproducible offline feature/label snapshots with delayed-label point-in-time semantics. No trained model or enforcement behavior is implemented.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module with a validated transaction contract, deterministic simulator, bounded Kafka producer, Spark ingestion, local audit and deduplicated Delta tables, an independent customer activity lifecycle, rolling and statistical feature queries, and stateless explainable CLEAR/REVIEW decisions.
- **Module B — `model-control-plane`:** an independently buildable Python module with fixed-version Delta/Arrow feature reads and immutable offline snapshots joined to independent delayed outcome labels. Training, drift, evaluation and model governance remain deferred.

## Current status

The repository is in **Phase 12 — Offline ML Dataset Contract & Delayed Labels**. Module B reads corrected `transaction_statistical_features_v2` snapshots, resolves independent label revisions at an explicit AS_OF_TIME, excludes unlabeled events and writes Parquet plus a reproducibility manifest. CLEAR/REVIEW decisions are never labels or candidate ML inputs. Module A's currency-safe features, advisory decisions and earlier state/checkpoint semantics remain unchanged. No model training, random split or class balancing occurs.

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

`make infra-down` preserves local Kafka data. `make infra-reset` deliberately removes it. Infrastructure is not started by `make verify`.

See the [transaction event v1 contract](docs/architecture/transaction-event-v1.md), [transaction wire contract](docs/architecture/transaction-wire-contract.md), [transaction producer](docs/architecture/transaction-producer.md), [minimal streaming ingestion](docs/architecture/streaming-ingestion.md), [durable Delta ingestion](docs/architecture/delta-ingestion.md), [event-time deduplication](docs/architecture/event-time-deduplication.md), [customer stateful processing](docs/architecture/customer-stateful-processing.md), [customer rolling features](docs/architecture/customer-rolling-features.md), [customer statistical features](docs/architecture/customer-statistical-features.md), [local Kafka foundation](docs/architecture/local-kafka.md), [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
