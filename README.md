# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. Phase 10.1 corrects rolling and prior-observed statistical monetary features to use same-currency history. It implements no fraud decisioning or model lifecycle behavior.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module with a validated transaction contract, deterministic simulator, bounded Kafka producer, Spark ingestion, local audit and deduplicated Delta tables, an independent customer activity lifecycle, and rolling and statistical feature queries. Fraud/risk decisioning remains future work.
- **Module B — `model-control-plane`:** an independently buildable Python module reserved for future drift monitoring, delayed-label evaluation, retraining, model governance, and promotion or rollback.

## Current status

The repository is in **Phase 10.1 — Currency-Safe Monetary Feature Compatibility Correction**. Module A consumes `transaction_customer_features` to append prior customer-and-currency amount count, mean, sample standard deviation, z-score, and readiness status to `transaction_statistical_features`. Velocity remains customer-wide; ten-minute sums include only the current currency. The current amount is scored before it updates the baseline; no fraud threshold or decision exists. Phase 9's event-time rolling semantics and Phase 10's prior-observed statistics are distinct. No FX conversion occurs. Fresh v2 paths and lineages avoid mixing corrected monetary semantics with v1 outputs. Outputs are not retroactively recomputed. Spark 4.2 requires RocksDB for these `transformWithState` queries; it is not configured as a performance optimization.

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

`make infra-down` preserves local Kafka data. `make infra-reset` deliberately removes it. Infrastructure is not started by `make verify`.

See the [transaction event v1 contract](docs/architecture/transaction-event-v1.md), [transaction wire contract](docs/architecture/transaction-wire-contract.md), [transaction producer](docs/architecture/transaction-producer.md), [minimal streaming ingestion](docs/architecture/streaming-ingestion.md), [durable Delta ingestion](docs/architecture/delta-ingestion.md), [event-time deduplication](docs/architecture/event-time-deduplication.md), [customer stateful processing](docs/architecture/customer-stateful-processing.md), [customer rolling features](docs/architecture/customer-rolling-features.md), [customer statistical features](docs/architecture/customer-statistical-features.md), [local Kafka foundation](docs/architecture/local-kafka.md), [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
