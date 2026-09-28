# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. Phase 9 adds two prior-only rolling customer features from the deduplicated Delta boundary while retaining the separate Phase 8 customer lifecycle. It implements no fraud decisioning or model lifecycle behavior.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module with a validated transaction contract, deterministic simulator, bounded Kafka producer, Spark ingestion, local audit and deduplicated Delta tables, an independent customer activity lifecycle, and a rolling feature query. Fraud/risk decisioning remains future work.
- **Module B — `model-control-plane`:** an independently buildable Python module reserved for future drift monitoring, delayed-label evaluation, retraining, model governance, and promotion or rollback.

## Current status

The repository is in **Phase 9 — Leakage-Safe Rolling Customer Features**. Module A independently consumes `transactions_deduplicated` to append per-event `prior_transaction_count_5m` and `prior_amount_sum_10m` to `transaction_customer_features`. Current events and equal-time peers are excluded from their own prior history. Outputs reflect online arrival order and are not retroactively recomputed. Spark 4.2 requires RocksDB for the Phase 8 and Phase 9 `transformWithState` queries; neither is configured as a performance optimization.

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

The feature query defaults to `.local/checkpoints/customer-rolling-features`, `.local/delta/transaction_customer_features`, and transaction application ID `sentinel-transaction-customer-features-v1`. The five- and ten-minute feature windows are versioned definitions, while the ten-minute default watermark is a local development policy. A new state/checkpoint lineage requires a new transaction application ID. `make reset-rolling-features` removes only these default Phase 9 paths.

`make infra-down` preserves local Kafka data. `make infra-reset` deliberately removes it. Infrastructure is not started by `make verify`.

See the [transaction event v1 contract](docs/architecture/transaction-event-v1.md), [transaction wire contract](docs/architecture/transaction-wire-contract.md), [transaction producer](docs/architecture/transaction-producer.md), [minimal streaming ingestion](docs/architecture/streaming-ingestion.md), [durable Delta ingestion](docs/architecture/delta-ingestion.md), [event-time deduplication](docs/architecture/event-time-deduplication.md), [customer stateful processing](docs/architecture/customer-stateful-processing.md), [customer rolling features](docs/architecture/customer-rolling-features.md), [local Kafka foundation](docs/architecture/local-kafka.md), [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
