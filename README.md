# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. Phase 6 adds local durable Delta ingestion for validated registry-backed transaction records, with tested idempotent retry of a Spark micro-batch after a post-commit failure. It implements no business-event deduplication, fraud decisioning, or model lifecycle behavior.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module with a validated transaction contract, deterministic simulator, bounded Kafka producer, Spark ingestion, and a local Delta table for validated records plus Kafka coordinates. Stateful features, fraud/risk decisioning, event-time policy, and business deduplication remain future work.
- **Module B — `model-control-plane`:** an independently buildable Python module reserved for future drift monitoring, delayed-label evaluation, retraining, model governance, and promotion or rollback.

## Current status

The repository is in **Phase 6 — Durable Delta Ingestion & Idempotent Recovery**. Module A can publish bounded registry-backed Avro samples to `transactions.raw`, consume and validate them with Spark 4.2.0, and persist them to a local Delta 4.4.0 table with Kafka coordinates. Delta transaction IDs suppress a retry of the same Spark micro-batch; this is not an exactly-once or duplicate-free business-event claim.

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

`make infra-down` preserves local Kafka data. `make infra-reset` deliberately removes it. Infrastructure is not started by `make verify`.

See the [transaction event v1 contract](docs/architecture/transaction-event-v1.md), [transaction wire contract](docs/architecture/transaction-wire-contract.md), [transaction producer](docs/architecture/transaction-producer.md), [minimal streaming ingestion](docs/architecture/streaming-ingestion.md), [durable Delta ingestion](docs/architecture/delta-ingestion.md), [local Kafka foundation](docs/architecture/local-kafka.md), [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
