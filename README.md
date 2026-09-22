# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. Phase 4 adds explicit Schema Registry governance and a bounded producer that publishes deterministic validated transaction events to local Kafka using registry-backed Avro serialization. No consumer, streaming processing, fraud decisioning, or model lifecycle behavior is implemented yet.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module reserved for future real-time transaction processing, stateful features, fraud/risk decisioning, event-time behavior, idempotency, and recovery.
- **Module B — `model-control-plane`:** an independently buildable Python module reserved for future drift monitoring, delayed-label evaluation, retraining, model governance, and promotion or rollback.

## Current status

The repository is in **Phase 4 — Registry-Backed Transaction Producer**. Module A validates generated candidates, maps trusted events to the canonical Avro schema, and can publish a bounded sample to `transactions.raw` using UTF-8 `customer_id` keys. Local Schema Registry explicitly provisions `transactions.raw-value` with `BACKWARD_TRANSITIVE` compatibility. No consumer, Spark, fraud behavior, or exactly-once business guarantee exists.

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

`make infra-down` preserves local Kafka data. `make infra-reset` deliberately removes it. Infrastructure is not started by `make verify`.

See the [transaction event v1 contract](docs/architecture/transaction-event-v1.md), [transaction wire contract](docs/architecture/transaction-wire-contract.md), [transaction producer](docs/architecture/transaction-producer.md), [local Kafka foundation](docs/architecture/local-kafka.md), [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
