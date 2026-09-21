# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. Phase 3 adds a formal Avro transaction wire contract and customer-key partitioning semantics to the validated transaction contract, deterministic simulator, and local Kafka foundation; no transaction producer, streaming processing, fraud decisioning, or model lifecycle behavior is implemented yet.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module reserved for future real-time transaction processing, stateful features, fraud/risk decisioning, event-time behavior, idempotency, and recovery.
- **Module B — `model-control-plane`:** an independently buildable Python module reserved for future drift monitoring, delayed-label evaluation, retraining, model governance, and promotion or rollback.

## Current status

The repository is in **Phase 3 — Kafka Wire Contract & Partitioning**. Module A provides a transport-independent transaction candidate, typed v1 validation boundary, deterministic simulator, and explicit Apache Avro mapping with local binary round-trip. A single-node local Apache Kafka environment provisions `transactions.raw`, whose future record key is documented as UTF-8 `customer_id`; no producer, consumer, Schema Registry, Spark, or fraud behavior exists yet.

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

`make infra-down` preserves local Kafka data. `make infra-reset` deliberately removes it. Infrastructure is not started by `make verify`.

See the [transaction event v1 contract](docs/architecture/transaction-event-v1.md), [transaction wire contract](docs/architecture/transaction-wire-contract.md), [local Kafka foundation](docs/architecture/local-kafka.md), [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
