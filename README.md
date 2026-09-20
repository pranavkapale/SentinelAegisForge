# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. Phase 1 adds a validated transaction event contract and deterministic simulator to the engineering foundation; no transaction streaming, fraud decisioning, or model lifecycle behavior is implemented yet.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module reserved for future real-time transaction processing, stateful features, fraud/risk decisioning, event-time behavior, idempotency, and recovery.
- **Module B — `model-control-plane`:** an independently buildable Python module reserved for future drift monitoring, delayed-label evaluation, retraining, model governance, and promotion or rollback.

## Current status

The repository is in **Phase 1 — Event Contract and Simulator**. Module A now provides a transport-independent transaction candidate, typed v1 validation boundary, and deterministic simulator. Kafka, Spark, serialization, and fraud behavior remain future architecture.

## Local development

Prerequisites:

- JDK 21 LTS
- sbt 2.0.9 (selected by the build)
- Python 3.13
- [uv](https://docs.astral.sh/uv/)
- GNU Make

Run all foundation checks from the repository root:

```sh
make verify
```

Run a module's checks independently with `make verify-scala` or `make verify-python`.

See the [transaction event v1 contract](docs/architecture/transaction-event-v1.md), [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
