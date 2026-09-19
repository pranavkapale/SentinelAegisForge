# SentinelAegisForge

SentinelAegisForge is intended to become an enterprise-style platform for real-time fraud intelligence and model lifecycle management. Phase 0 establishes reproducible Scala and Python engineering foundations only; no transaction processing, fraud decisioning, streaming infrastructure, or model lifecycle behavior is implemented yet.

## Modules

- **Module A — `streaming-engine`:** an independently buildable Scala/JVM module reserved for future real-time transaction processing, stateful features, fraud/risk decisioning, event-time behavior, idempotency, and recovery.
- **Module B — `model-control-plane`:** an independently buildable Python module reserved for future drift monitoring, delayed-label evaluation, retraining, model governance, and promotion or rollback.

## Current status

The repository is in **Phase 0 — Engineering Foundation**. It currently provides pinned build tools, formatting, static analysis, smoke tests, baseline CI, architecture documentation, and ADRs. The runtime technologies and domain behavior described above remain future architecture.

## Local development

Prerequisites:

- JDK 21 LTS
- sbt 1.13.0 (selected by the build)
- Python 3.13
- [uv](https://docs.astral.sh/uv/)
- GNU Make

Run all foundation checks from the repository root:

```sh
make verify
```

Run a module's checks independently with `make verify-scala` or `make verify-python`.

See the [architecture overview](docs/architecture/overview.md), [architecture decision records](docs/adr/README.md), and [current project state](PROJECT_STATE.md).
