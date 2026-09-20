# Project state

## Current Phase

Phase 2 — Local Kafka Infrastructure

The verified Phase 0 and Phase 1 foundations remain intact. Step 2 adds pinned local Kafka infrastructure configuration. Live broker verification is pending because the installed Docker Desktop daemon is not running.

## Implemented Capabilities

- Independently buildable Scala module foundation with pinned Scala and sbt versions.
- Independently buildable Python package foundation with a pinned Python range and locked development dependencies.
- Formatting, unit-test, lint, and static-analysis configuration.
- Root verification targets and baseline GitHub Actions CI.
- Architecture overview and lightweight ADR process.
- Typed `TransactionEvent` v1 domain model with distinct business-event and transaction identities.
- Untrusted `TransactionEventCandidate` representation and pure, accumulative typed validation.
- Deterministic seeded candidate generator with explicit base-time configuration.
- Reusable named invalid candidate scenarios for contract violations.
- Behavior-focused contract and simulator tests.
- Pinned `apache/kafka:4.3.1` single-node KRaft Compose configuration.
- Separate host (`localhost:9092`) and Compose-network (`kafka:19092`) broker listeners.
- Idempotent explicit provisioning configuration for `transactions.raw` with three partitions and replication factor one.
- Docker-managed broker data volume with separate stop and destructive reset commands.
- Infrastructure lifecycle and Kafka metadata verification interfaces.

No application producer, consumer, serialization, streaming processing, fraud decisioning, application persistence, model lifecycle, or other runtime capability is implemented. The Kafka runtime and topic configuration have not yet been exercised because the local Docker daemon is unavailable.

## Current Architecture

- `streaming-engine`: Scala/JVM transaction domain contract, validator, deterministic simulator, and focused tests. It has no Kafka, Spark, serialization, or fraud-processing runtime.
- `model-control-plane`: Python package and temporary foundation import test only.
- Local infrastructure: one configured Apache Kafka 4.3.1 combined KRaft broker/controller and one explicitly provisioned application topic, `transactions.raw`.
- `docs`: shared architecture overview and accepted ADRs.
- Repository root: shared verification, infrastructure lifecycle commands, hygiene, CI, and project-state metadata.

The modules have no runtime integration with Kafka or each other.

## Runtime Baseline

- `streaming-engine`: JDK 21 LTS with Scala 2.13.18 and sbt 2.0.9.
- `model-control-plane`: Python 3.13 with uv.

## Accepted ADRs

- [ADR-001: Monorepo module boundaries](docs/adr/ADR-001-monorepo-module-boundaries.md)
- [ADR-002: Development toolchains](docs/adr/ADR-002-development-toolchains.md)
- [ADR-003: Transaction event domain boundary](docs/adr/ADR-003-transaction-event-domain-boundary.md)
- [ADR-004: Local Kafka runtime](docs/adr/ADR-004-local-kafka-runtime.md)

## Verification Status

### Previous foundation verification

Before the runtime-baseline correction, the Python foundation was verified using CPython 3.12.14. `uv lock --check`, `uv sync --locked --group dev`, pytest, `ruff check`, `ruff format --check`, mypy, and `git diff --check` passed; the single Python foundation smoke test passed.

Previous Scala verification did not execute because local `sbt` was unavailable. This was a missing prerequisite, not a Scala build or test failure.

### Runtime-baseline migration verification

- `uv python find 3.13`: passed and resolved `/Library/Frameworks/Python.framework/Versions/3.13/bin/python3.13`.
- `uv lock --check`: passed using CPython 3.13.9; 12 packages resolved.
- `uv sync --locked --group dev`: passed using CPython 3.13.9.
- `uv run --locked python --version`: passed and reported Python 3.13.9.
- `uv run --locked pytest`: passed; 1 foundation smoke test passed.
- `uv run --locked ruff check .`: passed.
- `uv run --locked ruff format --check .`: passed; 2 files were already formatted.
- `uv run --locked mypy src tests`: passed with no issues in 2 source files.
- `make verify-python`: passed under Python 3.13.9.
- `sbt compile`, `sbt test`, and `sbt scalafmtCheckAll`: attempted but blocked before execution because local `sbt` is unavailable.
- `make verify-scala`: blocked because local `sbt` is unavailable.
- `make verify`: stopped at `verify-scala` because local `sbt` is unavailable; `verify-python` passed separately.
- JDK 21 verification is blocked because JDK 21 is not installed or selectable locally. The installed JDKs are versions 25 and 26, and neither is accepted as proof of the JDK 21 baseline.
- `git diff --check`: passed after the runtime-baseline correction.

The Scala and JDK blockers above describe the earlier runtime-baseline migration environment and were resolved before the final Phase 0 verification.

### Final Phase 0 runtime/build verification

- The project shell used Temurin JDK 21.0.12.1 from the repository's SDKMAN selection.
- The installed sbt runner reported version 2.0.9; the project now independently pins `sbt.version=2.0.9`.
- The original sbt-scalafmt 2.5.5 plugin had no published sbt 2 artifact. It was narrowly upgraded to sbt-scalafmt 2.6.2, which successfully resolved and loaded under sbt 2.0.9.
- The existing build syntax, Scala 2.13.18, ScalaTest 3.2.19, and Java 21 compiler options loaded successfully under sbt 2.0.9.
- `sbt clean` and `sbt compile`: passed under JDK 21 and sbt 2.0.9. The production source directory is intentionally empty in Phase 0.
- `sbt test`: passed under JDK 21 and sbt 2.0.9; 1 Scala foundation smoke test passed.
- `sbt scalafmtCheckAll`: passed for 1 Scala source.
- The compiled Scala smoke-test class has class-file major version 65, confirming the Java 21 bytecode target.
- `uv python find 3.13`, `uv lock --check`, and `uv sync --locked --group dev`: passed.
- `uv run --locked python --version`: reported Python 3.13.9.
- `uv run --locked pytest`: passed; 1 Python foundation smoke test passed.
- `uv run --locked ruff check .`, `uv run --locked ruff format --check .`, and `uv run --locked mypy src tests`: passed.
- `make verify-scala`, `make verify-python`, and `make verify`: passed. The Makefile and CI use sbt 2's required quoted, semicolon-separated multi-command syntax.
- `git diff --check`: passed after the sbt 2 migration and final verification updates.

### Step 1 verification

- `sbt "clean ; compile"`: passed under Temurin JDK 21.0.12.1, Scala 2.13.18, and sbt 2.0.9; 9 production Scala sources compiled.
- `sbt test`: passed; 10 tests passed across `TransactionEventValidatorSpec` and `DeterministicTransactionGeneratorSpec`.
- `sbt scalafmtCheckAll`: passed for 9 production and 2 test Scala sources.
- `make verify-scala`: passed under the explicitly selected JDK 21 environment.
- `make verify-python`: passed under Python 3.13.9; 1 pytest test passed, Ruff lint and format checks passed, and mypy reported no issues.
- `make verify`: passed for both independently buildable modules.
- `git diff --check`: passed after all Step 1 implementation and documentation changes.

### Step 2 verification

- `docker --version`: passed and reported Docker 29.7.2.
- `docker compose version`: passed and reported Docker Compose v5.5.0.
- `docker version`: the client reported version 29.7.2, but daemon access failed because the selected Docker Desktop socket does not exist.
- `docker compose config --quiet`: passed; the Compose model is syntactically valid.
- `bash -n scripts/verify-local-kafka.sh`: passed.
- `make infra-up`: blocked before container creation because the Docker daemon is not running.
- `make infra-status`: blocked because the Docker daemon is not running.
- `make verify-infra`: blocked and clearly reported that Kafka is unavailable.
- Normal-stop persistence, restart, topic metadata, and clean-reset/reprovision checks could not run without the Docker daemon.
- `make verify-scala`: passed under Temurin JDK 21.0.12.1 and sbt 2.0.9.
- Explicit `sbt "Test / testOnly *"`: passed; all 10 Phase 1 Scala tests passed across 2 suites.
- `make verify-python`: passed under Python 3.13.9; pytest, Ruff, and mypy passed.
- `make verify`: passed for both independently buildable modules and did not start infrastructure.
- `git diff --check`: passed after all Step 2 implementation and documentation changes.

## Known Technical Debt

- The temporary Python foundation smoke test should be removed once substantive model-control-plane tests provide equivalent build-wiring coverage.

## Known Failures

No current Phase 0 or Step 1 build or verification failures are known. Phase 2 live infrastructure verification is blocked by the unavailable Docker daemon; this is a local prerequisite issue, not an observed Kafka configuration failure.

## Deferred Decisions

- Serialization format and schema strategy.
- Schema Registry selection.
- Kafka partition key.
- Kafka metadata model.
- Kafka producer and consumer integration.
- Production Kafka topology, replication, retention sizing, and security.
- Watermark and late-event semantics.
- Deduplication boundaries and policy.
- Dead-letter queue routing and behavior.
- Spark version and runtime dependencies.
- Delta/MinIO persistence and idempotency strategy.
- Streaming state design.
- ML runtime contract.

## Next Planned Capability

Phase 2 configuration is implemented, but the local Kafka runtime remains unverified until a Docker daemon is available. No Phase 3 capability is implemented or selected here.
