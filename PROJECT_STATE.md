# Project state

## Current Phase

Phase 3 — Kafka Wire Contract & Partitioning

The verified Phase 0, Phase 1, and Phase 2 foundations remain intact. Step 3 adds the canonical Avro transaction wire contract, explicit validated domain-to-wire mapping, compatibility tests, and the `customer_id` Kafka key decision. It does not add producer, consumer, or Schema Registry runtime behavior.

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
- Canonical Apache Avro transaction-event v1 schema.
- Explicit domain-to-Avro mapping and validation-preserving Avro-to-domain mapping.
- Deterministic local Avro binary round-trip with explicit decimal and timestamp precision rules.
- Apache Avro reader/writer schema compatibility tests.
- UTF-8 `customer_id` partition-key contract with documented ordering, skew, and partition-expansion boundaries.

No application producer, consumer, Schema Registry integration, streaming processing, fraud decisioning, application persistence, model lifecycle, or other runtime capability is implemented. The local Kafka runtime and topic lifecycle remain verified independently of application behavior.

## Current Architecture

- `streaming-engine`: Scala/JVM transaction domain contract, validator, deterministic simulator, Apache Avro mapping/local codec, and focused tests. It has no Kafka client, Spark, registry-backed serialization, or fraud-processing runtime.
- `model-control-plane`: Python package and temporary foundation import test only.
- Local infrastructure: one configured Apache Kafka 4.3.1 combined KRaft broker/controller and one explicitly provisioned application topic, `transactions.raw`.
- Shared contracts: one canonical Avro schema at `contracts/events/transaction-event-v1.avsc`.
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
- [ADR-005: Transaction event serialization](docs/adr/ADR-005-transaction-event-serialization.md)
- [ADR-006: Kafka transaction partition key](docs/adr/ADR-006-kafka-transaction-partition-key.md)

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

### Phase 2 runtime verification closure

- Docker Desktop 4.89.0 was reachable with Docker Engine 29.7.2 and Docker Compose v5.5.0.
- `docker compose config --quiet`: passed; the rendered configuration retained the pinned `apache/kafka:4.3.1` image, KRaft roles, expected listeners, named data volume, and single application-topic initializer.
- A clean `make infra-up` passed: Kafka became healthy, `kafka-init` completed, and `verify-infra` passed.
- Live topic metadata reported `transactions.raw` with 3 partitions, replication factor 1, leaders present, and in-sync replicas present.
- The live broker configuration reported `auto.create.topics.enable=false`, `process.roles=broker,controller`, the expected advertised listeners, and `/var/lib/kafka/data` as the configured log directory.
- The host listener answered a Kafka API metadata operation at `localhost:9092`; the internal listener supported all provisioning and verification operations at `kafka:19092`/`localhost:19092` from the Compose network/container.
- Container inspection reported `running`, `healthy`, and zero restarts. Broker logs contained no `ERROR`, `FATAL`, exception, or `WARN` entries and showed normal KRaft startup.
- Non-destructive `infra-down` removed the project container/network but retained `sentinelaegisforge_kafka-data`; the following `infra-up` preserved topic ID `cvX19qxwT3OmhxMQbrVYhw` and the expected 3-by-1 metadata.
- `infra-reset` removed only the project Compose resources and Kafka volume. A subsequent `infra-up` recreated the volume and topic with new topic ID `786Qr11kQF6hZ_5hA2ANJQ` and the expected 3-by-1 metadata.
- Rerunning `kafka-init` with the topic already present succeeded, preserved its topic ID and partition count, and was followed by a passing `verify-infra`.
- The final `infra-down` preserved the recreated Kafka volume. `make verify-scala`, all 10 explicit Scala tests, `make verify-python`, and `make verify` passed while no Kafka container was running.
- `git diff --check`: passed after the runtime-verification state update.

### Step 3 verification

- Apache Avro `1.12.2` resolved and compiled under Temurin JDK 21.0.12.1, Scala 2.13.18, and sbt 2.0.9.
- `sbt "clean ; compile"`: passed using the sbt 2 multi-command syntax.
- `sbt "Test / testOnly *"`: passed; 22 tests passed across 4 suites, including all prior Phase 1 tests and the new domain-wire, codec, compatibility, precision, and generator-integration tests.
- `sbt scalafmtCheckAll`: passed for 14 production and 4 test Scala sources.
- `make verify-scala`: passed under JDK 21; the immediately preceding forced suite had already run all 22 tests, so sbt's incremental test invocation had no changed tests to rerun.
- `make verify-python`: passed under Python 3.13.9; 1 pytest test passed, Ruff lint and formatting passed, and mypy reported no issues.
- `make verify`: passed for both independently buildable modules without requiring Kafka or Docker.
- `git diff --check`: passed before the final project-state update and was rerun afterward.

## Known Technical Debt

- The temporary Python foundation smoke test should be removed once substantive model-control-plane tests provide equivalent build-wiring coverage.

## Known Failures

No current Phase 0, Phase 1, Phase 2, or Phase 3 build, test, or runtime verification failures are known.

## Deferred Decisions

- Schema Registry runtime and final registry-backed Kafka framing.
- Kafka metadata model.
- Kafka producer acknowledgements, retries, idempotence, and producer/consumer integration.
- Production Kafka topology, replication, retention sizing, and security.
- Watermark and late-event semantics.
- Deduplication boundaries and policy.
- Dead-letter queue routing and behavior.
- Spark version and runtime dependencies.
- Delta/MinIO persistence and idempotency strategy.
- Streaming state design.
- ML runtime contract.

## Next Planned Capability

Phase 3 wire-contract and partitioning semantics are implemented and fully verified. No Phase 4 capability is implemented here.
