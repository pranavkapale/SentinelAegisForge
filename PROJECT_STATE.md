# Project state

## Current Phase

Phase 4 — Registry-Backed Transaction Producer

The verified Phase 0 through Phase 3 foundations remain intact. Step 4 adds a governed local Schema Registry subject and a bounded producer that validates deterministic transactions, maps them to canonical Avro records, and publishes them to `transactions.raw` using the documented `customer_id` key. It does not add a consumer, stream processing, or business decisioning.

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
- Pinned local Schema Registry 8.3.2 runtime backed by Kafka's `_schemas` topic at replication factor one.
- Explicit, idempotent registration of canonical schema version 1 under `transactions.raw-value` using `TopicNameStrategy` and `BACKWARD_TRANSITIVE` compatibility.
- Kafka Avro serializer integration with schema auto-registration disabled.
- Bounded deterministic transaction producer using UTF-8 `customer_id` keys, `acks=all`, producer idempotence, explicit acknowledgement accounting, and controlled failure reporting.
- Live production of 10 validated transactions with 10 broker acknowledgements and an observed aggregate offset increase of 10.

No application consumer, stream processing, fraud decisioning, application persistence, model lifecycle, or other later-phase runtime capability is implemented. Producer idempotence is a Kafka delivery safeguard and is not an exactly-once business-processing claim.

## Current Architecture

- `streaming-engine`: Scala/JVM transaction domain contract, validator, deterministic simulator, Apache Avro mapping/local codec, and a bounded registry-backed Kafka producer. It has no Kafka consumer, Spark, or fraud-processing runtime.
- `model-control-plane`: Python package and temporary foundation import test only.
- Local infrastructure: one configured Apache Kafka 4.3.1 combined KRaft broker/controller, one explicitly provisioned application topic (`transactions.raw`), and Schema Registry 8.3.2 with one governed value subject (`transactions.raw-value`).
- Shared contracts: one canonical Avro schema at `contracts/events/transaction-event-v1.avsc`.
- `docs`: shared architecture overview and accepted ADRs.
- Repository root: shared verification, infrastructure lifecycle commands, hygiene, CI, and project-state metadata.

The streaming module can publish bounded validated samples to local Kafka. There is no consumer path and the two modules have no runtime integration with each other.

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
- [ADR-007: Schema Registry governance](docs/adr/ADR-007-schema-registry-governance.md)
- [ADR-008: Kafka producer delivery semantics](docs/adr/ADR-008-kafka-producer-delivery-semantics.md)

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

### Step 4 verification

- Docker Desktop 4.89.0 was reachable with Docker Engine/CLI 29.7.2 and Docker Compose v5.5.0.
- `docker compose config --quiet`: passed. Apache Kafka remains pinned to `apache/kafka:4.3.1`; Schema Registry is pinned to `confluentinc/cp-schema-registry:8.3.2` and uses `kafka:19092` for metadata storage.
- Initial live startup exposed that the Schema Registry image does not contain `curl`. Its readiness probe was narrowly corrected to use the image's existing Python standard library HTTP client; the service then reached `healthy` status.
- `make infra-up`, `make infra-status`, and `make verify-infra`: passed. Live metadata reported `transactions.raw` at 3 partitions and replication factor 1, `_schemas` at replication factor 1, and `transactions.raw-value` version 1 / schema ID 1 with `BACKWARD_TRANSITIVE` compatibility.
- Re-running explicit schema provisioning succeeded without adding a version: the subject remained at versions `[1]` and schema ID 1.
- Effective Scala dependency inspection reported `org.apache.kafka:kafka-clients:4.3.1`, `org.apache.avro:avro:1.12.2`, and `io.confluent:kafka-avro-serializer:8.3.2`. The serializer's Confluent-patched Kafka client was excluded so the project retains the required Apache Kafka client 4.3.1; transitive Avro 1.12.1 was evicted by direct Avro 1.12.2.
- A deterministic live batch with count 10, seed 4242, and base time `2026-09-21T00:00:00Z` reported 10 acknowledgements and zero failures. Aggregate topic end offsets increased from 0 to 10 (`0/0/0` to `1/8/1`). A CLI inspection observed UTF-8 key `customer-3178` and value prefix `0000000001` (Confluent framing magic byte 0 and schema ID 1).
- `infra-down` retained the project volume, all 10 records, subject version 1 / ID 1, and its compatibility setting. The following `infra-up` passed verification.
- `infra-reset` removed only the project Compose volume. The following `infra-up` recreated `transactions.raw`, `_schemas`, and `transactions.raw-value` version 1 / ID 1 with `BACKWARD_TRANSITIVE`; application-topic offsets correctly returned to zero.
- `sbt compile`: passed under Temurin JDK 21.0.12.1, Scala 2.13.18, and sbt 2.0.9.
- Forced `sbt "Test / testOnly *"`: passed; all 27 tests passed across 7 suites, including all 22 prior tests and 5 new producer tests.
- `sbt scalafmtCheckAll`: passed for all configured Scala and sbt sources.
- `make verify-scala`: passed with infrastructure stopped. The preceding forced suite executed all tests; sbt's incremental invocation had no changed tests to rerun.
- `make verify-python`: passed under Python 3.13.9; pytest, Ruff lint/format, and mypy passed.
- `make verify`: passed for both modules with Kafka and Schema Registry stopped.
- `git diff --check`: passed after the final project-state update.

## Known Technical Debt

- The temporary Python foundation smoke test should be removed once substantive model-control-plane tests provide equivalent build-wiring coverage.
- Module A does not yet select an application logging backend. Kafka/registry libraries therefore emit the standard SLF4J no-provider warning and use the no-operation fallback; the producer's explicit acknowledgement and failure reporting remains functional.

## Known Failures

No current Phase 0 through Phase 4 build, test, or runtime verification failures are known.

## Deferred Decisions

- Kafka metadata model.
- Kafka consumer and Spark ingestion.
- Dead-letter queue and invalid-event transport behavior.
- Kafka transactions and any future atomic multi-record/multi-topic boundary.
- Production Kafka topology, replication, retention sizing, and security.
- Watermark and late-event semantics.
- Business-event deduplication boundaries and policy.
- Validated-event and decision topics.
- Spark version and runtime dependencies.
- Delta/MinIO persistence and idempotency strategy.
- Streaming state design.
- ML runtime contract.
- Observability and reproducible performance benchmarking.

## Next Planned Capability

Phase 4 registry-backed transaction production is implemented and fully verified. No subsequent-phase capability is implemented here.
