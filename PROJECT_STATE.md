# Project state

## Current Phase

Phase 6 — Durable Delta Ingestion & Idempotent Recovery

The verified Phase 0 through Phase 5 foundations remain intact. Step 6 adds a local path-based Delta table for validated Kafka records and an explicit idempotent micro-batch retry boundary. It adds no business-event deduplication, event-time policy, stateful features, or fraud decisioning.

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
- Apache Spark 4.2.0 local Structured Streaming runtime with the matching Kafka source connector.
- `transactions.raw` key/value byte ingestion with topic, partition, offset, and broker timestamp preservation.
- Official registry-aware Avro deserialization with one deserializer per Spark task partition and no application-managed schema IDs or framing.
- Validation-preserving decoded-record mapping and strict Kafka-key/customer-ID consistency checks.
- Typed, visible ingestion failures for malformed keys, values, domain data, and key/customer mismatches.
- Bounded `Trigger.AvailableNow` diagnostic ingestion with an explicit checkpoint location.
- Live checkpoint-progress proof: 10 records processed, immediate same-checkpoint rerun processed 0, then 5 newly published records processed with that checkpoint.
- Pinned Delta Lake 4.4.0 integration for Spark 4.2 and Scala 2.13.
- Explicit snake_case `transactions_validated` storage schema with `decimal(18,4)` amounts, UTC timestamps, and retained Kafka key/topic/partition/offset/timestamp.
- Local path-based Delta persistence with no physical partition columns or automatic schema evolution.
- Durable `foreachBatch` sink using stable configured `txnAppId` and `txnVersion=batchId`.
- Dedicated durable-ingestion checkpoint and explicit checkpoint/application-ID lifecycle rule.
- Diagnostic post-commit failpoint and verified restart suppression of a retried Delta micro-batch.
- Delta table inspection and safe default local table/checkpoint reset interfaces.

No business-event deduplication, DLQ, event-time/stateful processing, fraud decisioning, model lifecycle, or other later-phase runtime capability is implemented. Producer idempotence, Spark checkpoint source progress, and Delta micro-batch transaction suppression are distinct boundaries; none is an unconditional exactly-once business-processing claim.

## Current Architecture

- `streaming-engine`: Scala/JVM transaction domain contract, validator, deterministic simulator, Apache Avro mapping/local codec, bounded registry-backed Kafka producer, Spark Structured Streaming consumer, and local Delta durable sink. The durable table contains validated records plus Kafka coordinates; the module has no business deduplication, state, or fraud-processing runtime.
- `model-control-plane`: Python package and temporary foundation import test only.
- Local infrastructure: one configured Apache Kafka 4.3.1 combined KRaft broker/controller, one explicitly provisioned application topic (`transactions.raw`), and Schema Registry 8.3.2 with one governed value subject (`transactions.raw-value`).
- Shared contracts: one canonical Avro schema at `contracts/events/transaction-event-v1.avsc`.
- `docs`: shared architecture overview and accepted ADRs.
- Repository root: shared verification, infrastructure lifecycle commands, hygiene, CI, and project-state metadata.

The streaming module can publish bounded validated samples to local Kafka, consume them through Spark, and persist validated records plus transport metadata to a local Delta table. The two runtime modules have no integration with each other.

## Runtime Baseline

- `streaming-engine`: JDK 21 LTS with Scala 2.13.18, sbt 2.0.9, Spark 4.2.0, and Delta Lake 4.4.0 for the local ingestion runtime.
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
- [ADR-009: Spark Structured Streaming ingestion](docs/adr/ADR-009-spark-structured-streaming-ingestion.md)
- [ADR-010: Delta durable ingestion](docs/adr/ADR-010-delta-durable-ingestion.md)
- [ADR-011: Delta streaming idempotency](docs/adr/ADR-011-delta-streaming-idempotency.md)

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

### Step 5 verification

- Spark SQL and the Spark SQL Kafka connector resolved at `4.2.0` under Temurin JDK 21.0.12.1, Scala 2.13.18, and sbt 2.0.9.
- The first local Spark test exposed a real Jackson incompatibility: Confluent selected databind 2.22.1 while Spark's Scala module 2.21.2 requires databind below 2.22. The build now narrowly overrides the relevant Jackson components to Spark's 2.21.x line; the Confluent mock-registry decoder, producer tests, local Spark transformation, and live registry path all passed afterward.
- Dependency inspection reported effective `spark-sql_2.13:4.2.0`, `spark-sql-kafka-0-10_2.13:4.2.0`, `kafka-clients:4.3.1`, `avro:1.12.2`, and `kafka-avro-serializer:8.3.2` (which supplies both serializer and deserializer). The connector's Kafka client 3.9.2 is explicitly excluded in favor of Module A's existing direct 4.3.1 pin; live Structured Streaming consumption passed with that client. Spark/Confluent Avro 1.12.1 is evicted by the direct 1.12.2 pin.
- `sbt "clean ; compile"`: passed; all 29 production Scala sources compiled with infrastructure stopped.
- Forced `sbt "Test / testOnly *"`: passed; all 34 tests passed across 9 suites, including all 27 prior tests and 7 new registry-decoder and local-Spark tests. The Spark test used `local[2]` with a mock Schema Registry URL and required neither Docker nor HTTP.
- `sbt scalafmtCheckAll`: passed for all configured Scala and sbt sources.
- Clean live infrastructure began with `transactions.raw` end offsets `0/0/0`. Publishing 10 deterministic records produced partition/offset ranges `0:0`, `1:0-7`, and `2:0`; the fresh AvailableNow query processed all 10 with zero ingestion failures.
- An immediate AvailableNow rerun with the same `/tmp/sentinel-phase5-checkpoint.sDzV2g` checkpoint and no publication processed 0 records.
- Publishing 5 more deterministic records produced partition/offset ranges `0:1` and `1:8-11`; reusing the same checkpoint processed exactly those 5. Final topic end offsets were `2/12/1`, totaling 15 records.
- This live sequence proves basic checkpoint-managed Kafka source progress. It does not prove crash recovery, a durable/idempotent sink, business deduplication, or end-to-end exactly-once behavior.
- Normal `make infra-down` stopped Kafka and Schema Registry after live verification and retained `sentinelaegisforge_kafka-data`.
- `make verify-scala`, `make verify-python`, and `make verify`: passed with infrastructure stopped. The explicit forced Scala run provides the 34-test evidence because sbt 2's subsequent incremental `test` invocation correctly had no changed tests to rerun.
- `git diff --check`: passed after the final Step 5 implementation and project-state update.

### Step 6 verification

- Apache Delta Lake `io.delta:delta-spark_4.2_2.13:4.4.0` resolved under Temurin JDK 21.0.12.1, Scala 2.13.18, sbt 2.0.9, and Spark 4.2.0. No Spark downgrade or new dependency override was required.
- Effective dependency inspection retained Spark SQL/Kafka connector 4.2.0, Kafka client 4.3.1, Avro 1.12.2, Confluent serializer 8.3.2, and the existing Jackson 2.21.x overrides. Delta added its 4.4.0 Spark/kernel/storage graph; reported transitive version selections were exercised successfully by local and live tests.
- `sbt "clean ; compile"`: passed; 34 production Scala sources compiled under JDK 21.
- Forced `sbt "Test / testOnly *"`: passed; all 37 tests passed across 10 suites. The 3 new local Delta tests verified the explicit storage schema, transaction-option planning, unpartitioned table detail, post-commit failure, same-transaction retry suppression, and next-transaction append without Docker.
- `sbt scalafmtCheckAll`: passed for all configured Scala and sbt sources.
- Live verification started from a clean Kafka/Schema Registry volume and used isolated paths under `/tmp/sentinel-phase6.UxU5Ug` with transaction application ID `sentinel-phase6-recovery-v1`.
- Publishing 10 deterministic records (seed 4242) produced 10 acknowledgements. Batch 0 wrote 10 Delta rows, then the diagnostic hook intentionally failed before Spark recorded batch completion. Inspection immediately after failure showed 10 rows and exactly one Delta history entry (version 0, 10 output rows).
- Restarting with the same checkpoint and `txnAppId` retried batch 0 successfully. Delta row count remained 10 and history remained at version 0, proving duplicate transaction suppression at this sink boundary. A no-new-record rerun also left the table unchanged.
- Publishing 5 additional deterministic records (seed 5151) and reusing the lineage wrote batch 1. Final inspection reported 15 rows and two history entries: version 0 with 10 output rows and version 1 with 5 output rows. A final no-new-record rerun completed without another write.
- Delta log transaction actions recorded `appId=sentinel-phase6-recovery-v1` at versions 0 and 1, matching the Spark micro-batch IDs.
- Normal `make infra-down` stopped Kafka and Schema Registry after live verification while retaining the project Kafka volume and the isolated local Delta data.
- `make verify-scala`, `make verify-python`, and `make verify`: passed with infrastructure stopped. `git diff --check` passed after the final Phase 6 updates.

## Known Technical Debt

- The temporary Python foundation smoke test should be removed once substantive model-control-plane tests provide equivalent build-wiring coverage.
- Spark's transitive graph reports minor Netty 4.2.13-over-4.2.9 and SLF4J 2.0.18-over-2.0.17/1.7.36 eviction warnings. The local Spark test, prior producer tests, and live producer/consumer path pass; no speculative override was added without an observed defect.

## Known Failures

No current Phase 0 through Phase 6 build, test, or runtime verification failures are known.

## Deferred Decisions

- Durable Kafka metadata model beyond the current validated streaming record.
- Dead-letter queue contract, topic, routing, and invalid-event reprocessing behavior.
- Kafka transactions and any future atomic multi-record/multi-topic boundary.
- Production Kafka topology, replication, retention sizing, and security.
- Watermark and late-event semantics.
- Business-event deduplication boundaries and policy.
- Validated-event and decision topics.
- Business-event deduplication identity, replay boundaries, and policy beyond micro-batch retry suppression.
- MinIO/S3-compatible object storage and production Delta deployment topology.
- Multi-sink atomicity and recovery semantics beyond the current single Delta table.
- Streaming state design, state-store selection, and RocksDB evaluation.
- ML runtime contract.
- Observability and reproducible performance benchmarking.

## Next Planned Capability

Phase 6 durable Delta ingestion and idempotent micro-batch recovery are implemented and fully verified. No subsequent-phase capability is implemented here.
