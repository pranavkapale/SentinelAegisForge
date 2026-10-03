# Project state

## Current Phase

Phase 11 — Deterministic Explainable Risk Decision Engine

Phases 0–10.1 remain preserved and verified. The stateless decision layer consumes only corrected `transaction_statistical_features_v2`, evaluates three explicit deterministic rules and durably appends advisory CLEAR/REVIEW decisions with ordered explanations and exact policy identity. Mixed-currency full-path evidence and Docker-independent regression verification passed. No ML, calibrated production policy or enforcement is implemented.

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
- Separate `transactions_deduplicated` Delta table with the same explicit event and Kafka-metadata schema as the validated audit table.
- Configurable `event_time` watermark followed by `dropDuplicatesWithinWatermark("event_id")` using a dedicated checkpoint lineage.
- First-accepted-occurrence semantics for duplicate business event IDs, independent of Kafka coordinates.
- Dedicated retry-safe deduplicated-table sink using stable `txnAppId` and `txnVersion=batchId` transaction identities.
- AvailableNow progress reporting for watermark, state rows updated/total/removed, rows dropped by watermark, and state memory.
- Docker-independent local Spark/Delta tests for duplicate suppression, watermark advancement, too-late drops, eligible out-of-order data, retained first-occurrence metadata, and sink retry safety.
- Spark 4.2 arbitrary state API v2 processing with `transformWithState`, `StatefulProcessor`, `TimeMode.EventTime`, and `OutputMode.Update`.
- One constant-sized `ValueState[CustomerActivityState]` named `customerActivity` per active `customer_id`.
- Exact `decimal(38,18)` numeric amount totals in the preserved Phase 8 lifecycle diagnostic, checked count arithmetic, order-independent first/latest event-time updates, and explicit overflow failures. Those legacy totals lack currency context and are not monetary decision features.
- Replaceable event-time inactivity timers guarded by the authoritative stored timer timestamp, with state-variable TTL disabled.
- Durable `UPDATED` and `EXPIRED` customer lifecycle snapshots in an unpartitioned `customer_activity_snapshots` Delta table.
- Dedicated customer-state checkpoint and retry-safe snapshot sink using stable `txnAppId` plus `txnVersion=batchId`.
- Watermark, state-row, state-memory, shuffle-partition, state-store-instance, and custom state-metric reporting.
- Direct `TwsTester` state-machine tests plus a real Docker-independent local Spark/Delta checkpoint-recovery test.
- Narrow Phase 8 RocksDB state-store configuration required by Spark 4.2 streaming `transformWithState`; earlier query sessions remain unchanged.
- Independent customer-keyed Phase 9 `transformWithState` query: customer-wide `prior_transaction_count_5m` and same-current-currency `prior_amount_sum_10m`, using inclusive lower and exclusive upper event-time bounds.
- Feature-before-update processing with equal-timestamp peer exclusion and event-time filtering of already stored future records.
- Minimal `rollingEvents` MapState keyed by `event_id` retaining event time, amount, and currency, one authoritative `nextCleanupTimerMs` ValueState per active customer, and watermark-driven ten-minute history cleanup.
- Microsecond-precise window comparisons, upward-rounded millisecond cleanup timers, and checked exact `decimal(38,4)` feature sums.
- Append-only, unpartitioned `transaction_customer_features_v2` Delta output retaining all 18 source event and Kafka lineage fields plus two feature columns.
- Dedicated Phase 9 checkpoint and retry-safe Delta sink application ID; RocksDB is selected only for this Spark 4.2 `transformWithState` application and its integration test session.
- `TwsTester`, temporary Spark/Delta, and controlled live evidence for online feature semantics, checkpoint recovery, out-of-order arrivals, state cleanup, and sink retry protection.
- Independent customer-currency-keyed Phase 10 `transformWithState` query consuming `transaction_customer_features_v2` and appending prior amount count, mean, sample standard deviation, z-score, and explicit status to `transaction_statistical_features_v2`.
- One constant-sized `ValueState[CustomerAmountStatistics]` per active `CustomerCurrencyKey(customerId, currency)` using Welford count/mean/M2, finite-value checks, score-before-update order, and deterministic Kafka partition/offset observation order.
- Explicit failure on active customer-currency Kafka partition remapping or non-advancing offset; late event-time rows can use previously observed later event-time amounts without rewriting earlier output.
- Independent replaceable event-time inactivity timers per customer-currency pair, watermark-driven state expiry/reset, and stale-callback protection with state-variable TTL disabled.
- Append-only, unpartitioned 25-column statistical Delta output preserving all 20 Phase 9 fields, exact decimal amounts, Kafka lineage, and a dedicated checkpoint and retry-safe Delta transaction application ID.
- Docker-independent processor and local Spark/Delta tests plus a controlled live five-event path through producer, Kafka, validation, deduplication, rolling features, and statistical features.

- Currency-safe feature semantic v2 with dedicated `customer-rolling-features-v2` / `customer-statistical-features-v2` checkpoints and `sentinel-transaction-customer-features-v2` / `sentinel-transaction-statistical-features-v2` transaction application IDs; reset interfaces affect only v2 defaults.
- Nine additional mixed-currency tests covering rolling membership, unchanged temporal bounds/peers, per-currency Welford baselines, independent expiry/reactivation, and temporary Delta checkpoint recovery.

- Pure deterministic R001 customer-wide velocity, R002 positive READY amount z-score and R003 combined rules, returning only advisory CLEAR/REVIEW with all overlapping matches in canonical order.
- Explicit immutable policy with required valid thresholds, human semantic version and SHA-256 fingerprint over exact canonical rule/configuration identity; no hidden policy defaults.
- Typed invalid-feature errors and fail-fast Spark mapping for impossible readiness, missing counts/status, negative counts/stddev and nonfinite statistics.
- Stateless AvailableNow Delta query from the corrected 25-column statistical boundary to unpartitioned 31-column `transaction_risk_decisions`, preserving every original field and adding native rule/reason arrays and policy identity.
- Dedicated risk checkpoint and Delta sink transaction identity with same-checkpoint resume and same-app/batch retry evidence.
- Nineteen new pure/configuration and temporary Spark/Delta tests, plus a real five-event mixed-currency full-path run demonstrating CLEAR and REVIEW without additional monetary aggregation.

No DLQ, late-event side output, merchant/device cardinality state, numeric composite risk score, ML, model lifecycle or enforcement capability is implemented. Producer idempotence, Spark checkpoint source progress, Delta micro-batch transaction suppression, watermark-bounded business-event deduplication, customer feature state and deterministic advisory decisions are distinct boundaries; none is an unconditional exactly-once business-processing claim.

## Current Architecture

- `streaming-engine`: Scala/JVM transaction domain contract, validator, deterministic simulator, Apache Avro mapping/local codec, bounded registry-backed Kafka producer, Spark Structured Streaming consumer, a validated-record Delta audit sink, a separate event-time/watermark-bounded event-ID deduplication sink, a customer-keyed activity-state lifecycle, a prior-only rolling-feature query, a downstream prior-observed statistical-feature query and a stateless deterministic advisory risk layer. No ML or payment enforcement exists.
- `model-control-plane`: Python package and temporary foundation import test only.
- Local infrastructure: one configured Apache Kafka 4.3.1 combined KRaft broker/controller, one explicitly provisioned application topic (`transactions.raw`), and Schema Registry 8.3.2 with one governed value subject (`transactions.raw-value`).
- Shared contracts: one canonical Avro schema at `contracts/events/transaction-event-v1.avsc`.
- `docs`: shared architecture overview and accepted ADRs.
- Repository root: shared verification, infrastructure lifecycle commands, hygiene, CI, and project-state metadata.

The streaming module can publish bounded validated samples to local Kafka, consume them through Spark, persist every validated record plus transport metadata, derive a durable watermark-bounded deduplicated table, and run independent customer activity and rolling-feature queries over that semantic source. The Phase 10 statistical query consumes the corrected Phase 9 v2 rolling-feature table. Behavioral velocity uses the customer dimension; raw monetary features use customer+currency. Kafka stays keyed by customer; currency switching leaves routing unchanged. The existing `currency` column denominates sums, means, and standard deviations. Phase 11 reads only `transaction_statistical_features_v2` and appends decisions to `transaction_risk_decisions`, with no stateful operator, watermark, RocksDB configuration or feature mutation. Phase 8 diagnostic amounts remain excluded. The two runtime modules have no integration with each other.

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
- [ADR-012: Business-event deduplication](docs/adr/ADR-012-business-event-deduplication.md)
- [ADR-013: Event-time watermark semantics](docs/adr/ADR-013-event-time-watermark-semantics.md)
- [ADR-014: Customer stateful processing](docs/adr/ADR-014-customer-stateful-processing.md)
- [ADR-015: Customer state expiration](docs/adr/ADR-015-customer-state-expiration.md)
- [ADR-016: Customer rolling feature semantics](docs/adr/ADR-016-customer-rolling-feature-semantics.md)
- [ADR-017: Rolling feature state retention](docs/adr/ADR-017-rolling-feature-state-retention.md)
- [ADR-018: Customer statistical feature semantics](docs/adr/ADR-018-customer-statistical-feature-semantics.md)
- [ADR-019: Customer statistical state lifecycle](docs/adr/ADR-019-customer-statistical-state-lifecycle.md)
- [ADR-020: Currency-safe monetary feature semantics](docs/adr/ADR-020-currency-safe-monetary-feature-semantics.md)
- [ADR-021: Deterministic risk policy](docs/adr/ADR-021-deterministic-risk-policy.md)
- [ADR-022: Risk policy identity and evolution](docs/adr/ADR-022-risk-policy-versioning.md)

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

### Phase 7 verification

- Effective runtime remained Temurin JDK 21.0.12.1, Scala 2.13.18, sbt 2.0.9, Spark 4.2.0, and Delta Lake 4.4.0. `sbt evicted` passed before implementation; no direct dependency was added.
- `sbt "clean ; compile"`: passed under JDK 21. `sbt scalafmtCheckAll`: passed for 39 production and 12 test Scala sources.
- Forced `sbt "Test / testOnly *"`: passed; all 40 tests passed across 11 suites. The 3 new Docker-independent tests exercise event-ID state, first-occurrence Kafka metadata, watermark advancement and state removal, a reported too-late drop, eligible out-of-order acceptance, configuration isolation, and the deduplicated sink's own retry-safe Delta transactions.
- The controlled live experiment used isolated paths under `/tmp/sentinel-phase7.Ro9buW`, watermark delay `10 minutes`, validated sink app ID `sentinel-phase7-validated-v1`, and deduplicated sink app ID `sentinel-phase7-deduplicated-v1`.
- Three initial events produced 3 validated and 3 deduplicated rows. Republishing the same seed/base/count created three new Kafka coordinates and raised the validated table to 6 rows, while the deduplicated table remained at 3.
- A unique event at `2030-01-01T12:30:00Z` raised the deduplicated table to 4 rows. Spark then reported watermark `2030-01-01T12:20:00.000Z`, `numRowsRemoved=3`, `numRowsTotal=1`, and `memoryUsedBytes=9600` for `dedupeWithinWatermark`.
- A unique event at `2030-01-01T12:19:59Z`, selected from the observed watermark, remained in `transactions_validated` but did not change the deduplicated row count. Spark reported `numRowsDroppedByWatermark=1`.
- A unique out-of-order event at `2030-01-01T12:21:00Z` was newer than the active watermark but older than the newest event; it was accepted and raised the deduplicated count to 5 with `numRowsUpdated=1` and no watermark drop.
- Final live inspection reported 9 rows in `transactions_validated` and 5 rows in `transactions_deduplicated`, with the same 18-column event/transport schema and no physical partitioning. The deduplicated table had three successful write versions containing 3, 1, and 1 rows.
- `make verify-scala`, `make verify-python`, and `make verify`: passed with Kafka and Schema Registry stopped. The explicit forced Scala run provides the 40-test evidence because subsequent sbt incremental test invocations correctly had no changed tests to rerun.
- `git diff --check`: passed before the final project-state update and was rerun afterward.

### Phase 8 verification

- Inspection of the installed Spark 4.2.0 sources and live execution established that streaming `transformWithState` supports only `RocksDBStateStoreProvider`. The default HDFS-backed provider failed with `STORE_BACKEND_NOT_SUPPORTED_FOR_TWS`; the user approved a narrow Phase 8 exception, and only the customer-state application/test session configures RocksDB.
- `sbt "clean ; compile"`: passed under Temurin JDK 21.0.12.1, Scala 2.13.18, and sbt 2.0.9. No direct dependency was added.
- Forced `sbt "Test / testOnly *"`: passed; all 49 tests passed across 13 suites. Six `TwsTester` tests cover initial/repeated state, independent keys, out-of-order event time, stale timer protection, expiration/reset, exact decimal handling, and overflow. Three real local Spark/Delta tests cover configuration, checkpoint recovery, event-time expiry, reactivation, output schema/layout, and snapshot-sink retry suppression.
- The controlled live experiment used isolated paths under `/tmp/sentinel-phase8.RmwYH3`, watermark delay `10 minutes`, inactivity duration `1 hour`, and dedicated validated, deduplicated, and customer-state checkpoint/transaction lineages.
- Deterministic seeds 834 and 7350 produced two events for `customer-9428` at 12:00 and 12:01. The first state run emitted count 2, total 11361.69, latest time 12:01, and expiry 13:01. Seed 1 produced independent `customer-1363` state at count 1, latest 12:05, and expiry 13:05.
- Seed 10734 produced another `customer-9428` event at 12:20. Restarting with the same state checkpoint restored the prior state and emitted count 3, total 14532.05, latest time 12:20, and replacement expiry 13:20. Progress reported one deleted timer and one registered replacement.
- A unique `customer-8430` event at 13:40 advanced the observed state-query watermark from 12:10 to 13:30. At 13:30 both older timers fired: `numRowsRemoved=2`, custom `numExpiredTimers=2`, custom `numDeletedTimers=2`, `numRowsTotal=1`, and `memoryUsedBytes=442117`. `EXPIRED` snapshots retained A's count 3/expiry 13:20 and B's count 1/expiry 13:05.
- Seed 16404 produced a new `customer-9428` event at 13:45. The same checkpoint emitted a fresh lifecycle with count 1, total 7273.11, and expiry 14:45. The final snapshot table contained 7 rows; final no-data progress reported two active customer states and 439120 state-memory bytes.
- Every live event passed through producer → registry-backed Kafka → validated Delta → event-ID deduplicated Delta → customer state. Existing Phase 7 evidence was not deleted.
- The live state operator was `transformWithStateExec`; progress reported 200 shuffle partitions/state-store instances from the existing Spark defaults. These are correctness diagnostics, not tuning or benchmark results.
- `sbt scalafmtCheckAll`: passed after the final Scala source update.
- `make verify-scala`: passed under Temurin JDK 21.0.12.1 with infrastructure stopped. The earlier forced test run supplies the 49-test evidence; the final incremental invocation had no changed tests to rerun.
- `make verify-python`: passed under Python 3.13.9; pytest, Ruff lint/format, and mypy passed.
- `make verify`: passed for both modules with Kafka and Schema Registry stopped.
- `git diff --check`: passed after the final Phase 8 implementation and documentation updates.

### Phase 9 verification

Historical v1 evidence below is preserved. Its cross-currency monetary sums are superseded by ADR-020; time, recovery, and retry evidence does not establish dimensional monetary validity.

- The pre-change worktree was clean. The resolved runtime remained Temurin JDK 21.0.12.1, Scala 2.13.18, sbt 2.0.9, Spark 4.2.0, and Delta Lake 4.4.0. No direct dependency was added.
- Spark 4.2's resolved `MapState`, timer, `StatefulProcessorHandle`, and `TwsTester` APIs were inspected before implementation. The Phase 9 application/test session selects RocksDB only because streaming `transformWithState` requires it in the pinned runtime.
- `sbt "clean ; compile"`: passed; 55 production Scala sources compiled.
- Forced `sbt "Test / testOnly *"`: passed; all 59 tests passed across 15 suites. The 10 Phase 9 tests cover prior-only windows, inclusive lower bounds, same-time peer exclusion, iterator order, out-of-order and online behavior, microsecond precision, timer replacement and cleanup, exact decimal/overflow handling, checkpoint recovery, customer isolation, explicit Delta schema, and sink retry suppression.
- `sbt scalafmtCheckAll`, `make verify-scala`, `make verify-python`, and `make verify`: passed with Docker infrastructure stopped. The forced Scala run supplies the 59-test evidence; subsequent sbt incremental test invocations had no changed tests to rerun. Python 3.13.9 pytest, Ruff, and mypy passed.
- The controlled live path used isolated `/tmp/sentinel-phase9.XDqpV5KBBx` Delta tables, checkpoints, and transaction application IDs. All six events passed producer → registry-backed Kafka → validated Delta → deduplicated Delta before feature processing. Existing Phase 8 paths were untouched.
- In the online lineage, deterministic seeds 834, 7350, 10734, and 16404 produced `customer-9428` events at 12:00, 12:01, 12:08, and 12:09. Their observed `(amount, prior count, prior sum)` values were respectively `(7171.8400, 0, 0.0000)`, `(4189.8500, 1, 7171.8400)`, `(3170.3600, 0, 11361.6900)`, and `(7273.1100, 1, 14532.0500)`. The query was stopped after the first two records and restarted with the same checkpoint for the later two, proving restored rolling history.
- Seed 28899 then arrived after 12:09 with event time 12:06, amount 4214.7500, count 1, and sum 11361.6900. It excluded the stored 12:08/12:09 future events. Previously written 12:08/12:09 feature rows remained unchanged; the online target contained five rows at that point.
- A separate `customer-1363` seed 1 event at 12:30 had amount 7059.8500 and zero prior features; the online target then contained six rows. This event advanced the feature-query watermark to 12:20. An initial experimental cleanup callback removed only the earliest expired entry, exposing a watermark-jump defect. The callback was corrected and verified with a new feature checkpoint, target, and transaction application ID over the same already validated/deduplicated live source. In that fresh lineage, `transformWithStateExec` reported `numRowsTotal=8` before advancement and `numRowsTotal=2`, `numRowsRemoved=6`, `numExpiredTimers=1`, and `memoryUsedBytes=435149` after the 12:20 watermark. The 8 and 2 state rows include both MapState entries and timer ValueState entries. Memory did not decrease monotonically and is not a performance claim.
- `bash -n scripts/reset-local-rolling-features.sh`: passed. Normal `make infra-down` stopped Kafka and Schema Registry without removing their data volume.
- `git diff --check`: passed after implementation and documentation updates.

### Phase 10 verification

Historical v1 evidence below is preserved. Its customer-wide mixed-currency statistical baseline is superseded by ADR-020 and must not be used for decisioning.

- The pre-change worktree was clean. The effective runtime remained Temurin JDK 21.0.12.1, Scala 2.13.18, sbt 2.0.9, Spark 4.2.0, and Delta Lake 4.4.0. Resolved Spark 4.2 `ValueState`, timer, handle, and `TwsTester` APIs were inspected. `sbt evicted` passed and retained the previously documented Netty/SLF4J transitive-version warnings; no direct dependency was added.
- Final `sbt "clean ; compile"`: passed; 63 production Scala sources compiled. Forced `sbt "Test / testOnly *"`: passed after the final finite-constructor guard with 70 tests across 17 suites. The 11 new tests exercise Welford prior-state output, sample variance, zero variance, transport ordering, late observed semantics, customer isolation, partition/offset violation, expiry/reset, stale timers, numerical checks, temporary Delta checkpoint restoration, exact/preserved source columns, and Delta retry suppression.
- Local Spark/Delta restart with the same Phase 10 checkpoint restored customer A's 100/200 baseline. The following 300 row had prior count 2, prior mean 150, sample standard deviation `70.710678...`, and z-score `2.121320...`; customer B remained independent. A repeated output batch with the same `txnAppId` and `txnVersion=0` left row count and Delta history unchanged.
- Live evidence used isolated `/tmp/sentinel-phase10.aAmQLW` tables, checkpoints, and sink application IDs. Four deterministic `customer-9428` events were acknowledged on Kafka partition 0 at offsets 13–16 and passed producer → Schema Registry/Kafka → validated Delta → deduplicated Delta → Phase 9 rolling features → Phase 10 statistical features. The Phase 10 table initially had four rows. Prior count/status progressed `0/NO_HISTORY`, `1/INSUFFICIENT_VARIANCE_HISTORY`, `2/READY`, `3/READY` for amounts `7171.8400`, `4189.8500`, `3170.3600`, `7273.1100` at 12:00, 12:01, 12:08, and 12:09.
- The fifth event, amount `4214.7500`, was later published at Kafka partition 0/offset 17 with earlier event time 12:06 and processed through every existing upstream stage using the same checkpoints. Its preserved Phase 9 event-time fields were prior five-minute count `1` and prior ten-minute sum `11361.6900`, excluding previously observed 12:08/12:09 events. Its Phase 10 prior-observed fields were count `4`, mean `5451.29`, sample standard deviation `2087.5179003943094`, z-score `-0.5923494115985454`, status `READY`. Independent arithmetic from the four actual earlier amounts agreed. The statistical target contained five rows; earlier rows were not rewritten.
- The live Phase 10 query reported `transformWithStateExec`, `numRowsTotal=1`, `numRowsUpdated=1`, `numRowsRemoved=0`, `memoryUsedBytes=431786`, `numStateStoreInstances=200`, and available timer metrics `numRegisteredTimers=0`, `numDeletedTimers=0`, `numExpiredTimers=0` for the late-event batch. The initial batch registered one inactivity timer. These are state diagnostics, not benchmarks. Processor tests verified expiry/reset and stale timer behavior; the five-event live sequence did not advance the watermark to the 24-hour expiry.
- An initial live invocation exposed that passing a spaced duration in the new Makefile target split the argument. Only that Makefile command was corrected to pass existing environment-based configuration; no state or table had started before the correction. The rerun passed.
- `make infra-down` stopped Kafka and Schema Registry after live verification without deleting the broker volume.
- With infrastructure stopped, `sbt scalafmtCheckAll`, `make verify-scala`, `make verify-python`, and `make verify` passed. The forced Scala run supplies the 70-test evidence; subsequent sbt incremental `test` invocations correctly had no changed tests to rerun. Python 3.13.9 pytest, Ruff, and mypy passed. `bash -n scripts/reset-local-statistical-features.sh` and `git diff --check` passed.

### Phase 10.1 currency-safe compatibility correction verification

- Pre-change worktree: clean `main`, with Phase 10 committed and 70 tests previously verified across 17 suites. The configured runtime/dependency baseline is unchanged; zero new direct dependencies. Explicit project Java selection reported Temurin JDK 21.0.12.1. Python checks used 3.13.9.
- `sbt "scalafmtAll ; clean ; compile ; Test / testOnly *RollingFeature* *StatisticalFeature* *CustomerAmountStatistics*"`: passed; 30 tests across the four affected suites. No earlier assertions were removed or weakened; existing statistical processor tests now supply the explicit customer-currency key.
- Mixed rolling example USD 100 at 12:00, EUR 200 at 12:01, USD 300 at 12:02: the last row has customer-wide count 2 and USD-only sum 100. A subsequent EUR event sees only EUR monetary history. Exact lower-bound USD 100 at 12:00 is included for 12:10 USD while same-time EUR 500 is excluded. Out-of-order 12:08 USD excludes stored 12:05 EUR and future 12:10 USD; equal-time peers remain excluded.
- Alternating statistical observations USD 100, EUR 1000, USD 200, EUR 1200, USD 300: independent final states are USD count 3 / mean 200 / M2 20000 and EUR count 2 / mean 1100 / M2 20000. The first EUR has `NO_HISTORY`; second USD prior count/mean are 1/100. Third USD prior count/mean/stddev/z-score are 2/150/70.710678.../2.121320..., scored before update. Partition/offset checks remain active per pair; offset gaps across currencies are valid.
- Temporary Delta/Structured Streaming recovery passed: first run USD 100, EUR 1000, USD 200; restart the same newly created v2 checkpoint for USD 300 and EUR 1200. USD restored prior count 2 / mean 150, EUR prior count 1 / mean 1000, and two independent ValueStates were reported. A separate rolling v2 recovery test restored both currencies while retaining customer-wide counts. All explicit 20-/25-column schemas and sink retry assertions remain passing.
- Processor expiry evidence: USD latest time 12:00 expires at 13:00 while EUR latest time 12:30 expires at 13:30 (one-hour test inactivity). At the USD timer only USD state clears. USD reactivation at 13:01 has `NO_HISTORY`; active EUR state remains intact and its next event uses prior EUR mean 1000.
- Live verification used fresh isolated `/tmp/sentinel-phase10-1.qNfeRD` paths: `transactions_validated`, `transactions_deduplicated`, `transaction_customer_features_v2`, and `transaction_statistical_features_v2`. Checkpoints were `validated-checkpoint`, `dedup-checkpoint`, `rolling-v2-checkpoint`, and `statistical-v2-checkpoint`; dedicated sink IDs were `sentinel-phase10-1-validated-v1`, `sentinel-phase10-1-dedup-v1`, `sentinel-phase10-1-rolling-v2`, and `sentinel-phase10-1-statistical-v2`. No old table or checkpoint was reused, edited, deleted, or mixed with v2 rows.
- Docker Desktop was initially stopped, then started for this authorized experiment. Existing `make infra-up` passed; Kafka retained three partitions/RF 1 and Schema Registry retained version 1 with BACKWARD_TRANSITIVE. Infrastructure configuration was unchanged. A fresh validated checkpoint was established with `STARTING_OFFSETS=latest` before publishing.
- All five live events traversed the existing producer → registry-backed Kafka → validated Delta → deduplicated Delta → corrected rolling features → corrected statistics path. Existing deterministic seeds already supported the fixture, so no generator/producer change was needed. The customer was `customer-9428`, Kafka partition 0, offsets 18–22, with event times 2030-01-01 12:00–12:04 UTC. Each producer invocation requested and acknowledged exactly one record.
- Seed 834 / 12:00 / USD 7171.8400: rolling count 0, sum 0.0000; statistical prior count 0, `NO_HISTORY`.
- Seed 7350 / 12:01 / EUR 4189.8500: rolling count 1, sum 0.0000; statistical prior count 0, `NO_HISTORY`.
- Seed 16404 / 12:02 / USD 7273.1100: rolling count 2, sum 7171.8400; statistical prior count 1, mean 7171.84, null stddev/z-score, `INSUFFICIENT_VARIANCE_HISTORY`.
- After these three rows, the queries restarted with their same fresh checkpoints, including the new rolling/statistical v2 lineages, for the next two events. Seed 28899 / 12:03 / EUR 4214.7500: rolling count 3, sum 4189.8500; statistical prior count 1, mean 4189.85, null stddev/z-score, `INSUFFICIENT_VARIANCE_HISTORY`.
- Seed 42049 / 12:04 / USD 7298.0100: rolling count 4, sum 14444.9500; statistical prior count 2, mean 7222.475, sample stddev 71.60870373076101, z-score 1.054829874926953, `READY`. Independent arithmetic using only the two preceding USD amounts agreed within floating-point precision. EUR amounts did not affect USD outputs.
- Inspections observed five rows in each of the four isolated Delta tables. The first three statistical rows remained unchanged on restart. The second live statistical run reported `transformWithStateExec`, `numRowsTotal=2`, `numRowsUpdated=2`, `numRowsRemoved=0`, `memoryUsedBytes=436840`, two replaced/registered timers, and watermark 11:52 before advancing to 11:54. These are correctness diagnostics, not benchmarks; independent expiry is demonstrated by processor tests, not this five-minute live sequence.
- `make infra-down` passed without deleting the broker volume; `docker compose ps -a` then listed no project services. With infrastructure stopped, `sbt "clean ; compile"`, forced `sbt "Test / testOnly *"`, `sbt scalafmtCheckAll`, `make verify-scala`, `make verify-python`, and `make verify` all passed. The forced run executed **79 tests across 17 suites** (all 70 previous tests plus 9 additional tests). Subsequent incremental `test` runs correctly had no changed tests to rerun. Python pytest (1 test), Ruff lint/format, and mypy passed under 3.13.9.
- `bash -n scripts/reset-local-rolling-features.sh scripts/reset-local-statistical-features.sh` and `git diff --check` passed. Reset scripts now target only v2 defaults; no destructive reset was needed or executed.
- ADR-020 is accepted. ADR-016/018 carry narrow monetary-semantic refinement notes, and ADR-017/019 cross-reference the revised state dimensions. Original decisions and Phase 9/10 verification evidence remain historical. Transaction Avro schema/version, Kafka key/topology, earlier queries, and Python implementation remain unchanged.

### Phase 11 deterministic explainable risk verification

- Pre-change worktree was clean `main`, tracking `origin/main`, with Phase 10.1 committed (`4304a39`) and 79 tests across 17 suites. The 25-column statistical schema, v2 monetary semantics, earlier checkpoints, infrastructure and dependency baseline were preserved. Zero direct dependencies were added. The session explicitly selected Temurin JDK 21.0.12.1; Spark ran 4.2.0 with existing Delta 4.4.0. Python verification used 3.13.9.
- `sbt "scalafmtAll ; compile ; Test / testOnly *Risk*"` passed 19 new tests across two suites. Pure tests cover CLEAR, each inclusive rule threshold, isolated/overlapping R003, all-rules canonical ordering, readiness gating, negative z-score, deterministic evaluation, fixed fingerprint identity, sensitivity to all four thresholds and human version, invalid policies, invalid feature contexts and required CLI configuration. Invalid feature tests intentionally provoke explicit Spark task failures and assert them; they are not unresolved application failures.
- Temporary Delta tests demonstrate one decision per source row, native string arrays, policy identity, exact preservation of all 25 source fields (including a large `decimal(38,4)` sum), CLEAR/REVIEW, no physical partition columns, zero state operators, same-checkpoint no-input resume, subsequent new input, and independent sink retry suppression. Writing an identical risk batch twice with the same application ID/version leaves one row and one Delta commit. No RocksDB is configured for this query.
- Docker Desktop was initially unavailable, then opened manually by the user. Existing `make infra-up` passed with Docker Engine 29.7.2 / Docker Desktop 4.89.0. Kafka retained three partitions/RF 1 and the registry subject retained version 1 / BACKWARD_TRANSITIVE. No infrastructure configuration changed.
- Live verification used fresh isolated `/tmp/sentinel-phase11.79hNIz` tables and checkpoints. Dedicated transaction IDs: `sentinel-phase11-validated-v1`, `sentinel-phase11-deduplicated-v1`, `sentinel-phase11-rolling-v2`, `sentinel-phase11-statistics-v2`, `sentinel-phase11-risk-decisions-v1`. A new validated checkpoint was initialized with `STARTING_OFFSETS=latest` before publishing. Old Phase 6–10.1 data and state were not edited, deleted or reused.
- All five records traversed the existing deterministic producer → registry-backed Kafka → validated Delta → deduplicated Delta → corrected rolling v2 → statistical v2 → risk engine. Each producer requested/acknowledged one event with zero failures. The customer was `customer-9428`, Kafka partition 0, offsets 23–27, event times 2030-01-01 12:00–12:04 UTC.
- Explicit **non-production engineering verification** policy: `phase11-verification-v1`, high velocity 3, high positive z-score 2.0, combined velocity 3, combined positive z-score 1.0. Persisted fingerprint: `594b7caf5f937e6098c4fb0ca3ea692759600c40461a26954a3e1012e2b9635e`. The effective canonical representation was printed on both runs and is specified in ADR-022; version and all thresholds participate. This policy is not fraud-calibrated and the fingerprint is not a security signature.
- Seed 834 / 12:00 / USD 7171.8400: velocity 0, z-score null / `NO_HISTORY`, matched IDs/reasons empty, `CLEAR`.
- Seed 7350 / 12:01 / EUR 4189.8500: velocity 1, z-score null / `NO_HISTORY`, empty matches, `CLEAR`.
- Seed 16404 / 12:02 / USD 7273.1100: velocity 2, z-score null / `INSUFFICIENT_VARIANCE_HISTORY`, empty matches, `CLEAR`.
- Seed 28899 / 12:03 / EUR 4214.7500: velocity 3, z-score null / `INSUFFICIENT_VARIANCE_HISTORY`, `R001_HIGH_TRANSACTION_VELOCITY_5M` / `HIGH_TRANSACTION_VELOCITY_5M`, `REVIEW`.
- Seed 42049 / 12:04 / USD 7298.0100: velocity 4, z-score `1.054829874926953` / `READY`, ordered R001 then `R003_VELOCITY_AND_AMOUNT_ANOMALY`, aligned velocity/combined reason codes, `REVIEW`. USD prior count/mean/stddev were 2 / 7222.475 / 71.60870373076101; EUR observations did not influence this baseline. R002's 2.0 threshold was not reached live; its individual match is covered by pure tests. Rolling USD sum 14444.9500 remains evidence only, never thresholded.
- First risk query reported batch 0 / five inputs / zero state operators. Inspection observed five 31-column rows: three CLEAR, two REVIEW. Same-policy/checkpoint rerun reported batch 1 / zero inputs / zero state operators; second inspection still observed five rows and the same fingerprint. No decision-time timestamp or randomness is added.
- `make infra-down` preserved broker data; `docker compose ps -a` listed no project containers. With infrastructure stopped, `sbt "clean ; compile"`, forced `sbt "Test / testOnly *"`, `sbt scalafmtCheckAll`, `make verify-scala`, `make verify-python`, `make verify` and `git diff --check` passed. The forced run executed **98 tests across 19 suites**, including all previous 79 tests unchanged and 19 new tests. Subsequent incremental `test` tasks correctly ran zero unchanged tests. Python pytest (1 test), Ruff lint/format and mypy passed. Final documentation changes were followed by another whitespace check.
- Decisions are append-only advisory evaluations. Same-app/batch sink retries and source checkpoint progress do not guarantee lifetime event+policy uniqueness; fresh-checkpoint replay can append another evaluation. No MERGE, historical re-evaluation, ML, numeric risk score, enforcement side effect, FX conversion, new stateful processor, Phase 11 RocksDB or benchmark is introduced. Prior Phase 9/10 histories above are retained unchanged.

#### Executed live commands

The existing infrastructure was started, then an isolated directory was allocated (returned `/tmp/sentinel-phase11.79hNIz`). Session-only runtime/path/policy exports applied to the subsequent pipeline commands; no global runtime was changed:

```sh
make infra-up
mktemp -d /tmp/sentinel-phase11.XXXXXX
export JAVA_HOME=/Users/pranavkapale/.sdkman/candidates/java/21.0.12+1.1-tem
export PATH="$JAVA_HOME/bin:/Users/pranavkapale/.sdkman/candidates/sbt/current/bin:$PATH"
export DELTA_PATH=/tmp/sentinel-phase11.79hNIz/transactions_validated DELTA_CHECKPOINT_DIR=/tmp/sentinel-phase11.79hNIz/validated-checkpoint DELTA_TXN_APP_ID=sentinel-phase11-validated-v1
export DEDUPLICATED_DELTA_PATH=/tmp/sentinel-phase11.79hNIz/transactions_deduplicated DEDUP_CHECKPOINT_DIR=/tmp/sentinel-phase11.79hNIz/dedup-checkpoint DEDUP_DELTA_TXN_APP_ID=sentinel-phase11-deduplicated-v1
export FEATURE_DELTA_PATH=/tmp/sentinel-phase11.79hNIz/transaction_customer_features_v2 FEATURE_CHECKPOINT_DIR=/tmp/sentinel-phase11.79hNIz/rolling-checkpoint-v2 FEATURE_DELTA_TXN_APP_ID=sentinel-phase11-rolling-v2
export STATISTICAL_FEATURE_DELTA_PATH=/tmp/sentinel-phase11.79hNIz/transaction_statistical_features_v2 STATISTICAL_FEATURE_CHECKPOINT_DIR=/tmp/sentinel-phase11.79hNIz/statistical-checkpoint-v2 STATISTICAL_FEATURE_DELTA_TXN_APP_ID=sentinel-phase11-statistics-v2
export RISK_DELTA_PATH=/tmp/sentinel-phase11.79hNIz/transaction_risk_decisions RISK_CHECKPOINT_DIR=/tmp/sentinel-phase11.79hNIz/risk-checkpoint-v1 RISK_DELTA_TXN_APP_ID=sentinel-phase11-risk-decisions-v1
export POLICY_VERSION=phase11-verification-v1 HIGH_VELOCITY_THRESHOLD_5M=3 HIGH_AMOUNT_ZSCORE_THRESHOLD=2.0 COMBINED_VELOCITY_THRESHOLD_5M=3 COMBINED_AMOUNT_ZSCORE_THRESHOLD=1.0
make ingest-delta STARTING_OFFSETS=latest
make produce-sample COUNT=1 SEED=834 BASE_TIME=2030-01-01T12:00:00Z
make produce-sample COUNT=1 SEED=7350 BASE_TIME=2030-01-01T12:01:00Z
make produce-sample COUNT=1 SEED=16404 BASE_TIME=2030-01-01T12:02:00Z
make produce-sample COUNT=1 SEED=28899 BASE_TIME=2030-01-01T12:03:00Z
make produce-sample COUNT=1 SEED=42049 BASE_TIME=2030-01-01T12:04:00Z
make ingest-delta STARTING_OFFSETS=latest
make deduplicate-transactions
make process-rolling-features
make process-statistical-features
make process-risk-decisions
make inspect-risk-decisions
make process-risk-decisions
make inspect-risk-decisions
make infra-down
docker compose ps -a
```

The risk default checkpoint/application ID is distinct from every earlier query. Future policy changes require explicit policy-version/fingerprint and checkpoint/output/application-ID review (ADR-022).

## Known Technical Debt

- Phase 8 `customer_activity_snapshots` retains historical lifecycle diagnostic numeric totals without currency context. It is outside the Phase 9/10 feature pipeline and preserved here; its mixed-currency totals must not be interpreted as a monetary balance or fed into decisioning. A separate correction is needed if that diagnostic is ever used monetarily.
- V1 Phase 9/10 monetary outputs are dimensionally invalid for mixed-currency inputs and superseded, not repaired retrospectively. Consumers must select fresh v2 feature paths; no in-place migration or retroactive recomputation is implemented.
- The temporary Python foundation smoke test should be removed once substantive model-control-plane tests provide equivalent build-wiring coverage.
- Spark's transitive graph reports minor Netty 4.2.13-over-4.2.9 and SLF4J 2.0.18-over-2.0.17/1.7.36 eviction warnings. The local Spark test, prior producer tests, and live producer/consumer path pass; no speculative override was added without an observed defect.
- Spark 4.2 couples streaming `transformWithState` to RocksDB. The Phase 8, 9, and 10 provider selections are mandatory for this API in the pinned runtime and are not evidence that RocksDB is otherwise preferable or production-tuned.
- Risk policy thresholds are explicit engineering verification settings, not calibrated production policy. Same-policy replay under a fresh checkpoint has no lifetime decision uniqueness guard; replay/lineage governance remains required.

## Known Failures

No current build, test or runtime verification failures are known for Phase 11. The temporary Docker prerequisite blocker was resolved before live verification. Superseded v1 monetary semantics, the separate Phase 8 diagnostic limitation and the corrected historical Phase 10 Makefile failure remain documented above.

## Deferred Decisions

- Durable Kafka metadata model beyond the current validated streaming record.
- Dead-letter queue contract, topic, routing, and invalid-event reprocessing behavior.
- Kafka transactions and any future atomic multi-record/multi-topic boundary.
- Production Kafka topology, replication, retention sizing, and security.
- Validated-event and decision topics.
- Conflicting-payload detection when one `event_id` is reused with different content.
- Late-event side-output retention, routing, and reprocessing policy.
- Production-derived watermark delay and any associated lateness service-level objective.
- MinIO/S3-compatible object storage and production Delta deployment topology.
- Multi-sink atomicity and recovery semantics beyond the current single Delta table.
- Production state-store sizing, RocksDB tuning, and future Spark-version provider reevaluation.
- Customer state-schema migration and initial-state bootstrapping.
- Offline/backfill feature recomputation and retroactive correction of online feature rows after late arrivals.
- Further rolling windows, unique merchant/device state, Welford rolling windows, and retrospective statistical recomputation.
- Production rule/threshold calibration, numeric risk scoring and fraud labels; current rules demonstrate engineering semantics only.
- Historical policy re-evaluation and decision MERGE/idempotency across fresh checkpoints.
- Payment blocking/enforcement, review workflow and policy promotion/governance services.
- ML training, MLflow, model serving and shadow/canary evaluation.
- Customer Kafka partition-remapping migration and Phase 10 checkpoint/state compatibility strategy.
- ML runtime contract.
- Observability and reproducible performance benchmarking.

## Next Planned Capability

Phase 11 is fully implemented and verified locally and through the isolated mixed-currency full path. Work stops at deterministic advisory decisions; no subsequent phase, ML, policy replay or enforcement has been started.
