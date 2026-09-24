# ADR-009: Spark Structured Streaming ingestion

## Status

Accepted

## Context

`transactions.raw` now has a governed registry-backed Avro value contract and a bounded producer. Module A needs its first consumer boundary while preserving the existing domain validator, Kafka transport metadata, and explicit source-progress semantics. This phase needs bounded local execution, not business processing or a durable sink.

## Decision

Use Apache Spark 4.2.0 Structured Streaming with its Kafka DataFrame source. The local application defaults to `local[*]`, reads keys and values as bytes, and immediately retains Kafka topic, partition, offset, and broker timestamp.

Decode registry-framed Avro values with the official `KafkaAvroDeserializer` on Spark executors. Reuse one configured deserializer for each Spark task partition and close it when the task completes. Convert each decoded generic record through the existing Avro-to-candidate mapper and domain validator, then require the UTF-8 Kafka key to equal the validated `customerId`.

Use an explicit checkpoint location for Spark-managed Kafka source progress. Use `Trigger.AvailableNow` for bounded local verification and a non-durable diagnostic `foreachBatch` sink. Invalid keys, registry values, domain values, or key/customer mismatches fail the query visibly until a dead-letter contract exists.

## Alternatives considered

- **Manual `KafkaConsumer`:** provides direct control over polling and commits, but would duplicate source-progress and partition-assignment responsibilities already handled by Structured Streaming and would not establish the intended Spark processing foundation.
- **Legacy Spark Streaming/DStreams:** can consume Kafka data, but uses the older micro-batch abstraction instead of the selected Structured Streaming DataFrame/Dataset APIs.
- **Keep raw Kafka bytes throughout the stream:** preserves transport data with minimal decoding work, but would postpone schema enforcement, domain validation, and key/customer consistency beyond the ingestion correctness boundary.

## Trade-offs

Structured Streaming gives Module A one declarative source and checkpoint model and preserves useful Kafka coordinates for later recovery and debugging. It also adds a substantial Spark runtime dependency and increases local JVM memory and resource requirements.

Executor-side registry decoding avoids collecting payloads at the driver and keeps schema resolution close to record processing. It requires Schema Registry to be reachable from executors and requires disciplined lifecycle management for the stateful deserializer.

## Consequences

- Spark SQL and its Kafka connector become Module A runtime dependencies.
- The Kafka source exposes transport bytes plus topic, partition, offset, and broker timestamp; transport metadata stays outside `TransactionEvent`.
- Schema Registry is required for actual registry-framed value decoding.
- Spark checkpoints manage query/source progress; application code does not commit Kafka offsets manually.
- The current diagnostic sink is not durable or idempotent, so checkpoint progress is not an end-to-end exactly-once business-processing guarantee.
- Malformed or inconsistent records currently fail the query. DLQ semantics, durable Delta materialization, crash/restart recovery tests, watermarks, state, and fraud processing remain deferred.
