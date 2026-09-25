# Minimal streaming ingestion

## Purpose

Phase 5 establishes the first Module A consumer boundary. It proves that Spark Structured Streaming can read the existing local topic, decode the governed registry-backed Avro values, preserve Kafka coordinates, and reapply the trusted transaction-domain boundary.

```text
Kafka transactions.raw
        ↓
Spark Kafka source
        ↓
transport metadata + key/value bytes
        ↓
Schema Registry-aware Avro decode
        ↓
candidate mapping + domain validation
        ↓
customer key consistency validation
        ↓
validated streaming record
        ↓
diagnostic foreachBatch sink
```

## Implemented

The runtime uses Spark SQL and Structured Streaming 4.2.0 with the matching Kafka connector. Local execution defaults to `local[*]`; callers can select another local master for tests. The Spark SQL session timezone is UTC so diagnostic timestamp rendering remains aligned with the event contract's absolute-instant semantics.

The Kafka source subscribes only to `transactions.raw`. It receives `key` and `value` as bytes and preserves `topic`, `partition`, `offset`, and Kafka `timestamp` immediately. These coordinates remain transport metadata on `ValidatedTransactionRecord`; they are not added to `TransactionEvent` and are not treated as event identity.

Each Spark task partition owns one official Confluent `KafkaAvroDeserializer` configured with the supplied Schema Registry URL. The deserializer resolves the wire schema ID; production code neither parses Confluent framing nor hard-codes a schema ID. Decoded generic records pass through the existing Avro mapper and `TransactionEventValidator`. The UTF-8 record key must exactly match the resulting `customerId`. Expected malformed data produces a typed ingestion error which fails the current query visibly.

The original local entrypoint uses `Trigger.AvailableNow` and a diagnostic `foreachBatch` sink. The sink remains useful for observing selected domain values and Kafka partition/offset ranges. Phase 6 adds a separate primary durable entrypoint that reuses this ingestion boundary and writes validated records to Delta; see [durable Delta ingestion](delta-ingestion.md).

## Source and checkpoint semantics

The checkpoint location is explicit. Its current purpose is to retain Structured Streaming query and Kafka source progress. `startingOffsets=earliest` is the default for a new checkpoint only; once checkpoint progress exists, Spark resumes from that progress rather than applying `startingOffsets` again.

Live verification with a fresh checkpoint demonstrated:

- 10 published records produced one bounded run of 10 records;
- an immediate run with the same checkpoint and no new publication processed 0 records;
- after 5 more records were published, the same checkpoint processed only those 5 records.

This demonstrates basic checkpoint-managed source progress. Phase 6 separately proves idempotent retry for one Delta micro-batch transaction after a post-commit failure. Neither result demonstrates business-event deduplication or unconditional end-to-end exactly-once processing.

## Failure policy

Null or malformed keys, registry/Avro decode failures, domain validation failures, and Kafka-key/customer mismatches fail the query. Records are neither silently dropped nor routed elsewhere. A future DLQ design must define the original-byte, failure-reason, Kafka-coordinate, schema-identifier, and reprocessing contracts before a DLQ topic is introduced.

## Deferred

- DLQ contract and routing;
- event-time lateness and watermark policy;
- business-event deduplication;
- MinIO/object-storage deployment;
- stateful customer features and RocksDB state-store selection;
- fraud rules or scoring;
- production observability;
- measured performance tuning and benchmarks.
