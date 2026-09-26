# Durable Delta ingestion

## Purpose

Phase 6 replaces the primary diagnostic-only output with a local durable table while preserving the existing decode and validation boundary.

```text
Kafka transactions.raw
        ↓
Spark Structured Streaming
        ↓
Schema Registry-aware Avro decode
        ↓
domain and customer-key validation
        ↓
validated transport record
        ↓
foreachBatch
        ↓
Delta transaction (txnAppId, batchId)
        ↓
transactions_validated Delta table
```

The application uses `Trigger.AvailableNow` for bounded developer runs. The Phase 5 diagnostic application remains available, while `make ingest-delta` is the durable ingestion path.

## Data grain

One row represents one successfully validated Kafka record. This is not a unique-business-transaction grain: two Kafka records describing the same event remain two records. Phase 7 consumes this unchanged audit boundary into a separate, watermark-bounded deduplicated table.

## Storage schema

The path-based table uses stable snake_case columns for all validated event fields and Kafka transport metadata. `amount` is `decimal(18,4)`. `event_time`, `ingestion_time`, and `kafka_timestamp` are Spark timestamps interpreted in the session's UTC timezone. `kafka_partition` and `schema_version` are integers; `kafka_offset` is a long. The UTF-8 Kafka key is retained alongside topic, partition, offset, and timestamp for replay and diagnosis.

The table has no physical partition columns. Automatic schema merging and optional Delta table features are not enabled. Schema and layout evolution must be deliberate and tested.

## Checkpoint semantics

The durable query has its own checkpoint, defaulting to `.local/checkpoints/delta-transaction-ingestion`; it does not reuse the Phase 5 diagnostic checkpoint. Spark stores Kafka source progress and micro-batch IDs there. `startingOffsets=earliest` affects a fresh checkpoint only.

Checkpoint lineage and Delta transaction application ID are coupled. A new checkpoint lineage must use a new `txnAppId`; otherwise its reset batch IDs can collide with transaction versions already recorded for the old lineage and valid writes may be ignored.

## Delta transaction semantics

For every non-empty batch, the sink writes with the configured stable `txnAppId` and `txnVersion=batchId`. `foreachBatch` callback execution is at least once. Delta's transaction pair—not `foreachBatch` alone—makes a retry of the same logical micro-batch a no-op at this one table.

The deterministic default application ID is `sentinel-transactions-validated-v1`. It may be overridden only as an explicit lineage decision; it is not generated from time or randomness.

## Failure boundary

The diagnostic failpoint runs after the Delta write returns successfully and is disabled by default. It models process failure after the table transaction has committed but before Spark can mark the batch complete. Restarting with the same checkpoint and application ID retries the batch ID; Delta suppresses the duplicate table transaction.

This boundary protects one Delta table from duplicate appends caused by retrying the same Spark micro-batch. It does not make Kafka input unique and does not coordinate another sink.

## Replay boundary

Replaying Kafka through a genuinely new checkpoint is a new logical writer and therefore requires a new `txnAppId`. Such a replay can append the same business events again. Phase 7 applies a separately defined event-ID deduplication policy downstream; it does not change this table or make this ingestion writer unique.

## Local lifecycle and inspection

- `make ingest-delta` verifies local infrastructure and runs bounded durable ingestion.
- `make inspect-delta` reads the configured path and prints row count, schema, and Delta history.
- `make reset-delta` deliberately removes only the default project-local table and its coupled checkpoint.

The default table path is `.local/delta/transactions_validated`. Normal Kafka infrastructure shutdown does not delete Delta data. Custom test paths must be cleaned explicitly by the caller.

## Current guarantees

- validated event values and Kafka coordinates have an explicit durable schema;
- a successfully committed Delta transaction survives query/process restart on the local filesystem;
- retrying the same `(txnAppId, batchId)` does not append that batch again;
- checkpoint progress and the transaction identity are explicit and testable.

## Explicit non-guarantees

- no unconditional end-to-end exactly-once or duplicate-free business-event claim;
- no business-key deduplication, `MERGE`, watermark, late-event policy, or stateful feature logic inside this Phase 6 ingestion query;
- no DLQ, multi-sink atomicity, object-store durability, high availability, or disaster recovery;
- no performance, scaling, compaction, clustering, retention, or production layout claim;
- no MinIO/S3, catalog/metastore deployment, or optional Delta feature enablement.
