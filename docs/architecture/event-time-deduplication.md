# Event-time deduplication

## Purpose and boundary

Phase 7 adds a semantic table without changing the Phase 6 audit table:

```text
transactions_validated
        ↓ Delta streaming source
withWatermark(event_time, configured delay)
        ↓
dropDuplicatesWithinWatermark(event_id)
        ↓ foreachBatch + Delta transaction
transactions_deduplicated
```

`transactions_validated` remains one row per successfully validated Kafka record and therefore retains transport-level duplicates. `transactions_deduplicated` has one accepted unique `event_id` within the configured event-time/watermark semantics. Neither table is physically partitioned in the current local design.

## Identity semantics

`event_id` identifies a business-event occurrence and is the sole deduplication key. `transaction_id` identifies the business transaction and may legitimately occur in more than one event. Kafka topic, partition, and offset identify a transport record rather than the business event.

When repeated event IDs are still represented in state, the first accepted occurrence wins. The output keeps that occurrence's `kafka_key`, topic, partition, offset, and timestamp; later metadata is not merged. A repeated `event_id` with a conflicting payload is suppressed without conflict analysis. Detecting such conflicts is deferred.

## Watermark and out-of-order semantics

The delay is explicit configuration. The default `10 minutes` is for controlled local development and verification, not a production-derived lateness SLA.

- A unique event newer than the active watermark is eligible for processing.
- A duplicate event whose ID is retained in watermark-bounded state is suppressed.
- A record older than the active watermark is dropped by Spark's stateful operator.
- An event can be older than the newest event and still be accepted when it remains newer than the watermark. Out of order does not automatically mean too late.

The query uses `event_time`, not wall-clock arrival or `ingestion_time`, for this policy.

## State retention and checkpoint lineage

`dropDuplicatesWithinWatermark` is the only stateful operator introduced here. The watermark allows Spark to remove old deduplication state. The default checkpoint is `.local/checkpoints/transaction-deduplication`, separate from Kafka-to-validated ingestion.

The checkpoint stores Delta-source progress, watermark progress, deduplication state, and sink progress. A change to the deduplication key, watermark column/delay, or stateful topology requires explicit compatibility review. Intentional changes should use a new checkpoint lineage and new Delta transaction application ID; checkpoint contents must never be manually edited.

Spark's default state-store provider remains in use. No RocksDB, custom state API, or risk-feature state is configured.

## Sink retry idempotency

The output uses `foreachBatch` and a dedicated stable application ID, defaulting to `sentinel-transactions-deduplicated-v1`, with `txnVersion=batchId`.

These are separate protections:

- Spark/Delta retry idempotency uses `(txnAppId, txnVersion)` to suppress a repeated output-batch transaction.
- Business-event deduplication uses `event_id` plus watermark-bounded Spark state to suppress repeated business events.

Neither mechanism is an unconditional end-to-end exactly-once guarantee.

## Late-drop and state diagnostics

After an AvailableNow execution, the application reports the event-time watermark and each state operator's name, total/updated/removed rows, rows dropped by watermark, and state memory, where Spark provides them. These are correctness and state diagnostics, not performance benchmarks.

No late-event side table or DLQ exists. Valid-but-late data is distinct from malformed data; a future decision may add a side output only after its retention and reprocessing contract is defined.

## Current guarantee

For a configured query lineage, duplicate event IDs that arrive within the event-time deduplication horizon are suppressed, while Delta micro-batch retries are independently protected by transactional batch identifiers.

## Non-guarantees and deferred capabilities

- no global or lifetime event uniqueness;
- no payload-conflict detection for reused event IDs;
- no recoverable route for too-late events;
- no DLQ, fraud features, custom state API, RocksDB, MinIO/S3, or performance tuning;
- no end-to-end exactly-once business-processing claim.
