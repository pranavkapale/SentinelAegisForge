# Customer stateful processing

## Purpose and boundary

Phase 8 establishes the first deliberately managed customer-keyed state lifecycle:

```text
transactions_deduplicated
        ↓ Delta streaming source
withWatermark(event_time, configured delay)
        ↓
groupByKey(customer_id)
        ↓
transformWithState / customerActivity ValueState
        ↓ event-time inactivity timer
foreachBatch + Delta transaction
        ↓
customer_activity_snapshots
```

The source remains the Phase 7 deduplicated table. The query does not read Kafka or `transactions_validated` again and does not alter either existing Delta table.

## State grain and fields

There is one constant-sized `ValueState[CustomerActivityState]` named `customerActivity` per active `customer_id`. It contains:

- `eventCount`: accepted deduplicated events in the current lifecycle;
- `amountTotal`: exact `decimal(38,18)` aggregate, with no floating point and no rounding;
- `firstEventTime`: minimum event time observed in the lifecycle;
- `latestEventTime`: maximum event time observed in the lifecycle;
- `expiryTimerTimestamp`: the currently authoritative event-time timer in milliseconds.

Overflow beyond `decimal(38,18)` or `Long` count capacity fails explicitly. The scale of 18 matches Spark's product encoder representation while retaining the existing four-decimal transaction amounts exactly.

## Key and update semantics

`customer_id` is the state key, consistent with the Kafka partition-key decision. Input ordering within a micro-batch is not assumed. Each customer invocation calculates batch count, exact batch total, and min/max event times before combining them with restored state. One `UPDATED` snapshot is emitted per customer invocation rather than one row per transaction.

Out-of-order eligible events increase count and amount and may move `firstEventTime` earlier. They cannot regress `latestEventTime` or its inactivity timer.

## Timer and watermark semantics

The query uses `TimeMode.EventTime()` and an explicit input watermark. Its local watermark delay defaults to 10 minutes for controlled development; that value is not a production lateness SLA.

The inactivity timer is `latestEventTime + configured duration`. The ordinary local default is 24 hours; tests and controlled live verification may use one hour. A newer latest event replaces the previous timer. The stored expected timestamp guards against stale timer callbacks.

Expiration occurs only after the event-time watermark advances past a registered timer. It is not wall-clock TTL. `TTLConfig.NONE` disables state-variable TTL. When the authoritative timer fires, the query emits `EXPIRED`, clears the state, and permits a later watermark-eligible event to begin a fresh lifecycle at count one.

## Spark 4.2 state-store requirement

Spark 4.2 streaming `transformWithState` currently supports only `RocksDBStateStoreProvider`; attempting to use the default HDFS-backed provider fails with `STORE_BACKEND_NOT_SUPPORTED_FOR_TWS`. The customer-state application therefore configures RocksDB narrowly for this query. This is a required execution backend, not a performance-tuning decision, and no performance claim follows from it. Existing Phase 7 and earlier queries are not reconfigured.

## Checkpoint recovery

The default checkpoint is `.local/checkpoints/customer-activity-state`. It contains source progress, watermark progress, the `customerActivity` state variable, registered timers, and sink progress. A real local Spark/Delta test stops and restarts AvailableNow queries with this checkpoint and verifies that a later customer event continues from restored state.

The state key, schema, time mode, watermark, timer rules, and stateful topology are checkpoint contracts. An intentional incompatible change requires explicit review and normally a new checkpoint plus a new Delta transaction application ID. Checkpoint files must never be edited manually. No initial-state migration is implemented.

## Snapshot output and retry semantics

`customer_activity_snapshots` has one row per emitted customer update or expiration. It is append-only lifecycle evidence, not a final online feature store. Its columns are:

```text
customer_id
lifecycle_type
batch_event_count
batch_amount_total
state_event_count
state_amount_total
state_first_event_time
state_latest_event_time
state_expiry_time
```

The table has no physical partition columns. The sink uses `foreachBatch` with dedicated default `txnAppId=sentinel-customer-activity-snapshots-v1` and `txnVersion=batchId`. These identifiers protect Delta micro-batch retries; they do not make customer-state processing unconditionally exactly once.

## Diagnostics

After AvailableNow completes, the application reports the watermark and, where Spark exposes them, operator name, state rows total/updated/removed, memory, shuffle partitions, state-store instances, and custom state metrics. These are state-lifecycle diagnostics, not benchmarks. `TwsTester` covers processor logic but does not prove automatic watermark propagation, checkpoint persistence, or distributed execution; the real local streaming test covers those boundaries.

## Current guarantees

- deduplicated events update one constant-sized state value for their customer;
- exact count and amount totals survive restart within one compatible checkpoint lineage;
- out-of-order eligible input cannot move the latest event time or timer backward;
- watermark-driven inactivity emits an expiration record and clears state;
- a subsequent eligible event begins a new customer lifecycle;
- snapshot Delta retries are independently guarded by transaction application/version IDs.

## Non-guarantees and deferred capabilities

- no rolling 1m/5m/10m features, unique sets, Welford statistics, anomaly scoring, or fraud rules;
- no processing-time TTL, initial-state migration, or general state-schema migration mechanism;
- no Redis, external online feature store, MinIO/S3, or performance benchmark;
- no unconditional exactly-once or production-scale claim.

