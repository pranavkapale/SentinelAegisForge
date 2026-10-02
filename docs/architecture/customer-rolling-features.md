# Customer rolling features

## Current flow

```text
transactions_deduplicated
        ↓ Delta streaming source
event_time watermark (configurable)
        ↓
customer_id group
        ↓ Spark 4.2 transformWithState
prior-only rolling calculations
        ↓ foreachBatch + Delta transaction
transaction_customer_features
```

This query and its checkpoint are independent of Phase 8 `customer_activity_snapshots`. The source keeps its existing 18 transaction and Kafka lineage fields. The feature table appends those same fields plus `prior_transaction_count_5m` (`long`) and `prior_amount_sum_10m` (`decimal(38,4)`). Its grain is one feature row per event accepted by this query's event-time boundary; a too-late source row can be dropped by this query's own watermark. The table is not physically partitioned.

## Feature definitions

For an accepted event at event time `t`:

- `prior_transaction_count_5m`: count of previously observed events for its customer with event time in `[t - 5 minutes, t)`, regardless of currency.
- `prior_amount_sum_10m`: exact amount sum of previously observed events for its customer **and the current transaction’s currency** with event time in `[t - 10 minutes, t)`. The existing `currency` column defines the sum’s monetary unit.

These are feature semantic v2 definitions ([ADR-020](../adr/ADR-020-currency-safe-monetary-feature-semantics.md)). Behavioral velocity uses the customer dimension; raw monetary aggregation uses customer+currency. For USD 100 at 12:00, EUR 200 at 12:01, and USD 300 at 12:02, the last row has count 2 and USD sum 100, not 300. No FX conversion or broader currency-reference validation exists.

The lower bound is included. The current event and peers at the exact same timestamp are excluded. The processor sorts by event time and event ID for stable output, computes every equal-time peer against the same earlier state, emits their features, and only then adds those peers to state. Its answers do not depend on iterator order. A later-arriving 12:08 event may see already stored 12:10 history, but excludes it because it is future relative to 12:08.

These are online/as-observed outputs. If a 12:05 event arrives after a 12:08 row was emitted, it can affect subsequent feature calculations, but the 12:08 row is not retroactively rewritten. Offline/backfill recomputation is deferred. No feature-generated wall-clock timestamp is added.

## Precision and state

Window comparison converts `Timestamp` to exact epoch microseconds. A timestamp with finer-than-microsecond precision fails rather than being silently truncated. The original amount comes from `decimal(18,4)`; sums use checked `java.math.BigDecimal` arithmetic and fail on rounding or overflow beyond `decimal(38,4)`. The feature aggregate is carried through Spark's product encoder as exact decimal text and cast to the declared `decimal(38,4)` storage column, avoiding that encoder's default `decimal(38,18)` capacity limit.

One `rollingEvents` MapState entry per retained `event_id` stores event-time microseconds, original amount, and currency. `nextCleanupTimerMs` stores one authoritative cleanup timer for each active customer. An event expires from retained history at `event_time + 10 minutes`; watermark delay is not added to the feature horizon. Timer registration rounds microsecond expiry upward to milliseconds, so cleanup cannot occur early. When the watermark jumps across several expiries, one authoritative callback removes every entry due by the current watermark, then schedules the next earliest expiry if any. Obsolete callbacks are ignored. State-variable TTL is disabled.

This state grows with the number of events inside the retained time horizon. Watermark advancement is required to fire timers; wall-clock passage alone does not clean it. The local default watermark delay of ten minutes is a development setting, not a production-derived lateness SLA.

## Recovery and output

The default checkpoint is `.local/checkpoints/customer-rolling-features-v2`, and the target is `.local/delta/transaction_customer_features_v2`. The Delta sink uses a dedicated default `txnAppId=sentinel-transaction-customer-features-v2` and `txnVersion=batchId` to suppress repeated writes of the same micro-batch. These transaction IDs protect sink retries; they do not constitute lifetime business-event uniqueness or unconditional exactly-once processing.

The v1 monetary semantics and state schema are superseded. Never resume their checkpoint with this code or mix v1 and v2 rows in one output path. Old tables/checkpoints remain untouched; the reset target removes only the new defaults. Output columns remain structurally identical; transaction `schema_version=1` is unchanged and is distinct from feature semantic v2.

Window definitions, same-time and feature-before-update rules, state schema, cleanup timers, and watermark policy are checkpoint semantics. Changing them requires compatibility review and ordinarily a new checkpoint and transaction application ID. Spark 4.2 requires RocksDB for this `transformWithState` query. Its selection is confined to this application and tests, without memory, compaction, or checkpoint tuning.

The application reports event-time watermark, operator name, state rows total/updated/removed, state memory, state-store instances, and available timer metrics after AvailableNow runs. These are correctness diagnostics, not benchmarks. No fraud scores, merchant/device cardinalities, statistical features, external state service, or retrospective correction is implemented.
