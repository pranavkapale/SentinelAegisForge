# Customer statistical features

```text
transaction_customer_features (Phase 9, unchanged)
    ↓ Delta streaming source, customer_id grouping
prior Welford ValueState → calculate current features → emit → update state
    ↓ foreachBatch + dedicated Delta transaction identity
transaction_statistical_features
```

The target retains all 20 Phase 9 columns—including exact transaction amount, rolling features, and Kafka lineage—and appends `prior_amount_observation_count` (long), nullable `prior_amount_mean`, `prior_amount_stddev`, `amount_zscore` (double), and `statistical_feature_status` (string). It is append-only, unpartitioned, and has one row per Phase 9 source row accepted by this query's watermark. Phase 9's table and checkpoint are not changed.

## Definitions and numerical boundary

For one customer's current amount `x`, output is computed from previously **observed** count `n`, mean, and M2. Count zero has no mean, standard deviation, or z-score (`NO_HISTORY`). Count one has a mean but no sample variance or z-score (`INSUFFICIENT_VARIANCE_HISTORY`). For `n >= 2`, sample variance is `M2 / (n - 1)`, then standard deviation is its square root. If standard deviation is numerically greater than `1e-12`, z-score is `(x - prior mean) / prior stddev` (`READY`); otherwise z-score is null (`ZERO_VARIANCE`). The floor is a floating-point guard, not a risk threshold.

Only after output creation does Welford update: `n1 = n + 1`, `delta = x - mean`, `mean1 = mean + delta/n1`, `delta2 = x - mean1`, `M2_1 = M2 + delta*delta2`. There is no current-event contribution to its own baseline. Transaction amount stays exact `decimal(18,4)`, and the Phase 9 sum stays exact `decimal(38,4)`. Statistical values are finite-checked `Double` approximations. A variance in `[-1e-12, 0)` is clamped to zero as roundoff; a materially negative or non-finite value fails explicitly. No NaN or infinity should reach Delta.

## Observed order versus event time

For each customer, rows within a processor invocation are sorted by Kafka partition/offset, never by amount or event time. Offsets must strictly advance across invocations. The active checkpoint assumes one stable Kafka partition lineage for that customer, consistent with the current UTF-8 `customer_id` key. A partition change fails rather than comparing offsets from different partitions. Partition expansion can remap customers; it therefore requires ordering/state migration and checkpoint review.

Phase 9's rolling windows are event-time-relative: a 12:05 event excludes a previously observed 12:10 event from its `[t-window,t)` feature. Phase 10's baseline is prior **online observation**: if that 12:10 event was already observed, a late 12:05 event may use it. This is not processing-time future leakage; the current event still cannot score itself. Neither stage retroactively corrects old outputs. The Phase 10 watermark filters too-late source rows and drives event-time timers; it does not turn Welford into an event-time rolling window. The local ten-minute watermark is a development policy, not a production lateness SLA.

## State lifecycle and recovery

One constant-sized `ValueState[CustomerAmountStatistics]` per active customer stores count, mean, M2, latest event-time microseconds, authoritative expiry timer, Kafka partition, and last offset. There is no historical amount MapState. With `TimeMode.EventTime()` and state TTL disabled, the timer expires at maximum observed event time plus the configurable inactivity duration (24 hours locally). A newer maximum replaces the timer; an older event cannot shorten it; a stale callback is ignored. Timer expiry clears state without a feature row, so reactivation begins at count zero. Watermark advancement is needed for cleanup; customer count is not globally constant. Spark 4.2 requires RocksDB for this API, selected only for this query and its integration tests without tuning.

Defaults are `.local/delta/transaction_statistical_features`, `.local/checkpoints/customer-statistical-features`, and `txnAppId=sentinel-transaction-statistical-features-v1`. `foreachBatch` writes use `txnVersion=batchId`; this protects a retried output batch, not lifetime statistical uniqueness. A new checkpoint lineage needs a new transaction application ID. State schema, timer, ordering, watermark, and scoring changes require checkpoint compatibility review. AvailableNow progress reports watermark, state rows, memory, state-store instances, and available timer metrics as correctness diagnostics, not benchmarks.

No fraud threshold, risk score, decision, rolling statistical window, external state store, retroactive MERGE, or exactly-once business claim is implemented.
