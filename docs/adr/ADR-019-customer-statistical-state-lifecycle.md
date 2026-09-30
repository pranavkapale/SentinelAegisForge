# ADR-019: Customer statistical state lifecycle

## Status

Accepted

## Context

Welford statistics are constant-sized per customer but cannot be retained for inactive customers forever. Phase 10 must not alter Phase 8 or Phase 9 state/checkpoint contracts. Transport order is only directly comparable within a stable Kafka partition lineage.

## Decision

Use a separate customer-keyed Spark 4.2 `transformWithState` query with one `ValueState[CustomerAmountStatistics]` named `customerAmountStatistics`. It contains count, mean, M2, latest event-time microseconds, authoritative expiry timer milliseconds, observed Kafka partition, and last observed offset. No historical amount MapState is used.

The query uses `TimeMode.EventTime()` and `TTLConfig.NONE`. An inactivity timer is scheduled at maximum observed event time plus a configurable duration (24 hours locally); only a newer maximum replaces it. The expiry is rounded upward to milliseconds. An older out-of-order event cannot shorten expiry. An obsolete callback cannot clear state. Expiration emits no transaction row; the next observation starts at prior count zero. Watermark progress, not wall-clock passage, drives expiration.

Within one customer invocation rows are sorted by Kafka partition/offset, with strictly advancing offsets. A customer partition change during an active state/checkpoint lineage fails explicitly: offsets on different partitions cannot be safely combined into one transport order. The decision assumes stable Kafka key-to-partition routing for that lineage. A future partition expansion/remapping needs an explicit migration and checkpoint review, not a silent continuation.

Spark 4.2 requires `RocksDBStateStoreProvider` for streaming `transformWithState`, so only this application and its integration test session select it. No RocksDB tuning or performance inference is made. The query has its own checkpoint, target table, and Delta `txnAppId`; each output batch uses `txnVersion=batchId`.

## Alternatives considered

- Reuse Phase 8/9 state: fewer queries, but changes already verified state schemas and mixes distinct semantics.
- Never expire state: simple, but retains inactive customers indefinitely.
- Processing-time TTL: convenient, but introduces wall-clock-dependent lifecycle behavior.
- Compare offsets across changed partitions: cannot define a trustworthy ordering from partition-local offsets alone.

## Trade-offs

One ValueState per active customer is constant sized, but the number of active customers still matters. A stalled watermark delays cleanup. A partition-lineage violation stops the query and requires operational review. Timer policy, state schema, ordering, and watermark are checkpoint semantics.

## Consequences

Changing those semantics ordinarily requires a new checkpoint and Delta transaction application ID. Delta batch retry protection and prior-observed statistical scoring are separate guarantees; neither supports an unconditional exactly-once business-processing claim. State expiry deliberately resets the baseline.
