# ADR-014: Customer stateful processing

## Status

Accepted

## Context

The deduplicated transaction table is the durable semantic input for future customer-centric processing. Phase 8 needs to prove keyed state updates, event-time timers, checkpoint restoration, and bounded state size per customer before adding rolling risk features.

Spark 4.2's arbitrary state API v2 rejects its default HDFS-backed state store for streaming `transformWithState` queries and currently supports only `RocksDBStateStoreProvider`. Live execution confirmed this constraint with `STORE_BACKEND_NOT_SUPPORTED_FOR_TWS` before the provider was selected.

## Decision

Read `transactions_deduplicated` as a Delta streaming source, watermark `event_time`, group by `customer_id`, and use Spark 4.2 `transformWithState` with `TimeMode.EventTime()` and `OutputMode.Update()`.

Maintain exactly one constant-sized `ValueState[CustomerActivityState]` named `customerActivity` for each active customer. It stores event count, exact amount total, first/latest event time, and the registered expiration timestamp. TTL is disabled with `TTLConfig.NONE`.

Configure `RocksDBStateStoreProvider` only for this Phase 8 application and its tests because Spark 4.2 requires that provider for streaming `transformWithState`. This is a framework-compatibility requirement, not a performance-based state-store choice or optimization claim. Other project streaming queries retain their existing session configuration.

## Alternatives considered

- `mapGroupsWithState` or `flatMapGroupsWithState`: mature state APIs, but legacy relative to the Spark 4.x arbitrary state API v2 this phase is intended to exercise.
- SQL aggregation or windows: useful for declarative aggregates, but not a direct fit for an explicit customer lifecycle with replaceable timers and lifecycle outputs.
- External Redis or database state: can support independently managed online state, but adds another distributed consistency boundary before it is required.
- Retain the default HDFS-backed state store: preferred initially, but Spark 4.2 rejects it for streaming `transformWithState`, so it cannot execute this decision.

## Trade-offs

The processor has an explicit, testable lifecycle and constant-sized state, and checkpoint recovery is owned by Spark. It also couples this query lineage to an evolving Spark API and to RocksDB checkpoint state because of the current Spark 4.2 implementation constraint.

## Consequences

The query needs its own checkpoint and a dedicated Delta transaction application ID. Changes to its key, state schema, timers, time mode, watermark, or stateful topology require checkpoint-compatibility review and normally a fresh lineage. Rolling features, fraud scoring, external state services, and state migration remain deferred.

