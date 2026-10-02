# ADR-017: Rolling feature state retention

## Status

Accepted

[ADR-020](ADR-020-currency-safe-monetary-feature-semantics.md) adds currency to retained events under a fresh v2 lineage. The original v1 state description below is historical; retention and timer behavior are unchanged.

## Context

Rolling features need event-level history within their maximum useful horizon. Phase 8's constant-sized lifecycle value cannot provide these lookups without changing its existing state contract. Retained history and timer counts need explicit bounds.

## Decision

Use a separate customer-keyed Spark 4.2 `transformWithState` query. `MapState[String, RollingCustomerEvent]` named `rollingEvents` maps `event_id` to only epoch microseconds and the exact original amount. `ValueState[Long]` named `nextCleanupTimerMs` records one authoritative event-time timer per active customer. Both use `TTLConfig.NONE` and `TimeMode.EventTime()`.

An event's cleanup deadline is its event time plus ten minutes, the maximum feature horizon. Timer milliseconds are rounded upward from microseconds. If a newly received older event requires an earlier cleanup, replace the timer. An authoritative callback removes all entries due by the current watermark, including multiple expiries crossed by one watermark jump, then schedules the next earliest deadline or clears timer state. A callback not matching the stored timer cannot remove history.

Spark 4.2 requires `RocksDBStateStoreProvider` for streaming `transformWithState`; only this application and its integration tests configure it. No RocksDB tuning or performance conclusion is implied.

## Alternatives considered

- Reuse Phase 8 `customerActivity`: fewer queries, but silently changes a verified state schema and checkpoint lineage.
- Keep lifetime event history: easiest lookup, but unbounded state.
- One timer per event: direct expiry mapping, but timer count grows with event volume.
- Processing-time TTL: operationally convenient, but cannot enforce event-time window cleanup.

## Trade-offs

Stored event count grows with traffic inside the ten-minute horizon; it is time bounded when the watermark advances, not constant sized. A stalled watermark stalls cleanup. The timer and MapState schema introduce a new checkpoint compatibility surface. Online output remains append only and is not retroactively corrected after late arrivals.

## Consequences

The Phase 9 query has its own checkpoint, output Delta table, and retry transaction application ID. A change to state schema, timer policy, watermark, or feature semantics requires explicit compatibility review and normally a new lineage. No external state store, RocksDB tuning, or performance benchmark is introduced.
