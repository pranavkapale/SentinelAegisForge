# ADR-015: Customer state expiration

## Status

Accepted

## Context

Per-customer state must not remain active forever, but inactivity must follow transaction event time rather than machine wall-clock time. Out-of-order transactions must update totals without moving the latest event time or expiration boundary backward.

## Decision

Use a configurable event-time inactivity timer at:

```text
latestEventTime + configured inactivity duration
```

The ordinary local default is 24 hours. Controlled verification uses one hour. Neither value is a production-derived inactivity policy.

When a newer `latestEventTime` is observed, delete the previous timer and register its replacement. An older out-of-order event does not move the timer. The expected expiry timestamp is stored in `CustomerActivityState`; an expired callback clears state only when its timestamp matches the stored timestamp, so a stale callback cannot delete newer state.

Timer expiry emits an `EXPIRED` snapshot and clears `customerActivity`. A later eligible event for that customer starts a new lifecycle with count one. Expiration depends on the query watermark advancing past the timer. Event-time timers are not wall-clock TTL, and state-variable TTL remains disabled.

## Alternatives considered

- Processing-time TTL: useful for operational cache expiry, but ties lifecycle semantics to runtime clock and downtime rather than transaction event time.
- No state expiration: simple, but allows the number of retained customer keys to grow without a semantic release rule.
- Manual scheduled cleanup: adds a second cleanup workflow and coordination boundary.
- Rely on RocksDB for expiration: RocksDB is narrowly required by Spark 4.2 for `transformWithState`, but a storage backend does not define the business inactivity boundary. The explicit event-time timer remains authoritative.

## Trade-offs

The policy is deterministic in event time and keeps state bounded by active customer lifecycles. A stalled watermark also stalls expiration, and late records at or behind the active watermark do not reactivate state.

## Consequences

Watermark progress and state-removal metrics are required diagnostics. Timer and watermark semantics form part of the checkpoint contract. Processing-time TTL, late-event recovery, and migrated initial state remain deferred.

