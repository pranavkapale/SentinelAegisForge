# ADR-013: Event-time watermark semantics

## Status

Accepted

## Context

Streaming deduplication must tolerate bounded out-of-order event arrival while eventually releasing old event IDs from state. Processing time cannot express this event-domain boundary, and unwatermarked deduplication would retain state without a semantic bound.

## Decision

Use `event_time` as the event-time column. Apply a configurable delay with `withWatermark("event_time", configuredDelay)` before `dropDuplicatesWithinWatermark("event_id")`.

The local verification baseline is `10 minutes`. It is a controlled development and testing policy, not a production-derived lateness SLA. Changing the watermark column, delay, deduplication key, or stateful topology requires checkpoint-compatibility review; this project prefers a new checkpoint lineage and Delta transaction application ID for an intentional semantic change.

A unique event newer than the active watermark remains eligible even when it is older than the newest observed event. A record older than the active watermark is dropped by Spark's stateful operator in the current phase. Query progress exposes the watermark, state row counts, removed rows, dropped-by-watermark rows, and state memory where Spark reports them.

## Alternatives considered

- Processing-time policy: operationally simple, but does not express when the business event occurred and makes arrival delays determine semantics.
- No watermark with unbounded deduplication state: maximizes the remembered horizon but has no defensible state bound.
- Custom late-event side output: could retain too-late valid events, but requires a separate operational contract and storage/reprocessing policy that has not been established.

## Trade-offs

The watermark both defines tolerated event-time disorder and bounds deduplication state. Events beyond the active boundary are dropped rather than retained, and very old duplicates may be accepted again after their state has expired. Correct threshold selection will require future production evidence.

## Consequences

The deduplication query has a dedicated checkpoint containing Delta source progress, watermark progress, state, and sink progress. Checkpoint files must never be edited manually. No DLQ or late-event side output is created in this phase; late drops remain observable through Structured Streaming progress diagnostics.
