# ADR-012: Business-event deduplication

## Status

Accepted

## Context

`transactions_validated` intentionally records every successfully validated Kafka record. Retries, replays, or upstream publication can therefore place multiple transport records for the same business event in that audit table. Downstream semantic processing needs a bounded way to accept one occurrence without changing the audit grain established in Phase 6.

The domain contract distinguishes `event_id`, which identifies a business-event occurrence, from `transaction_id`, which identifies the transaction being described. Kafka topic/partition/offset coordinates identify transport records.

## Decision

Create a separate `transactions_deduplicated` Delta table. Use `event_id` as its sole business deduplication key and Spark Structured Streaming's `dropDuplicatesWithinWatermark("event_id")` after an `event_time` watermark. For duplicate event IDs represented by different Kafka records, the first accepted occurrence wins and its Kafka metadata is retained.

## Alternatives considered

- `transaction_id`: useful for transaction-level grouping, but one transaction may legitimately produce multiple business events.
- Kafka coordinates: uniquely identify transport records and are necessary for auditability, but cannot identify repeated publication of one business event.
- Full-row equality: would preserve records whose transport metadata or conflicting payload differs even when their business-event identity is the same.
- `event_id`: matches the current domain identity semantics while leaving transaction and transport identities intact.

## Trade-offs

The separate table keeps the Phase 6 audit history complete and gives downstream processing a clear semantic grain. Watermark-bounded state avoids lifetime-unbounded event-ID storage, but duplicates outside retained state are not guaranteed to be suppressed.

Native event-ID deduplication does not compare duplicate payloads. If a later record reuses an `event_id` with different content, it is suppressed rather than reported as a conflict. Conflict detection is a deferred data-integrity capability.

## Consequences

Downstream semantic stages may consume `transactions_deduplicated` when they need the first accepted occurrence of an event within the active watermark semantics. Kafka metadata remains available but does not define business equality. This decision provides neither lifetime uniqueness nor an exactly-once business-processing guarantee.
