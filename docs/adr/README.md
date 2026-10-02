# Architecture decision records

ADRs capture important architectural decisions and their consequences. Use the next sequential number, keep each record concise, and update its status rather than silently rewriting an accepted decision.

## Template

```markdown
# ADR-NNN: Decision title

## Status

Proposed | Accepted | Superseded by ADR-NNN

## Context

What forces or constraints require a decision?

## Decision

What was decided?

## Alternatives considered

What other viable options were evaluated?

## Trade-offs

What benefits and costs influenced the decision?

## Consequences

What becomes easier, harder, required, or intentionally deferred?
```

## Accepted records

- [ADR-001: Monorepo module boundaries](ADR-001-monorepo-module-boundaries.md)
- [ADR-002: Development toolchains](ADR-002-development-toolchains.md)
- [ADR-003: Transaction event domain boundary](ADR-003-transaction-event-domain-boundary.md)
- [ADR-004: Local Kafka runtime](ADR-004-local-kafka-runtime.md)
- [ADR-005: Transaction event serialization](ADR-005-transaction-event-serialization.md)
- [ADR-006: Kafka transaction partition key](ADR-006-kafka-transaction-partition-key.md)
- [ADR-007: Schema Registry governance](ADR-007-schema-registry-governance.md)
- [ADR-008: Kafka producer delivery semantics](ADR-008-kafka-producer-delivery-semantics.md)
- [ADR-009: Spark Structured Streaming ingestion](ADR-009-spark-structured-streaming-ingestion.md)
- [ADR-010: Delta durable ingestion](ADR-010-delta-durable-ingestion.md)
- [ADR-011: Delta streaming idempotency](ADR-011-delta-streaming-idempotency.md)
- [ADR-012: Business-event deduplication](ADR-012-business-event-deduplication.md)
- [ADR-013: Event-time watermark semantics](ADR-013-event-time-watermark-semantics.md)
- [ADR-014: Customer stateful processing](ADR-014-customer-stateful-processing.md)
- [ADR-015: Customer state expiration](ADR-015-customer-state-expiration.md)
- [ADR-016: Customer rolling feature semantics](ADR-016-customer-rolling-feature-semantics.md)
- [ADR-017: Rolling feature state retention](ADR-017-rolling-feature-state-retention.md)
- [ADR-018: Customer statistical feature semantics](ADR-018-customer-statistical-feature-semantics.md)
- [ADR-019: Customer statistical state lifecycle](ADR-019-customer-statistical-state-lifecycle.md)
- [ADR-020: Currency-safe monetary feature semantics](ADR-020-currency-safe-monetary-feature-semantics.md)
