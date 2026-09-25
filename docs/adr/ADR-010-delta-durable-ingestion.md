# ADR-010: Delta durable ingestion

## Status

Accepted

## Context

The Phase 5 Spark query validates registry-backed transaction records but ends at a diagnostic sink. Module A needs a durable, inspectable materialization before stateful features or fraud decisions are introduced. The storage decision must preserve the validated event and its Kafka coordinates while allowing restart and retry behavior to be tested locally.

## Decision

Use Delta Lake 4.4.0 with Spark 4.2.0. Materialize one row per successfully validated Kafka record using an explicit snake_case storage schema. Preserve the trusted transaction fields, UTF-8 Kafka key, topic, partition, offset, and broker timestamp. Store money as `decimal(18,4)` and event, ingestion, and Kafka times as Spark timestamps in a UTC Spark session.

Use a path-based Delta table on the local filesystem for this phase. Do not physically partition the initial table: its correctness dataset is small, and no measured query or write pattern justifies a layout decision yet.

## Alternatives considered

- **Plain Parquet:** simple and broadly interoperable, but it lacks the transaction log and duplicate-transaction handling used by the recovery boundary.
- **Database sink:** could provide transactional storage and indexing, but would add an operational service and shift this data-stream/lakehouse foundation toward a database-specific write path.
- **Kafka-only retention:** avoids a second storage system, but does not provide the intended durable analytical table or Delta transaction semantics.
- **Immediate MinIO/S3 storage:** is closer to a future object-store topology, but mixes sink correctness with object-store configuration before that infrastructure is needed.

## Trade-offs

Delta provides a transaction log, schema enforcement, Spark streaming integration, inspectable history, and a path toward later lakehouse storage. It adds a pinned runtime dependency and local transaction-log files. Local filesystem durability is limited to one development machine and is not a production availability design.

## Consequences

- The canonical local table path defaults to `.local/delta/transactions_validated`.
- The table grain is one successfully validated Kafka record, not one unique business event.
- Schema changes and physical layout changes require deliberate evaluation and tests; automatic schema merging is not enabled.
- MinIO/S3, catalog registration, retention policy, optimization, and production storage topology remain deferred.
- Retry behavior is governed separately by ADR-011.
