# ADR-011: Delta streaming idempotency

## Status

Accepted

## Context

Spark `foreachBatch` can invoke a micro-batch callback again after a failure. A process can fail after a Delta write commits but before Spark records batch completion in its checkpoint. A plain append would then materialize the retried micro-batch twice.

## Decision

Write each non-empty micro-batch through `foreachBatch` with a stable configured Delta `txnAppId` and `txnVersion = batchId`. Delta treats a repeated write with the same transaction pair as already applied, so retrying that logical micro-batch is a no-op at the table transaction boundary.

The `txnAppId` identifies one sink/checkpoint lineage. The local default is `sentinel-transactions-validated-v1`; it is never derived from a random UUID, wall-clock time, or Spark query ID. A diagnostic failpoint can throw only after the Delta write returns successfully, allowing the commit-before-checkpoint recovery boundary to be exercised.

Operational rule: **a new checkpoint lineage must use a new Delta transaction application ID.** Deleting a checkpoint resets Spark batch IDs. Reusing the old application ID can cause legitimate new batches to reuse committed transaction versions and be ignored.

## Alternatives considered

- **Plain `foreachBatch` append:** is simpler but can duplicate an entire batch after a commit-before-checkpoint failure.
- **Delta `MERGE` by event ID:** could implement business-key deduplication, but it is a different semantic and performance decision that is not required for micro-batch retry safety.
- **Random application ID per process:** avoids transaction-version collisions across new lineages, but makes process restarts unable to recognize retries from the same lineage.
- **Rely only on Spark checkpoint progress:** tracks source progress but cannot alone make an external `foreachBatch` side effect idempotent.

## Trade-offs

The transaction pair creates a clear, testable micro-batch retry boundary with little code. Operators must keep checkpoint and application-ID lifecycle coupled, and diagnostics must distinguish callback retries from business-level duplicate events.

## Consequences

- The same batch ID and application ID can be retried without appending its rows again.
- `foreachBatch` itself is not described as exactly once; the idempotency comes from Delta's transaction options.
- Event-level duplicates already present in Kafka remain separate rows. No business-event deduplication is implemented.
- Resetting or replacing a checkpoint requires an explicitly new `txnAppId`.
- Replay policy, business deduplication, DLQ behavior, and multi-sink atomicity remain deferred.
