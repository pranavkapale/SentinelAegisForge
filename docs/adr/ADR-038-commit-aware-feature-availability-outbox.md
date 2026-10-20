# ADR-038: Commit-aware feature availability and local outbox

## Status

Accepted

## Context

Phase 10.1 writes 25-column statistical feature rows through a retry-safe Delta `foreachBatch` sink. Phase 23 historical shadow replay orders requests by transaction `ingestion_time`, but that time does not establish when a complete feature row became available. The preserved Phase 14 table has versions 0–2 and no Change Data Feed (CDF). A crash may occur after the Delta commit but before any notification or receiver receipt.

## Decision

Treat a successfully committed, retained Delta table version as the source of feature visibility. A bounded Python reader validates every requested version through delta-rs and the existing Phase 12 25-column source contract. For each version, compare its event-ID/complete-row-fingerprint map with the preceding snapshot. Version zero has an empty predecessor. An event's `first_visible_delta_version` is the first version in which its complete row is present. Reject deletion, content changes, repeated event IDs, table/schema/protocol changes, or unreadable history. Metadata-only unchanged versions have explicit empty segments. This is read-only reconstruction; the Scala writer, checkpoint and transaction application ID remain unchanged.

`feature-availability-v1` records a canonical SHA-256 ready-event ID over contract version, table ID, event ID, first-visible version and full source-row fingerprint, plus source schema fingerprint and `DELTA_COMMITTED` state. Order by `(first_visible_delta_version,event_id)`; sorting within a commit does not imply arrival or Spark-task order. Persist one immutable `feature-commit-segment-v1` per examined version and one immutable `feature-ready-outbox-v1` over a contiguous inclusive version range. Each records deterministic logical identity and artifact byte hashes. Rebuilds inspect existing directories; incomplete or conflicting output fails, never overwrites. The Delta table remains authoritative; segments/outbox are reconstructible projections.

A bounded local research receiver verifies the outbox, writes one durable receipt per `ready_event_id`, records separate delivery attempts, and derives progress from receipts. Identical redelivery is recognized; conflicting reuse fails. A partial segment never advances the fully acknowledged version. Local attempts are at least once; a receipt proves only this no-side-effect receiver accepted an envelope. The receiver neither scores a model nor performs payment/risk/Registry actions.

Delta version, Spark checkpoint-scoped `batch_id`, transaction `event_time`, upstream `ingestion_time`, outbox order, delivery attempt and receipt are distinct. `first_visible_delta_version` is logical visibility evidence, not a wall-clock feature-ready timestamp. Historical Phase 14 reconstruction is labeled `HISTORICAL_COMMIT_RECONSTRUCTION`, not real-time notification. No commit-to-consumer latency or multi-sink atomicity is claimed.

## Alternatives considered

- Notify Kafka before Delta commit: could expose a row that never commits.
- Depend only on `DeltaPostCommitHook`: a crash after commit but before the callback loses the signal; the hook is not durable progress.
- Write Delta and Kafka within one `foreachBatch`: two independent sinks do not become atomic merely by sharing a callback.
- Turn on CDF now and assume historical changes exist: retroactive enabling cannot recover unrecorded CDF events and would alter the established table protocol.
- Scan only the latest snapshot: finds present rows but cannot assign a verified first-visible version or detect earlier mutations.
- Use transaction `ingestion_time` as readiness: it is an upstream timestamp, not feature materialization evidence.
- Build a production shadow consumer now: premature while the model remains research-only and transport/operational contracts are incomplete.
- Treat a local acknowledgement as exactly-once business processing: an external side effect may occur before its receipt, so replay could repeat it.

## Trade-offs

Repeated full snapshots cost more than CDF and are bounded here to 64 versions and 5,000 rows/events for local research. Historical log/data retention is mandatory; missing versions fail rather than being inferred from latest. Hashes detect ordinary byte/content drift, not malicious tampering. Exclusive local artifact claims and file-linked receipts do not supply globally coordinated writers or distributed exactly-once delivery. Receipt and attempt files are separate durability boundaries; a crash can leave an accepted receipt without an attempt log. This is diagnosed from the receipt rather than treated as a failed delivery.

## Consequences

Committed rows can be rediscovered after a missed handoff without re-running Welford computation or the original `foreachBatch`. A published outbox can be replayed after a receiver crash; identical logical ready events retain their IDs. Unsupported source updates/deletes require a future change-event contract. Efficient live CDF, Kafka transport, timestamps/latency SLOs, consumer side-effect atomicity, access control, HA, model eligibility and payment enforcement remain separate decisions.
