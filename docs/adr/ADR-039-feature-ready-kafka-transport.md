# ADR-039: Kafka transport for committed-feature notifications

## Status

Accepted

## Context

ADR-038 established retained Delta commits as authoritative feature visibility and a reconstructible, immutable local outbox. Its receipt exercise did not use a broker. Research transport now needs independent publication, consumption and restart evidence without involving the production-ineligible model or payments.

## Decision

Publish verified Phase 24 `feature-availability-v1` envelopes to an explicitly provisioned, delete-retained `features.ready` topic (three local partitions, replication factor one). A Python export re-inspects the Delta-backed outbox and creates an immutable bounded `feature-ready-kafka-publish-batch-v1`; the Scala publisher verifies its byte/hash/source bindings before using Kafka. The key is UTF-8 `ready_event_id`; the value is the exact canonical UTF-8 JSON envelope plus one newline. Cross-language SHA-256 vectors fix the identity contract. Unknown fields, duplicate JSON keys, mismatched identity/key and noncanonical values fail. The topic has no full feature rows, labels or model scores.

Use `acks=all`, producer idempotence, bounded retries/timeouts and a local exclusive acknowledgement journal indexed by ready-event ID, recording topic/cluster identity and broker partition/offset. Normal restart publishes only IDs without durable acknowledgement. An explicit research replay publishes acknowledged IDs again without replacing the original evidence. A broker append/ack before local journal persistence is uncertain after a crash; restart may create another physical Kafka record. The outbox remains the recovery authority. Kafka idempotence does not close this process-restart window, and Kafka transactions are not atomic with Delta.

An independent stable research consumer group starts at earliest if it has no offsets, disables auto-commit, validates exact membership and envelope bytes against one verified batch, then durably publishes one receipt per ready-event ID. Each physical coordinate and delivery attempt is recorded separately. Only after receipt publication/verification is `offset+1` synchronously committed for that partition. A crash before receipt leaves the offset uncommitted; a crash after receipt but before commit causes redelivery, verified duplicate receipt reuse and safe offset advancement. Conflicting payloads stop without committing past the record. Bounded runs capture per-partition end-offset fences; incomplete runs do not reconcile. A separate broker scan compares source, journal, physical log, receipts and group offsets. This is a single-consumer local research contract; concurrent group rebalance/HA and authenticated cross-host writers are not established.

## Alternatives considered

- Publishing directly inside Scala `foreachBatch`, or deleting the Phase 24 outbox, would couple independent Delta and Kafka effects without a durable recovery source.
- Producer idempotence alone does not prevent post-crash application replay duplicates.
- Kafka offsets identify transport positions, not logical ready events; one global cursor is invalid across partitions.
- Auto-commit or commit-before-receipt can lose an unprocessed notification after failure.
- In-memory deduplication disappears at restart.
- Full feature-row messages increase exposure and duplicate authoritative state in the notification.
- Model inference in this consumer would cross the research-only governance boundary without serving/side-effect semantics.
- Kafka transactions cannot make Delta commits and Kafka publications one atomic operation.

## Trade-offs

Canonical JSON is inspectable and reuses Phase 24 identity, but is larger than a compact binary format and requires strict cross-language byte discipline. One filesystem receipt and synchronous offset commit per notification favor a small auditable local run over throughput. Local file links, fsync and hashes detect normal corruption, not hostile writers or distributed consensus. Three partitions give partition-scoped ordering only; Delta version order is not a global broker order. Key-to-partition mapping can change if partition count changes. A hot key can skew traffic, though ready-event IDs are high cardinality. Delete retention keeps duplicate evidence for the local experiment but does not guarantee indefinite replay. Local plaintext single-node Kafka has no HA, ACL/TLS or production retention policy.

## Consequences

The committed Delta table, outbox, broker acknowledgements and durable research receipts remain distinct evidence boundaries. Reconciliation can report more physical records than unique ready events after explicit replay or an uncertain acknowledgement. A receipt proves only validated research acceptance, not model execution or payment action. Future feature-update semantics, authentication, retention/SLOs, multi-consumer rebalance behavior, production operation and inference remain separate decisions.
