# ADR-041: Bounded incremental shadow operations and backpressure

## Status

Accepted for local, research-only operations. It grants no model approval or payment-action authority.

## Context

ADR-038 discovers fixed retained Delta versions; ADR-039 publishes and receives one bounded `features.ready` batch; ADR-040 seals one complete historical inbox and compares its results with a frozen Phase 19 ledger. None is an incremental multi-commit operational cursor. In particular, the Phase 26 group, receipt root, inbox identity, per-inbox result path and historical prediction reference are intentionally single-experiment contracts. Simply scanning the latest Delta snapshot or extending that inbox would hide version gaps, Kafka offset changes and accepted-but-unprocessed work.

## Decision

Use `incremental-shadow-operations-v1` in an isolated `.local/` lineage, a stable `sentinel-feature-ready-incremental-shadow-v1` consumer group, and the existing `features.ready` wire/topic. Initialize that group once at a durable per-partition Kafka end-offset fence containing cluster/topic IDs and beginning offsets. A recreated topic or retention gap fails closed. New Delta versions are discovered contiguously, including empty metadata-only commits, with the Phase 24 append-only/schema/table checks. Each version has an immutable segment and discovery record; nonempty versions produce an immutable outbox and publication batch.

Activate only the earliest unaccepted batch. The Phase 25 publisher preserves acknowledgement journals, uncertain-ack retries and source membership. The operational Scala receiver preserves receipt-before-offset commit, checks the preceding accepted offset boundary, and seals exact per-partition batch ranges. Its receipt and acceptance roots are per batch. Python independently rechecks source, batch, journal, receipt and accepted-range evidence before scoring. Accepted batches may queue while inference progresses independently.

The semantic stream ID hashes stable Delta table/schema, Kafka cluster/topic/group, candidate/model tree, first source version and logic version; attempts and timing do not change it. Inference request IDs hash ready-event, candidate, model tree and the new result-contract version, independent of batch packaging. Results are exclusively claimed in a stream/candidate namespace and reused after restart. Progress is reconstructed from contiguous immutable version records, accepted ranges and terminal results, not one mutable cursor. Separate configured bounds cover undiscovered versions, prepared events, accepted/unprocessed events, active publication batches, per-tick inference and cached source rows. Publication pauses when accepting another batch would exceed inference capacity; worker ticks drain it. The upstream Scala feature writer is **not** throttled.

The isolated 25-column local feature fixture is explicitly `LOCAL_INCREMENTAL_FEATURE_FIXTURE`, not evidence that Spark generated those rows. New-fixture probability comparison uses the independently invoked trusted offline scorer, not the historical Phase 19 ledger. Local monotonic operation durations and UTC observations live outside semantic identities and are diagnostic only.

## Alternatives considered

- An infinite in-memory polling loop would obscure stage boundaries and lose progress on restart. A workflow orchestrator would add machinery before a concrete scheduling requirement.
- Rereading all Delta history on every tick or skipping straight to the latest version would mask gaps and produce overlapping notifications.
- Reusing the Phase 26 sealed inbox/result namespace would conflate a completed historical batch with future batches. Accepting arbitrary Kafka batch membership would weaken Phase 25 source verification.
- Scoring before durable Kafka receipt would couple offsets to model availability. Throttling payment processing when research inference stalls would cross a separate operational boundary.
- Synthetic `ingestion_time` is not a measured feature-ready timestamp. Switching candidates automatically on outage would silently change the inference identity. Production SLO alerts are premature without capacity evidence.

## Trade-offs

Only one publication batch is active at a time, favoring checkable membership over transport concurrency. The receiver and worker depend on retained Kafka ranges, exact Delta versions, local outboxes and local candidate bytes. A crash before a durable result may repeat physical inference; receipt and result claims protect local logical identity, not a cross-system transaction. Single-node Kafka and local files are not HA or adversarial-writer-safe. The fixture is deliberately small and does not establish Spark feature-generation throughput or original historical online availability.

## Consequences

Operators run finite, independently restartable discover, publish, accept, process, inspect and reconcile steps. Backpressure controls the downstream research workflow only. A missing source version, changed topic ID, unretained Kafka offset, conflicting artifact or exhausted bound stops progress visibly. No Registry mutation, model promotion, serving, label input, payment enforcement, global exactly-once claim or production latency guarantee follows from this decision.
