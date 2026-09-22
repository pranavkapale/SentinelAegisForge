# ADR-008: Kafka producer delivery semantics

## Status

Accepted

## Context

The first bounded transaction producer must surface delivery failures and avoid duplicates caused by supported producer retries, without claiming a broader business-processing guarantee. There is one producer writing one topic and no atomic multi-topic or consume-transform-produce boundary.

## Decision

Configure `acks=all` and `enable.idempotence=true`. Send bounded batches asynchronously, observe every returned result, flush, and close the producer with a bounded timeout. Use Kafka's built-in retry behavior; do not wrap sends in an application-managed retry loop.

Do not configure `transactional.id` or invoke Kafka transaction APIs. Transactions will be considered only when an implemented atomicity boundary requires them.

Producer idempotence protects against duplicate writes caused by retries within Kafka's supported producer-session semantics. It is not exactly-once business processing.

## Alternatives considered

- **`acks=1`:** can reduce the acknowledgement boundary to the leader, but provides a weaker durability signal than the selected correctness-oriented setting.
- **`acks=0`:** minimizes acknowledgement work but cannot provide per-record broker success or failure evidence.
- **Kafka transactions:** provide atomic write semantics when a transactional boundary exists, but add lifecycle and fencing complexity without a current multi-record atomicity requirement.
- **Application-managed retries:** can implement domain-specific policies, but naive retries layered over producer retries can create duplicates unless their identity and recovery boundaries are designed explicitly.

## Trade-offs

Waiting for all in-sync replicas and observing every send favors explicit delivery evidence over minimum latency. In the current single-broker topology, `acks=all` still acknowledges only that one broker and does not create high availability. Idempotence adds correctness within a producer session but no durable business-event memory across process runs.

## Consequences

Publishing fails visibly if any record is not acknowledged. Producer resources are always closed. This does not protect against business-event duplicates, rerunning identical simulator input, replay, producer restart followed by republishing, or downstream duplicate materialization. Business-event deduplication remains a later processing responsibility.
