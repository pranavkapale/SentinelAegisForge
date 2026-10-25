# ADR-042: Stateful streaming features to research shadow integration

## Status

Accepted for isolated local integration verification. Live results and limitations are recorded in `PROJECT_STATE.md`; this decision grants no model approval or payment-action authority.

## Context

Phase 27 verified the commit-aware Kafka research workflow using valid, controlled 25-column feature fixtures. That proved its transport and inference contracts but not the upstream Scala stateful feature generation path. The existing Phase 14 generator and publisher, Spark/Delta processing stages, and Phase 24–27 handoff already provide the needed components. The shared `transactions.raw` topic also retains older data, so an isolated Delta directory alone cannot establish source isolation.

## Decision

Run a finite three-wave experiment from a fresh deterministic transaction plan. Record the broker's topic identity and per-partition end offsets, then prime a new validated Spark checkpoint with `startingOffsets=latest` before publishing. Reuse the existing Scala producer, validated ingestion, event-ID deduplication, currency-safe rolling transformation and customer-currency statistical transformation. Each stage receives a separate Delta path, checkpoint and stable transaction application ID derived from one experiment identity. Reuse each checkpoint and application ID across waves; retain RocksDB state and the ten-minute local watermark policy.

Treat the committed statistical Delta table as the source of truth for feature readiness. Discover its first-visible versions, construct the existing immutable segments/outboxes, publish the established model-independent `features.ready` envelope, and accept it with an experiment-scoped operational group and receipt namespace. Phase 27's bounded worker scores only source-verified accepted notifications using the unchanged research-only candidate. Check planned, acknowledged, validated, deduplicated, rolling, statistical, ready, accepted and terminal event-ID sets; preserve the distinct source, transport and inference identities. Observe stage durations with a monotonic process clock, separately from deterministic identity. These `AvailableNow` invocations are bounded local runs, not a continuously running service.

The Delta writer and Kafka publisher are separate operations. A committed feature may precede or outlive a failed publication; retained Delta versions and the outbox permit recovery. Receipt and result identities protect local logical retries, without making the systems atomic or guaranteeing globally exactly-once execution. Research backpressure limits downstream publication only. Synthetic truth, labels and Phase 11 decisions are not inference inputs.

## Alternatives considered

- Continuing with precomputed 25-column fixtures is useful for isolated transport tests, but cannot verify the Scala feature pipeline.
- Reimplementing rolling or Welford logic in Python would create a competing processor and obscure state/checkpoint behavior.
- Publishing scores directly from Spark `foreachBatch` would couple research model availability to feature commits and bypass the established outbox.
- Resetting Kafka topics/checkpoints for every wave would destroy retained evidence and prevent state-continuity testing. Reusing Phase 14 historical Delta paths would mix lineages.
- Treating a successful Spark batch as proof of Kafka publication would ignore a real cross-system failure boundary.
- Coupling inference completion to transaction processing would let a research backlog control the payment-facing path.
- Adding online serving or retraining now would require distinct governance and evaluation decisions. A local full-path run cannot establish production readiness.

## Trade-offs

The controlled harness is deliberately operator-driven and sequential. It relies on retained Kafka records, Delta history, compatible Spark checkpoints, the local Registry/candidate and single-writer artifact claims. Strict exact-set reconciliation may stop on a legitimate watermark exclusion until that loss is inspected and documented. The shared transaction topic requires a recorded offset fence and an explicit fresh checkpoint. The local monotonic durations describe these invocations only; synthetic event timestamps must not be subtracted from host time as latency measurements.

## Consequences

Phase 28 evidence can distinguish real Scala-produced features from Phase 27 fixtures and trace a selected transaction through committed features and research results. A restart must preserve the same stage configuration and application IDs. Missing source versions, conflicting artifacts, unaccounted events or failed results stop completion visibly. The registered model remains `REGISTERED_RESEARCH_ONLY`, `production_eligible=false`, with the Phase 18 assessment `NOT_ELIGIBLE_FOR_PROMOTION`. No payment enforcement, model promotion, model retraining, production latency SLO or exactly-once business claim follows.
