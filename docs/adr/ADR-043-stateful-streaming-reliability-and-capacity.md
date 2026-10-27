# ADR-043: Stateful streaming reliability and local capacity characterization

## Status

Accepted for Phase 29 experiments. Results and incomplete gates are recorded in `PROJECT_STATE.md`; this decision grants no production availability, capacity or model approval.

## Context

Phase 28 reconciled three waves from the real Scala producer through customer-currency Welford state and research-only inference. It did not force a failure after a statistical Delta commit, check checkpoint compatibility on a controlled retry, or characterize changed workload and downstream interruption with a common evidence contract. A successful Delta append alone does not prove Spark state recovery. Local `AvailableNow` process time includes startup and cannot be interpreted as steady-state throughput or event-to-decision latency.

## Decision

Use fresh, isolated Phase 28 lineages for A (baseline), B (changed bounded workload), C (statistical post-commit fault) and D (downstream outage). Preserve the existing Scala producer, Kafka key, Spark transformations, Delta schemas, RocksDB provider, feature-ready transport and research candidate. The statistical application accepts an opt-in nonnegative batch ID and invokes its existing `DeltaPostCommitHook.FailAfterBatch` only after the Delta write returns. The normal path has no fault hook. Restart the failed stateful query with the same checkpoint, transaction application ID and compatible source/target identities, without the hook. Reconcile source and output event IDs, committed rows and subsequent Welford history; separately reconcile ready notifications and research results.

An experiment-scoped compatibility record binds Spark/Scala versions, application and state semantics, RocksDB provider, watermark, inactivity timeout, source Delta table ID, target path, checkpoint and Delta transaction application ID before the first statistical query. Bind the target Delta table ID after its first commit. Reject changed configuration or target identity on retry; never edit checkpoint files or silently clear state. Event-time timer expiry and later reactivation are distinct from retained-state continuation and must be classified using observed progress and output histories.

Capture real Spark progress JSON, Kafka partition/customer-key distribution, statistical status and customer-currency state composition, local file inventory, and monotonic durations of bounded invocations. Report process invocation and Spark trigger execution separately. The remaining interval includes setup, teardown and logging, so it is not a precise startup measurement. Every bounded invocation starts a new process; distinguish new-state initialization from checkpoint restoration rather than calling every later process warm. Compare repeated samples with explicit sample counts. Treat these as local observations, not production SLOs, scaling laws or throughput capacity. Preserve immutable, hashed reliability artifacts and inspect them read-only. Kafka transaction and feature-ready retention ranges and Delta history gaps fail closed; old physical offsets cannot be recovered by seeking latest. Outbox republishing, if used, creates new auditable transport evidence. Bounded research backpressure does not throttle the feature writer.

## Alternatives considered

- Declaring production readiness after Phase 28 or testing only validated-ingestion failure would miss the statistical stateful recovery boundary.
- Treating Delta `txnAppId`/`txnVersion` as proof of Spark state recovery would conflate sink retry safety with checkpoint restoration.
- Clearing checkpoints or fabricating feature rows would hide the failure being tested.
- Using synthetic event timestamps or one `AvailableNow` invocation as throughput or latency would confuse simulated chronology and process startup with service behavior.
- Introducing a continuous service solely for benchmarking would expand deployment scope without a production workload contract.
- Tuning RocksDB, increasing Kafka partitions or adding arbitrary overload thresholds before observing state/partition metrics could change correctness or ordering without evidence.
- Automatically retraining or switching models during research outages would cross the current governance boundary.

## Trade-offs

The matrix takes multiple bounded local runs and retains their disk state. The guard is scoped to the Phase 29 operator workflow; historical Phase 28 checkpoints have no retroactive compatibility record. Spark progress fields depend on the installed runtime, and missing fields remain absent rather than inferred. Small synthetic workload differences may be informative but cannot establish production capacity. Broker and Schema Registry remain single-node local infrastructure.

## Consequences

Phase 29 can inspect actual post-commit retry behavior and compare isolated workload observations without mutating prior evidence. A source-retention or compatibility gap stops recovery rather than silently seeking forward. Delta retry idempotency, Spark state restoration, Kafka publication and research inference remain separate boundaries. No automatic tuning, production SLO, model approval, payment enforcement or exactly-once business guarantee follows.
