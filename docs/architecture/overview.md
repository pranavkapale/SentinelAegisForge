# Architecture overview

## Current architecture

Phase 11's deterministic advisory risk layer builds on two independently buildable module foundations in one repository:

- `streaming-engine` is a Scala 2.13/JDK 21 build with a version 1 transaction domain contract, pure candidate validation, a deterministic seeded simulator, explicit Apache Avro mapping, a bounded registry-backed Kafka producer, Spark 4.2.0 Structured Streaming ingestion, a local Delta 4.4.0 validated-record audit table, a separate watermark-bounded event-ID deduplication table, a customer-keyed activity lifecycle, two prior-only rolling customer features, prior-observed statistical amount features, and stateless deterministic advisory risk decisions. It contains no ML or enforcement capability.
- `model-control-plane` is a Python 3.13 package with pytest, Ruff, and mypy. It contains no model lifecycle or domain implementation.
- The repository root provides pinned, single-node Apache Kafka 4.3.1 and Confluent Schema Registry 8.3.2 runtimes for local development. Infrastructure provisions only `transactions.raw` and its `transactions.raw-value` schema subject.
- `contracts/events/transaction-event-v1.avsc` is the canonical Avro value schema. Published records use UTF-8 `customer_id` keys and registry-managed Avro values.
- Root verification, repository hygiene, baseline CI, this architecture overview, and architecture decision records provide shared engineering conventions.

The Scala module can publish bounded deterministic samples to local Kafka, consume them with registry-aware validation, durably append every validated record plus Kafka coordinates, and derive a separate event-ID-deduplicated Delta table. Phase 8 independently appends customer activity lifecycle snapshots; Phase 9 appends per-transaction prior-only event-time rolling features; Phase 10 consumes those feature rows to append prior-observed Welford amount statistics. Retried micro-batches use table-specific stable Delta transaction identifiers. Spark 4.2 requires RocksDB for these `transformWithState` queries. Monetary feature semantic v2 uses customer+currency membership: rolling velocity stays customer-wide, rolling monetary sums use the current currency, and Welford state is independently keyed and expired per customer-currency pair. Raw amounts in different currencies are never combined in these feature baselines; no FX conversion exists. Fresh v2 output/checkpoint/transaction-ID lineages supersede the old Phase 9/10 monetary semantics without rewriting historical rows (see [ADR-020](../adr/ADR-020-currency-safe-monetary-feature-semantics.md)). These boundaries do not provide lifetime uniqueness, retrospective feature correction, or a general end-to-end exactly-once guarantee. The two runtime modules do not communicate.

Phase 11 consumes only `transaction_statistical_features_v2` with pure ordered R001 velocity, R002 positive READY z-score and R003 combined rules. It preserves every source feature/lineage column and appends CLEAR/REVIEW, ordered rule/reason arrays and version/fingerprint to `transaction_risk_decisions`. There is no stateful operator or RocksDB setting in this query. Explicit thresholds are engineering policy, not production calibration. Decisions are advisory data, not payment actions or numeric scores. A dedicated checkpoint and Delta transaction identity protect source resume and batch retries, not lifetime event+policy uniqueness. See [deterministic risk engine](deterministic-risk-engine.md), [ADR-021](../adr/ADR-021-deterministic-risk-policy.md) and [ADR-022](../adr/ADR-022-risk-policy-versioning.md).

## Target architecture

The following is a future direction, not a description of implemented behavior:

```text
transactions
    ↓
Kafka
    ↓
Scala/Spark streaming engine
    ↓
features + decisions
    ↓
durable/streaming outputs
    ↓
Python model control plane
    ↓
drift / delayed labels / retraining
    ↓
approved model
    ↓
streaming engine
```

Runtime and data technologies will be introduced only when their requirements and operational semantics can be evaluated.

## Engineering principles

1. Correctness before scale.
2. Recovery before optimization.
3. Distributed-system semantics must be explicit.
4. State must be bounded.
5. Event-time behavior must be tested.
6. Idempotency must have defined boundaries.
7. Do not make unconditional "exactly once" claims.
8. Do not make performance claims without reproducible evidence.
9. Production behavior matters more than framework count.
10. Prefer local-first development.
11. Important decisions require architecture decision records (ADRs).
