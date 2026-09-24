# Architecture overview

## Current architecture

Phase 5 builds on two independently buildable module foundations in one repository:

- `streaming-engine` is a Scala 2.13/JDK 21 build with a version 1 transaction domain contract, pure candidate validation, a deterministic seeded simulator, explicit Apache Avro mapping, a bounded registry-backed Kafka producer, and minimal Spark 4.2.0 Structured Streaming ingestion. It contains no durable application sink, stateful processing, or fraud decisioning.
- `model-control-plane` is a Python 3.13 package with pytest, Ruff, and mypy. It contains no model lifecycle or domain implementation.
- The repository root provides pinned, single-node Apache Kafka 4.3.1 and Confluent Schema Registry 8.3.2 runtimes for local development. Infrastructure provisions only `transactions.raw` and its `transactions.raw-value` schema subject.
- `contracts/events/transaction-event-v1.avsc` is the canonical Avro value schema. Published records use UTF-8 `customer_id` keys and registry-managed Avro values.
- Root verification, repository hygiene, baseline CI, this architecture overview, and architecture decision records provide shared engineering conventions.

The Scala module can publish bounded deterministic samples to local Kafka and consume them with registry-aware validation while preserving Kafka coordinates. The current sink is diagnostic; no durable application storage, event-time/stateful behavior, fraud decisioning, or ML capability exists yet, and the two runtime modules do not communicate.

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
