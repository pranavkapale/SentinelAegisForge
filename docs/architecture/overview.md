# Architecture overview

## Current architecture

Phase 3 builds on two independently buildable module foundations in one repository:

- `streaming-engine` is a Scala 2.13/JDK 21 build with a version 1 transaction domain contract, pure candidate validation, a deterministic seeded simulator, and an explicit Apache Avro mapping and local binary codec. It contains no Kafka client, streaming runtime, or fraud decisioning.
- `model-control-plane` is a Python 3.13 package with pytest, Ruff, and mypy. It contains no model lifecycle or domain implementation.
- The repository root provides a pinned, single-node Apache Kafka 4.3.1 KRaft environment for local development. It explicitly provisions only `transactions.raw`; no producer or consumer exists.
- `contracts/events/transaction-event-v1.avsc` is the canonical Avro value schema. The planned `transactions.raw` record key is the UTF-8 `customer_id`; the key is documented but no application currently writes Kafka records.
- Root verification, repository hygiene, baseline CI, this architecture overview, and architecture decision records provide shared engineering conventions.

The modules do not communicate. Kafka infrastructure and a transport contract exist, but no application transport integration, streaming processing, durable application storage, fraud decisioning, or ML capability exists yet.

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
