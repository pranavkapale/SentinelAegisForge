# ADR-004: Local Kafka runtime

## Status

Accepted

## Context

SentinelAegisForge needs a reproducible local streaming broker before producer, serialization, or Spark integration can be evaluated. The project intends to exercise Kafka-specific semantics, while this phase needs only a small local development topology.

## Decision

Use the official JVM `apache/kafka:4.3.1` Docker image in KRaft mode. Run one combined broker/controller for local development, expose a host listener at `localhost:9092`, retain broker data in a Docker-managed volume, and explicitly provision only `transactions.raw`.

The local topic has three partitions and replication factor one. This is local test configuration, not production sizing. This decision does not select a partition key or serialization format.

## Alternatives considered

- **Redpanda:** offers a Kafka-compatible API and a compact local experience, but running Apache Kafka directly reduces behavioral ambiguity for a project intended to demonstrate Kafka semantics.
- **Confluent Platform:** offers a broader integrated ecosystem and operational tooling, but introduces components and conventions that this infrastructure-only phase does not require.
- **Locally installed Kafka binaries:** avoid a container runtime for the broker, but make versions, configuration, storage locations, and cleanup less uniform across developer machines.

## Trade-offs

The official image and Compose file provide a pinned, reviewable local runtime and keep ZooKeeper out of the topology. Docker becomes a prerequisite for integration work, and a combined single-node process favors simplicity over role isolation and availability testing.

## Consequences

Developers can start, stop, reset, and verify the same local Kafka topology through root Make targets. Normal shutdown preserves the broker volume; reset is explicit and destructive. The single node cannot demonstrate broker-level high availability. Production topology, partition key, serialization, retention, security, and consumer configuration remain future decisions.
