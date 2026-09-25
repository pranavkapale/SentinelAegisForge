# Local Kafka foundation

## Purpose

Kafka will provide the transport boundary between transaction sources and the future streaming engine. Step 2 establishes only a reproducible local broker and raw input topic. It does not produce, consume, serialize, validate, or process transaction messages.

## Current implementation

The local environment uses the official JVM `apache/kafka:4.3.1` image as one combined KRaft broker/controller. Host applications connect to `localhost:9092`; future services on the Compose network can connect to `kafka:19092`. The controller listener is internal to the container network.

The single application topic is `transactions.raw`, explicitly provisioned with three partitions and replication factor one. Automatic topic creation is disabled so a mistyped topic name does not silently create application infrastructure.

Confluent Schema Registry 8.3.2 runs at `http://localhost:8081` and connects to Kafka through `kafka:19092`. It stores metadata in the compacted `_schemas` Kafka topic with local replication factor one. No separate registry volume exists: the Kafka data volume preserves registry state during normal shutdown and deliberately removes it during reset.

The broker uses a Docker-managed volume. `make infra-down` stops and removes the Compose containers and network while preserving that volume. `make infra-reset` deliberately removes the containers and volume; a later `make infra-up` creates fresh broker storage and reprovisions the topic.

## Why single-node

One combined broker/controller keeps local development and future deterministic integration tests small. It cannot demonstrate broker-level availability, failover, or production readiness. Replication factor one is appropriate only for this single-node local environment.

## Topic grain

`transactions.raw` receives validated deterministic sample events from the bounded producer. Avro is the value contract and UTF-8 `customer_id` is the key. The Spark ingestion application consumes this topic locally, preserves its Kafka coordinates, and can materialize validated records through the separately documented Delta sink.

## Partition count

Three partitions support future local experiments with ordering, routing, consumer parallelism, and skew. They are a development/test choice, not production sizing, throughput evidence, or a scalability claim.

## Local operation

Docker and Docker Compose are prerequisites for infrastructure commands.

```sh
make infra-up
make infra-status
make verify-infra
make infra-down
```

Normal shutdown preserves Kafka data. To intentionally discard local broker data:

```sh
make infra-reset
```

Run `make infra-up` afterward when a fresh broker is needed. Infrastructure startup is deliberately separate from `make verify`.

## Deferred decisions

The following remain undecided and unimplemented:

- retention sizing;
- production replication factor;
- security and TLS/SASL;
- multi-broker deployment;
- production consumer-group configuration;
- durable Kafka metadata representation;
- dead-letter routing;
- MinIO or Delta persistence.

The selected value and key contracts are described in the [transaction wire contract](transaction-wire-contract.md); they do not change this local broker topology.
