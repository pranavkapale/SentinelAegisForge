# ADR-007: Schema Registry governance

## Status

Accepted

## Context

The first Kafka producer requires a registry-managed schema identifier and standardized Avro framing. The canonical schema already exists in the repository, so schema registration ownership and compatibility enforcement must be explicit rather than left to individual producer processes.

## Decision

Run `confluentinc/cp-schema-registry:8.3.2` alongside the existing Apache Kafka broker for local development. Register the canonical transaction schema under `transactions.raw-value` using the default topic-based `TopicNameStrategy`. Configure `BACKWARD_TRANSITIVE` compatibility on that subject so the newest future reader schema must remain able to read every previously registered version.

Provision the schema explicitly from `contracts/events/transaction-event-v1.avsc`. Producers use `auto.register.schemas=false` and perform an exact schema lookup. The UTF-8 string key is not Avro-serialized, so no `transactions.raw-key` subject exists.

Schema Registry stores governance metadata in its compacted `_schemas` Kafka topic. Its replication factor is one only because the local environment contains one broker. No separate registry data volume is added.

## Alternatives considered

- **Producer auto-registration:** reduces provisioning work but couples application startup to governance mutations and allows producer code to create schema versions unexpectedly.
- **Apicurio Registry:** provides open registry implementations and multiple API options, but would add a different serializer/runtime ecosystem from the selected Confluent Kafka Avro serializer.
- **No registry/raw Avro:** keeps infrastructure smaller, but leaves schema identifiers, standardized Kafka framing, and centrally enforced compatibility absent.

## Trade-offs

Explicit provisioning makes schema changes reviewable and repeatable, while adding a registry runtime and an availability dependency for producers. Module A adds the Confluent Maven repository and serializer client. The Kafka broker remains the official Apache image rather than a Confluent broker distribution.

Schema Registry and its client artifacts are Confluent-distributed components rather than Apache Kafka components. Deployments must review the applicable Confluent licensing and vendor-operational implications; this ADR does not enable other Confluent Platform services.

## Consequences

Local integration work requires Kafka and Schema Registry. Normal `infra-down` preserves registry state because the Kafka volume retains `_schemas`; `infra-reset` removes that history and the next `infra-up` reprovisions version 1. Registry availability becomes a producer dependency. Basic registry use does not change Module A's JDK 21 baseline.
