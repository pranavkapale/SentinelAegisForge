# ADR-005: Transaction event serialization

## Status

Accepted

## Context

`transactions.raw` needs a durable value contract before the first producer or consumer is implemented. The internal `TransactionEvent` already defines validated domain semantics, but a transport-independent external schema is needed for compact binary encoding, explicit evolution checks, and eventual registry governance.

## Decision

Use Apache Avro for transaction event values. The canonical version 1 schema is `contracts/events/transaction-event-v1.avsc`; Module A maps explicitly between the validated domain event and an Avro generic record. The local codec writes and reads standard Avro binary data solely to verify schema and mapping behavior. It does not implement Kafka framing, registry schema IDs, or Confluent magic bytes.

Use Avro logical types for UUID, absolute microsecond timestamps, and `decimal(18,4)` amounts. Sub-microsecond instants and amounts outside the declared decimal representation are rejected rather than silently truncated or rounded. Represent `transaction_type` as a string whose allowed values remain governed by domain validation. This avoids coupling every additive transaction-type value to an Avro enum symbol change.

ADR-007 subsequently introduces Schema Registry and enforces `BACKWARD_TRANSITIVE` on the transaction value subject. Compatibility behavior remains covered locally with Apache Avro's compatibility API as well.

## Alternatives considered

- **Protobuf:** provides strong generated types, explicit field numbering, and an excellent cross-language RPC and messaging ecosystem. It would add code generation and generated message classes to a repository whose immediate direction is data-stream and lakehouse oriented.
- **JSON:** is human-readable, easy to inspect, and requires little tooling. It generally produces larger payloads, and strong schema enforcement and evolution governance require an additional deliberate mechanism. JSON can be governed by schemas, but that machinery is not inherent in a plain JSON payload.

## Trade-offs

Avro provides an explicit schema, compact binary representation, mature evolution rules, and a strong Kafka/data-platform ecosystem fit. The wire data is not human-readable, schema tooling is required, and logical types demand disciplined conversion and precision handling. Direct use of Apache Avro keeps the boundary visible but requires more mapping code than a generated-code integration.

## Consequences

The repository has one canonical transaction-event schema and a tested domain-to-wire boundary. Future producers must apply the same mapping rather than inventing a second payload. Schema changes require compatibility tests and contract review.

ADR-007 and ADR-008 subsequently establish registry-backed Kafka serialization and producer delivery behavior. No fake production v2 schema is created by this decision.
