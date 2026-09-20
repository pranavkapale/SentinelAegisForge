# ADR-003: Transaction event domain boundary

## Status

Accepted

## Context

External transaction data will eventually arrive through a transport boundary and may contain missing, malformed, or unsupported values. Transport choices have not yet been evaluated, and untrusted values must not silently become trusted business-domain state.

## Decision

Maintain separate representations for an untrusted `TransactionEventCandidate` and a validated, typed `TransactionEvent`. A pure validator converts candidates into either all detected typed validation errors or a trusted version 1 event. The validated event has no public constructor for arbitrary external strings and contains no transport metadata.

Later transport adapters will decode into the candidate representation. Only candidates accepted by the domain validator may enter downstream business processing.

This decision does not choose JSON, Avro, Protobuf, Schema Registry, Kafka partitioning, or any other transport mechanism.

## Alternatives considered

- Use one permissive case class for untrusted input and trusted domain state.
- Use transport-specific decoded classes directly as domain models.

## Trade-offs

The boundary adds explicit mapping and validation code. In return, it creates a stronger correctness boundary, keeps malformed data representable without exceptions, preserves transport independence, and makes validation behavior directly testable.

## Consequences

Future Kafka or serialization code must map decoded values into `TransactionEventCandidate` and invoke validation before downstream processing. New contract versions or transaction types require deliberate domain changes. Serialization format, compatibility policy, Kafka topology, partitioning, and failure routing remain separate decisions.
