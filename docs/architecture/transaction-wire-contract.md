# Transaction wire contract

## Purpose

The transaction wire contract defines how a trusted `TransactionEvent` version 1 maps to an external Apache Avro value and how a future Kafka record is keyed. It provides local mapping, raw Avro binary round-trip, and compatibility evidence without implementing a Kafka producer, consumer, or Schema Registry.

```text
TransactionEventCandidate
        ↓ validation
TransactionEvent
        ↓ explicit mapping
Avro record
        ↓ future registry serializer
Kafka transactions.raw
```

The final two stages are future integration work: the repository currently maps and locally encodes Avro records but does not publish them.

## Canonical schema and mapping

`contracts/events/transaction-event-v1.avsc` is the one canonical schema. The Scala build exposes that repository file as a generated runtime resource; there is no second manually maintained schema copy. Apache Avro is used directly, without code generation or a generic serialization framework.

The mapper converts every validated domain field to its corresponding snake-case Avro field. Decode performs the inverse primitive mapping into an untrusted `TransactionEventCandidate` and invokes `TransactionEventValidator`. Malformed binary data, invalid Avro values, and domain-invalid decoded values return explicit codec errors instead of silently creating trusted domain state.

The local codec emits ordinary Avro binary data for tests. It does not add a magic byte, schema ID, or any other Schema Registry framing.

## Logical representations

- `event_id` is an Avro string with the `uuid` logical type and maps to `java.util.UUID`.
- `event_time` and `ingestion_time` are Avro `timestamp-micros` longs. They remain absolute instants. An instant with sub-microsecond precision is rejected at mapping time; it is never silently truncated.
- `amount` is Avro bytes with logical type `decimal`, precision 18, and scale 4. The domain validator accepts positive amounts only when they fit that representation. Trailing zeroes do not create a false scale violation, and values are never rounded or converted through floating point. The contract supports up to four fractional decimal places; it does not claim that every currency uses the same number of minor units.
- `transaction_type` is an Avro string. Version 1 domain validation currently accepts only `CARD_PAYMENT`. A string avoids making each new domain value an Avro enum-symbol compatibility event; the supported set remains explicit and validated.
- `schema_version` is the application-level event contract version and remains `1`. It is not a Kafka offset, Schema Registry schema ID, or Avro fingerprint.

## Compatibility approach

Tests use Apache Avro's reader/writer compatibility API. They demonstrate that a new optional field with a default can read version 1 data and that changing the required `customer_id` field to an incompatible type is rejected. These are test-only schema variants, not production v2 contracts.

The intended future Schema Registry policy is `BACKWARD_TRANSITIVE`, so new readers would be checked against all prior registered versions. This is planned guidance only; no registry exists and no infrastructure currently enforces it.

## Kafka key and ordering boundary

The future `transactions.raw` record key is `customer_id`, encoded as a UTF-8 string. It is not manually hashed, and no custom partitioner is selected. Under a stable partition topology, normal keyed partitioning provides customer locality and a useful per-customer ordering property. Kafka ordering remains partition-scoped; this is not global ordering.

Highly active customers and reused synthetic identities can create hot partitions. Future load tests must observe per-partition record rates, processing rates, lag, and state-size distribution before considering mitigations such as salting.

Increasing the topic's partition count can remap later records for a customer. Production expansion therefore requires an explicit plan for ordering, state migration, replay boundaries, and rollout. The current three partitions remain a local development choice.

## Deferred decisions

- Schema Registry runtime and final registry-backed Kafka framing;
- Kafka producer acknowledgements, retries, and idempotence;
- Kafka producer and consumer implementation;
- consumer groups and Kafka metadata representation;
- schema version 2 or later;
- dead-letter queue behavior;
- Spark ingestion, watermarks, and deduplication;
- Delta and MinIO persistence.
