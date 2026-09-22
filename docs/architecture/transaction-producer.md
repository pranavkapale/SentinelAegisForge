# Registry-backed transaction producer

## Current flow

```text
deterministic generator
        ↓
TransactionEventCandidate
        ↓ validation
TransactionEvent
        ↓ explicit Avro mapping
GenericRecord
        ↓ KafkaAvroSerializer
Schema Registry framing + schema ID
        ↓ UTF-8 customer_id key
Kafka transactions.raw
```

The bounded producer publishes only generated candidates that pass domain validation and Avro mapping. It does not use the Phase 3 raw binary codec as input to `KafkaAvroSerializer`.

## Registry governance

- Local endpoint: `http://localhost:8081`.
- Subject: `transactions.raw-value` using `TopicNameStrategy`.
- Canonical source: `contracts/events/transaction-event-v1.avsc`.
- Subject compatibility: `BACKWARD_TRANSITIVE`.
- Registration owner: the infrastructure provisioning script, not the producer.
- Producer setting: `auto.register.schemas=false`.

The producer supplies the canonical `GenericRecord` schema for exact registered-schema lookup. It does not request the latest schema, create a key subject, implement registry framing, or maintain schema IDs in application code.

Registry metadata lives in Kafka's `_schemas` topic. Normal shutdown preserves the Kafka volume and therefore the subject, version, and compatibility configuration. Resetting the Kafka volume removes registry history; the next infrastructure start provisions the schema again.

## Record and delivery semantics

Every record targets `transactions.raw`, uses the exact `event.customerId` as its UTF-8 key, and leaves the partition unset so Kafka performs normal keyed selection. Acknowledgement configuration is `acks=all`; producer idempotence is enabled. Every send result is observed, and any mapping or delivery failure makes the bounded run fail visibly.

Producer idempotence protects against duplicates caused by supported retries within a producer session. It does not prevent duplicate business events from simulator reruns, replay, producer restarts and republishing, or downstream materialization. No exactly-once business guarantee is claimed.

Kafka transactions are intentionally absent because there is no atomic multi-topic or consume-transform-produce requirement. No custom retry loop, partitioner, hashing, or performance tuning is present.

## Local operation

Start and verify infrastructure:

```sh
make infra-up
make verify-infra
```

Publish a deterministic bounded sample:

```sh
make produce-sample \
  COUNT=10 \
  SEED=42 \
  BASE_TIME=2026-09-21T00:00:00Z
```

`BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL` may also be overridden. The command verifies existing infrastructure but does not start it implicitly.

## Current non-goals

- Kafka consumer or Spark ingestion;
- invalid-event or dead-letter production;
- business-event deduplication;
- Kafka transactions;
- validated, decision, or alert topics;
- performance benchmarks or tuning;
- Delta or MinIO persistence;
- fraud rules or model behavior.
