# Kafka transport for committed-feature notifications

```text
Committed Phase 10.1 statistical Delta
  → verified Phase 24 outbox
  → immutable bounded Kafka publish batch
  → Scala Kafka publisher → durable acknowledgement journal
  → Kafka features.ready
  → isolated research consumer group
  → strict envelope/outbox-membership validation
  → durable logical receipt → manual per-partition offset commit
  → source/broker/receipt reconciliation
  ✕ no inference or payment enforcement
```

## Wire and topic contract

`feature-ready-kafka-wire-v1` uses topic `features.ready`, UTF-8 key `ready_event_id`, and an exact canonical UTF-8 JSON value with one trailing newline. The value is the Phase 24 `feature-availability-v1` envelope with exactly eight fields: `availability_contract_version`, `ready_event_id`, `event_id`, `source_delta_table_id`, `first_visible_delta_version`, `source_schema_fingerprint`, `source_feature_row_fingerprint`, and `availability_state=DELTA_COMMITTED`. Object keys sort lexicographically, separators are compact, Unicode remains unescaped, nonfinite numbers are forbidden, and the value is at most 4,096 bytes. Parsing rejects duplicate JSON keys, unknown/missing fields, invalid UUIDs/fingerprints, noncanonical bytes and mismatched key/ready-event identity. The ID is SHA-256 of canonical JSON (including newline) for `[availability_contract_version,source_delta_table_id,event_id,first_visible_delta_version,source_feature_row_fingerprint]`. Shared Python/Scala golden vectors guard byte parity. New fields or identity rules require a new wire/availability version rather than permissive v1 parsing.

The local official Kafka 4.3.1 topic has three partitions, replication factor one, explicit creation, auto-topic-creation disabled and `cleanup.policy=delete`. Publication checks the effective topic, cluster and topic ID. The ready-event key routes repeated identities consistently only while partition topology is stable. It gives no global order, and partition expansion can remap keys. Broker offsets are independent per partition and do not encode Delta commit order or logical event identity. Retention may eventually remove history; a full broker scan then fails rather than claiming reconciliation.

The record deliberately omits the 25 feature columns and all customer/card/device/IP, labels, risk/model output and payment data. Kafka/Compose uses localhost plaintext listeners only for local development, not production security. ACLs, TLS/SASL, governed retention and HA are deferred.

## Preparation, publication and recovery

`prepare-feature-kafka-batch` calls Phase 24 `inspect_outbox`, which revalidates retained Delta versions and immutable segment/outbox artifacts. It writes one deterministic, inspectable batch under ignored `.local/kafka/feature-ready-batches/`, bounded to 5,000 records. Its manifest fixes outbox/table/schema/version identities, exact record bytes and hashes; it is not broker evidence. Scala rechecks manifest identity, all record bytes, exact outbox bytes, record identities and size limits. SHA-256 is integrity checking, not authentication against a privileged adversary.

The Scala publisher uses byte serializers, `acks=all`, idempotence, five configured retries, compatible in-flight limit and a bounded delivery timeout. It stores broker coordinates per ready ID under `.local/kafka/feature-ready-publications/`, bound to cluster/topic ID and batch/outbox ID. A normal restart sends only IDs without durable acknowledgement. A crash after Kafka appended/acknowledged but before the local journal write can leave an unjournaled broker record; retry can append a duplicate. Explicit `--replay-acknowledged` makes duplicates intentionally for research. Neither producer idempotence nor the journal establishes cross-system exactly-once publication.

## Independent receiver and reconciliation

The stable `sentinel-feature-ready-research-v1` group disables auto-commit and uses `earliest` only when no group offsets exist. A bounded run fences each partition at its observed end offset. For each Kafka record: verify topic/key/value and exact membership in the prepared outbox, persist or verify one local receipt keyed by ready-event ID, record its physical/delivery observation, then synchronously commit the partition's next offset. The local receipt root binds the outbox, group, cluster and topic ID. Files are written to a flushed/synced temp file, exclusively linked into place and directory-synced on the local filesystem. This is a local single-writer durability assumption, not distributed consensus. An identical redelivery reuses the receipt; a conflicting value stops before offset advancement. A crash before receipt leaves the record eligible; a crash after receipt and before commit redelivers it and safely reuses the receipt. Offset holes, unexpected broker records, retention loss and incomplete bounded consumption fail rather than being silently skipped.

The reconciliation scan independently reads the declared retained topic range and checks that all source ready IDs have journalled acknowledgements and one receipt, every acknowledgement/first receipt coordinate exists in the broker, all broker records equal expected bytes, consumer observations cover the range, and group committed offsets reach the end fence. It reports physical records, duplicate records, unique receipts and partition distribution separately. Initial local verification uses an empty topic; retained unrelated records would require an explicit isolated range/topic before publication. An offset commit is research delivery progress, not model execution. The Phase 18 model remains production-ineligible; this path has no inference, Registry write, label input or payment side effect.

`make verify` remains Docker-independent. The explicit `verify-feature-ready-kafka-integration` target publishes and deliberately replays; use isolated journal/receipt roots and stable group IDs. Normal `infra-down` preserves Kafka data. Never run `infra-reset` as a test cleanup for this path. See [ADR-039](../adr/ADR-039-feature-ready-kafka-transport.md).
