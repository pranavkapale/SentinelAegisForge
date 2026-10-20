# Commit-aware feature availability and local handoff

```text
Phase 10.1 statistical-feature Spark query
  → committed 25-column Delta version
  → version-by-version delta-rs snapshot validation/difference
  → immutable commit segments
  → immutable feature-ready outbox
  → bounded local research receiver
  → durable logical receipts
  ✕ no model inference or payment action
```

The existing Scala writer and `transaction_statistical_features_v2` schema are unchanged. The committed Delta table is authoritative. The Phase 24 outbox is a reproducible projection of it, not a second atomic sink. A crash after feature commit but before outbox publication is recovered by rescanning retained versions. A crash after outbox publication but before receipt is recovered by redelivery. Neither boundary proves globally exactly-once side effects.

## Discovery and identity

An explicit inclusive `[start_delta_version,end_delta_version]` is required. For each version, the reader validates the complete Phase 12 25-column schema and all rows, then compares event IDs and full row fingerprints with the predecessor version. Version zero uses an empty predecessor. A first appearance creates one `feature-availability-v1` envelope. An unchanged metadata-only version creates an empty segment. Deletes, same-ID content changes, duplicate IDs, table/schema/protocol changes and missing historical versions fail closed. No raw Parquet or manual Delta-log parsing is used. The current 64-version/5,000-row local bound and repeated full snapshot reads are correctness-first research limits, not a scalable CDC design. CDF was not enabled on the preserved Phase 14 table and is not enabled retroactively here.

Each envelope contains `ready_event_id`, `event_id`, source Delta table ID, first-visible version, source schema fingerprint, source full-row fingerprint and `availability_state=DELTA_COMMITTED`. `ready_event_id` is SHA-256 of a canonical JSON array containing the contract version, table ID, event ID, first-visible version and row fingerprint. Events sort by version then event ID. A segment records predecessor/current snapshot fingerprints, new/unchanged counts, source identities and JSONL byte hash. The outbox records ordered segment IDs, range, ready-event/source-ID fingerprints and artifact byte hash. Existing artifacts are inspected, never overwritten; incomplete publication is visible and refused.

`event_time` is business event time; `ingestion_time` is upstream transaction ingestion time; Spark `batch_id` is checkpoint-scoped processing progress; Delta version is a committed table-log version; first-visible version is the earliest retained verified version containing the complete row; outbox ordering is deterministic presentation. None is interchangeable. Phase 14 versions 0–2 are reconstructed historically (`HISTORICAL_COMMIT_RECONSTRUCTION`): they do not prove a Phase 24 consumer existed then. No exact `feature_ready_at`, commit-to-consumer latency or SLA is inferred.

## Local receiver

The default receiver only records verified notifications. It cannot invoke Phase 23 scoring, outcomes, Registry mutation, risk decisions or payments. The outbox and receipt roots are explicit. The receiver persists one canonical receipt per ready-event ID and separate operational attempt files (`ACCEPTED`, `DUPLICATE`, `FAILED`). Receipt lookup survives restart; an identical replay does not create a second logical receipt. A changed envelope reusing an ID fails. Processing is bounded by an explicit maximum per call. Progress derives from actual receipts and reports the last fully acknowledged segment, unique receipts, duplicate/failed attempts and outstanding events; a partial segment cannot be skipped. Full verification compares the exact outbox ready-ID set with durable receipts. The receiver's receipt is not proof that a future external consumer performed a side effect exactly once.

`make discover-feature-commits` and `make build-feature-ready-outbox` require an explicit Delta path and version range. `make inspect-feature-commit-segment`, `make inspect-feature-ready-outbox`, `make handoff-ready-events`, `make inspect-feature-handoff` and `make verify-feature-handoff` provide separate inspection/delivery operations. Handoff requires an explicit bounded maximum; it starts no Kafka, Spark, Docker, MLflow or model service. Generated artifacts remain under ignored `.local/` paths by default.

## Guarantees and limits

This phase proves only retained committed-row visibility, deterministic first-version attribution, reproducible identities, exact event-ID reconciliation, and local idempotent receipt handling. It does not prove the original wall-clock materialization time, historical online feature availability, production Kafka delivery, HA, global exactly-once delivery, production model eligibility or payment integration. Existing research-only model governance remains unchanged. See [ADR-038](../adr/ADR-038-commit-aware-feature-availability-outbox.md).
