# Stateful streaming to research shadow integration

Phase 28 exercises a fresh deterministic transaction plan through the existing Scala pipeline and the independent research-only handoff. The plan contains no label; the simulator's private truth sidecar is never opened by the integration harness or model worker.

```text
Scala scenario plan → existing Avro producer → Kafka transactions.raw
  → validated Spark ingestion → validated Delta
  → event-ID deduplication → deduplicated Delta
  → currency-safe rolling Spark state → rolling Delta
  → customer/currency Welford Spark state → statistical Delta
  → first-visible commit discovery → immutable ready segment/outbox
  → features.ready Kafka → isolated research group/receipts
  → pinned research-only model → durable shadow result
  → exact-set and provenance reconciliation
  X payment enforcement or model promotion
```

## Source and state boundaries

The transaction topic is shared. A broker-inspected cluster/topic ID and per-partition end-offset fence is captured before publication. A new validated `startingOffsets=latest` checkpoint is run before wave 0; Kafka acknowledgement coordinates must lie at or beyond the fence and match the validated Delta records exactly. This is source isolation through a checkpoint and offset boundary, not a private topic. No broker volume or historical data is reset.

Each of the four Delta stages has its own new output path, checkpoint and stable `txnAppId`. Wave invocations use the same paths, watermark settings and application IDs. The rolling stage retains the customer-wide five-minute prior count and same-currency ten-minute prior amount sum. The statistical stage retains customer/currency Welford state, prior-only scoring, Kafka-observed ordering and its event-time inactivity lifecycle. Both use their established RocksDB configuration. The ten-minute watermark is a local development policy, not a production lateness SLA. Source and output IDs are checked after every wave; a missing event or late drop is a failure until explained, not silently filled in.

## Committed feature handoff

Delta commit visibility precedes feature-ready discovery. The Phase 24 identity uses Delta table ID, first-visible version, event ID and row fingerprint. Phase 25 sends a model-independent ready envelope, never the 25 feature values, truth, risk decision or prediction. A Phase 28-specific group and receipt namespace leave Phase 27 offsets intact. The Phase 27 worker loads the unchanged research-only candidate and reads the pinned committed feature row; training-period events become `OUT_OF_SCOPE`, and eligible events may be scored. A full pass requires exact source-to-ready-to-result membership and independent reference probability comparison. A selected event trace supplements this full-set check.

Delta and Kafka are not one atomic transaction. If publication fails after a feature commit, retained Delta versions and the immutable outbox support recovery without recalculating that feature. If Kafka redelivers, receipts and logical result claims guard the local research workflow. Its accepted-unprocessed bound may pause further research publication, while Scala feature commits and transaction publication remain independent. An inference pause cannot block payment processing because this repository has no payment enforcement path.

## Running and interpreting the experiment

Use the new `capture-phase28-transaction-fence`, `initialize-stateful-shadow-integration`, `prime-stateful-shadow-ingestion`, `run-stateful-shadow-wave`, `reconcile-stateful-shadow-wave` and `verify-stateful-shadow-integration` targets with explicit ignored `.local/` paths. Generate and inspect the Phase 14 corpus first. Run waves 0, 1 and 2 in order, with a commit-aware handoff after each. The harness invokes the existing Make targets for actual Scala work and writes per-stage logs, monotonic invocation durations and read-only Delta wave reports. Phase 27's existing discover/publish/accept/process/reconcile commands perform the downstream work. Ordinary `make verify` never starts Docker.

Durations use Python `time.perf_counter` around each bounded invocation and are not event-to-decision latency. Spark progress supplies micro-batch, watermark and state metrics where logged by the existing applications. Synthetic event/ingestion timestamps are semantic UTC data, not host processing-clock measurements. The local single-broker experiment does not demonstrate high availability, production throughput, calibrated fraud quality or globally exactly-once business processing.
