# Technical walkthrough and architecture defense

[Portfolio home](README.md) · [evidence index](evidence-index.md) · [current architecture](../../README.md#architecture)

Use this as a presentation guide, not a script claiming deployment experience the repository does not establish. Distinguish source/test evidence, recorded local execution and open production questions.

## Three-minute overview

1. **First minute — purpose and system:** “SentinelAegisForge demonstrates trustworthy streaming features and auditable research inference. Scala handles Kafka/Avro, Spark state and Delta; Python handles immutable ML provenance, independent inference and monitoring.” Show the [root diagram](../../README.md#architecture). Both the advisory risk branch and research model are non-enforcing.
2. **Second minute — strongest contribution:** show [the real statistical commit/restart](correctness-and-recovery.md#real-statistical-post-commit-recovery): ten rows committed, then failure, then the same checkpoint/app ID produces thirty unique rows with expected prior EUR statistics. Explain why retry suppression alone would not prove state restoration.
3. **Final minute — measured evidence and limits:** [3,560 reconciled events / 2,393 matched predictions](evidence-index.md#measured-results-and-provenance), then [30 restored events](correctness-and-recovery.md#isolated-restoration). Say “same-disk isolated Docker restore, not offsite disaster recovery.” End with research-only model status, not a fraud-accuracy pitch.

## Ten-minute technical walkthrough

- **Minutes 0–2:** [one recorded event](architecture-walkthrough.md#one-recorded-event), from deterministic plan to transaction acknowledgement and validated audit row. Explain business identity versus transport identity.
- **Minutes 2–4:** [rolling and Welford semantics](correctness-and-recovery.md#stateful-spark-correctness). Contrast prior event-time windows with prior observed order, and customer-wide velocity with customer/currency monetary history.
- **Minutes 4–6:** first-visible Delta version → [outbox](../../model-control-plane/src/sentinelaegisforge_control_plane/availability/outbox.py) → feature-ready acknowledgement → receipt. Show why inspected latest version 2 differs from this event's first-visible version 1.
- **Minutes 6–8:** [worker](../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py) → exact candidate/source verification → request identity → durable result. Discuss physical replay and independent parity.
- **Minutes 8–10:** [four failure windows](correctness-and-recovery.md#four-cross-system-failure-windows), then [governance](architecture-walkthrough.md#model-lifecycle-and-governance). No global exactly-once or production fraud-effectiveness claim.

## Twenty-minute architecture defense

| Time | Evidence to show | Question to defend |
|---|---|---|
| 0–4 min | [Rolling processor/test example](correctness-and-recovery.md#stateful-spark-correctness) | Which history is available, which clocks order it, and which currencies can be compared? |
| 4–8 min | [Real Spark recovery test](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/StatisticalFeatureStreamingSpec.scala), [Experiment C](correctness-and-recovery.md#real-statistical-post-commit-recovery) | What does each of checkpoint state, source replay and Delta txn identity prove? |
| 8–12 min | [Transport crash tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyTransportSpec.scala), [worker restart tests](../../model-control-plane/tests/test_incremental_shadow.py) | What happens in each non-atomic delivery gap? |
| 12–15 min | [Candidate loader](../../model-control-plane/src/sentinelaegisforge_control_plane/candidates/run.py), [ten validation gates](../assessments/phase30/capability-matrix.md#model-readiness--all-ten-phase-18-requirements) | Why do verified bytes and prediction parity not imply deployment approval? |
| 15–18 min | [Restore boundary](correctness-and-recovery.md#isolated-restoration), [recovery tests](../../model-control-plane/tests/test_recovery.py) | What survived an isolated restore, and what remains unproven after physical disk loss? |
| 18–20 min | [Security review](../assessments/phase30/architecture-threat-model.md#security-control-review), [bounded measurements](../assessments/phase30/capability-matrix.md#capacity-evidence--local-bounded-observations) | What would you secure, measure or change before a different operating scope? |

## Questions a Staff Engineer may ask

### 1. Why Spark Structured Streaming?

The implemented need is checkpointed Kafka ingestion, event-time state and durable Delta micro-batches in Scala. Spark provides those APIs and lets temporary local tests exercise the real state/recovery behavior. The repository demonstrates that fit; it does not benchmark Spark against every alternative. [ADR-009](../adr/ADR-009-spark-structured-streaming-ingestion.md), [stateful restart tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/StatisticalFeatureStreamingSpec.scala).

### 2. What is event time versus ingestion time here?

Event time is the business timestamp used for windows/watermarks. Ingestion time is explicit transaction observation chronology used by the ML temporal split. Kafka coordinates provide observed transport order; first-visible Delta version provides committed feature visibility. A historical ingestion timestamp cannot prove when the completed feature was available. [ADR-025](../adr/ADR-025-temporal-ml-evaluation.md), [ADR-038](../adr/ADR-038-commit-aware-feature-availability-outbox.md).

### 3. How does the watermark affect correctness?

It bounds retained dedup/state history and determines eligibility/cleanup. A unique older-than-watermark record can be dropped even though domain-valid. Eligible out-of-order records may pass. Ten minutes is a controlled local policy; it does not imply lifetime uniqueness, recoverable late routing or a production SLA. [Late-drop tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/deduplication), [ADR-013](../adr/ADR-013-event-time-watermark-semantics.md).

### 4. Why not key all state by customer and currency?

Velocity is intentionally customer-wide. Rolling state stays customer-keyed and filters monetary membership by the current currency; Welford uses one ValueState per customer/currency. Kafka remains customer-keyed, so currency switches are not partition changes. [Currency correction](../adr/ADR-020-currency-safe-monetary-feature-semantics.md), [mixed-currency tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/rollingfeatures/RollingFeatureProcessorSpec.scala).

### 5. How is Welford state restored, and how do you know it was useful?

The same compatible checkpoint restores RocksDB state, timers and progress. Experiment C's later EUR event retained count 2 and independently checked mean/stddev after a real post-commit fault. Equal output counts would be insufficient; those state-dependent values are the stronger evidence. [Experiment C](correctness-and-recovery.md#real-statistical-post-commit-recovery).

### 6. What happens after Delta commit but before checkpoint completion?

Spark may replay the unfinished batch. The sink reuses its stable app ID and batch ID, suppressing the already-committed Delta append. State recovery and future output are checked separately. Never republish the successful source wave or erase its checkpoint as a recovery shortcut. [Statistical sink](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/persistence/delta/TransactionStatisticalFeatureDeltaSink.scala), [recovery test](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/StatisticalFeatureStreamingSpec.scala).

### 7. Why both txnAppId and txnVersion?

The application ID identifies the sink/checkpoint lineage; batch ID becomes its transaction version. A new checkpoint resets batch numbering and therefore needs a new app ID. Reusing the old pair could suppress legitimate new writes. This protection is table-local, not a transaction across Kafka, four Delta tables and Python. [ADR-011](../adr/ADR-011-delta-streaming-idempotency.md).

### 8. Why is Kafka delivery not globally exactly once?

The producer/journal and receipt/offset are separate commits, and Delta/ML results are outside Kafka's transaction boundary. Broker acceptance can be uncertain after a crash. Producer idempotence protects supported retries, not arbitrary process restarts or business-event lifetime uniqueness. RF 1 also provides no replica failover. [ADR-008](../adr/ADR-008-kafka-producer-delivery-semantics.md), [ADR-039](../adr/ADR-039-feature-ready-kafka-transport.md).

### 9. How are publication duplicates handled?

The same ready ID binds the same first-visible source row; receipts recognize identical redelivery and reject conflicting payload reuse. Physical offsets/attempts remain separately visible. Request/result identity then protects logical inference output. This depends on retained authority and the local ownership contract. [Transport tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyTransportSpec.scala).

### 10. Why persist receipts before committing offsets?

Committing first could acknowledge work with no durable acceptance record. Writing the receipt first makes the crash gap replayable: redelivery checks the existing receipt before advancing offset + 1. Lost receipt storage is a separate recovery failure; group offsets alone cannot reconstruct it. [Consumer](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyKafkaConsumer.scala), [failure C](correctness-and-recovery.md#c-receipt-is-durable-offset-commit-has-not-happened).

### 11. What if the model process crashes?

After a result claim, restart verifies/reuses it. Before the claim, physical inference may repeat. Source/candidate/request identities and exclusive claims prevent accepting a conflicting logical result in the supported namespace. There is no payment effect to repeat and no exactly-once invocation promise. [Worker](../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py), [restart test](../../model-control-plane/tests/test_kafka_shadow.py).

### 12. How is model-source identity verified?

The chain binds fixed Delta version, snapshot, training run, candidate/model-tree hash and exact numeric Registry version. The loader checks bytes/runtime/trusted types; the worker checks the first-visible row fingerprint and feature allowlist. Archived/reference probability parity is independently checked. SHA-256 proves consistency, not authorization of a malicious writer. [Loader](../../model-control-plane/src/sentinelaegisforge_control_plane/candidates/run.py), [Registry](../../model-control-plane/src/sentinelaegisforge_control_plane/registry/run.py).

### 13. Why is the model not production eligible?

Synthetic provenance/reconstruction satisfy only VAL001–003. Representative real labels, temporal generalization, discrimination, calibration, segments, deployed operations and business authority remain NOT_ESTABLISHED. Research execution does not amend the assessment. `REGISTERED_RESEARCH_ONLY`, `production_eligible=false`, `NOT_ELIGIBLE_FOR_PROMOTION` remain explicit. [All ten criteria](../assessments/phase30/capability-matrix.md#model-readiness--all-ten-phase-18-requirements).

### 14. What does Phase 31 restore prove?

A quiesced selected bundle restored original identities into a separate Docker namespace and resumed pending work: 30 events and 30 matching probabilities. It differs from merely restarting a process. Its same-disk backup does not prove recovery after physical source-disk loss, so independent-media status remains PARTIALLY_VERIFIED. [Restore evidence](correctness-and-recovery.md#isolated-restoration), [ADR-044](../adr/ADR-044-consistent-recovery-bundle-and-isolated-restore.md).

### 15. What is missing before cloud operation?

A defined operating scope and accountable owners, authenticated/authorized/encrypted service boundaries, durable independent storage and tested restore, retention ownership, checkpoint/runtime migration strategy, supervised execution, release/rollback evidence and operational objectives. A cloud VM or Kubernetes alone supplies none of those end-to-end guarantees. Model approval remains a separate gate. [Threat model](../assessments/phase30/architecture-threat-model.md), [conditional roadmap](../assessments/phase30/prioritized-roadmap.md).

### 16. What would you optimize first, based on evidence?

First measure, without tuning the retained lineage. Stateful invocations were longest in the local three-wave samples, but each launched a new process and B changed several population variables. A separately authorized experiment could isolate startup/restore versus trigger work, then test one configuration variable with fresh checkpoints and exact-value reconciliation. Existing numbers do not justify a production throughput claim or arbitrary RocksDB/GC/shuffle changes. [Measured capacity and limitations](../assessments/phase30/capability-matrix.md#capacity-evidence--local-bounded-observations), [bounded experiment proposals](../assessments/phase30/prioritized-roadmap.md#bounded-experiments-worth-considering).

### 17. Can all of this be reproduced from a clone?

Code and ordinary tests can; a new bounded integration needs Docker and explicit fresh lineage setup. Reproducing historical IDs, Registry versions, predictions and restore evidence needs retained compatible artifacts/paths. Those are three different reproducibility levels. Missing `.local` files are not solved by manufacturing a replacement with the same name. [Reproduction guide](README.md#reproducibility-levels).

### 18. What makes the labels independent?

The simulator's private outcome sidecar is separate from canonical Kafka events and feature state. Delayed labels join later by event ID; neither CLEAR/REVIEW nor model predictions generate truth. Provider-style feedback fixtures test revision mechanics, not authentic adjudication. This prevents circular labels but does not establish representative real-world fraud outcomes. [ADR-027](../adr/ADR-027-deterministic-synthetic-fraud-corpus.md), [ADR-036](../adr/ADR-036-durable-outcome-feedback-ingestion.md).
