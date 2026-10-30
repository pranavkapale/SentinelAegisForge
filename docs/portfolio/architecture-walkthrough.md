# Architecture walkthrough

[Portfolio home](README.md) · [correctness and recovery](correctness-and-recovery.md) · [evidence index](evidence-index.md)

## One system, separate responsibilities

The canonical path is sequential: validated audit history → event-ID deduplication → rolling features → statistical features. Deduplication is not a parallel replacement for stateful features. Each stage reads a durable predecessor and owns a separate checkpoint/output transaction lineage.

| Boundary | Grain and authority | Important distinction |
|---|---|---|
| Candidate → trusted event → Avro | One validated business event; [canonical schema](../../contracts/events/transaction-event-v1.avsc) | `event_id` identifies an occurrence; `transaction_id` can identify a transaction with several legitimate events. No label or transport coordinate belongs to this domain object. |
| Kafka `transactions.raw` | A physical record; UTF-8 `customer_id` key | Ordering is partition-scoped. Partition expansion may remap keys. Producer acknowledgements are transport evidence, not lifetime event uniqueness. |
| `transactions_validated` | Every successfully validated Kafka record, including transport duplicates | Registry-aware decoding reuses domain validation and verifies key/customer agreement; lineage is retained. |
| `transactions_deduplicated` | Accepted event IDs within the watermark semantics | First accepted occurrence retains its Kafka metadata. This does not delete audit rows. |
| `transaction_customer_features_v2` | One accepted transaction plus prior rolling features | Five-minute count across currencies; ten-minute monetary sum only in the current currency. |
| `transaction_statistical_features_v2` | The authoritative 25-column feature boundary | Prior-observed Welford state per customer/currency; exact decimal source money plus approximate Double statistics. |
| Delta commit → ready outbox | One logical notification per first-visible event/row/version | Discovery validates retained versions; no fabricated historical wall-clock readiness timestamp. |
| Kafka `features.ready` → research receipts | Physical delivery attempts and durable logical acceptance | Key is `ready_event_id`, not `customer_id`. Envelope contains source identities/fingerprints, not full features, labels or scores. |
| Independent research worker → result | One logical request for a ready event and frozen candidate/model identity | Reads the pinned feature row, scores only eligible post-training scope, and persists terminal results. Physical invocation can repeat. |

The [advisory deterministic risk branch](../architecture/deterministic-risk-engine.md) consumes the same corrected statistical boundary and writes only CLEAR/REVIEW with policy identity and reasons. It never supplies ground truth or commands the research model. The research worker can pause without stopping upstream feature commits; there is no payment system to block. [ADR-041](../adr/ADR-041-incremental-shadow-operations-and-backpressure.md) scopes backpressure to downstream research publication.

Model training/catalog governance is a separate lifecycle. Backup/restore captures dependencies across these authorities; it is not another transformation in the transaction stream.

## One recorded event

This is a **derived, minimal excerpt from retained Phase 29 Experiment C evidence**, reinspected read-only on 2026-10-31. It is an actual synthetic event, not a new demonstration ID, a full transaction history or a cryptographically authenticated record. No private truth or labels were read by the event tracer. The [evidence index](evidence-index.md#measured-results-and-provenance) identifies the containing dossier and its limitations.

Event `737d2bdb-7a4a-43e7-92bf-ba3cb29e9b52` belongs to wave 1, synthetic `customer-0000`, currency EUR. Its semantic event time is `2030-01-05T00:00:00Z`; ingestion time is `2030-01-05T00:00:02Z`. These future-dated fixture times are not the host execution clock or measured latency.

| Step | Observed trace | Code to inspect |
|---|---|---|
| 1. Canonical generation | Present in corpus `1d4dcbd986ae84a4a89bbbb5b3a01454ae7b7cbe3bee6858ef994b0a5e00afee`, wave 1 | [Scenario generator](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/simulation), [domain validator](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/domain/transaction/TransactionEventValidator.scala) |
| 2. Transaction acknowledgement | `transactions.raw`, partition **0**, offset **1691** | [Existing publisher](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/producer/TransactionEventPublisher.scala) |
| 3. Validated Delta | Exactly one matching row at inspected version **2**; table `bf9f2bf1-a6bf-4590-9b03-9a2fd173ad1e`; same Kafka coordinate | [Ingestion](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/persistence/delta/DeltaTransactionIngestionApp.scala) |
| 4. Deduplicated Delta | Exactly one matching row at version **2**; table `c11ec2f5-c8a2-47d5-997b-152684b41388` | [Dedup transformer](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/deduplication/TransactionDeduplicationTransformer.scala) |
| 5. Rolling Delta | Version **2**, table `a6d76b89-4ff3-4923-a6c8-0b07eaea2820`; prior count **0**, prior EUR sum **0.0000** | [Rolling processor](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/rollingfeatures/RollingFeatureProcessor.scala) |
| 6. Statistical Delta | Version **2**, table `ba915a98-bc6c-45b9-bf70-395e306456a5`; prior EUR count **2**, mean **190.01999999999998**, stddev **81.65669109142249**, z-score **-1.6489769325730625**, `READY` | [Statistical processor](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/CustomerAmountStatisticsProcessor.scala) |
| 7. First-visible commit | Statistical Delta version **1**, distinct from the latest inspected version 2 | [Discovery](../../model-control-plane/src/sentinelaegisforge_control_plane/availability/discovery.py) |
| 8. Ready identity | `c3790967630e6231e86f3c303e92814bcc10653997ea4573251edceb5ffdb014` | [Outbox](../../model-control-plane/src/sentinelaegisforge_control_plane/availability/outbox.py) |
| 9. Notification acknowledgement | `features.ready`, partition **1**, offset **3056** | [Ready publisher](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyKafkaPublisher.scala) |
| 10. Durable receipt | `8c9e03cdc4706877a3577f9de43bf946b9952d3090921628a679a637a8c75084`, same ready ID and first notification coordinate | [Receipt store](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyReceiptStore.scala) |
| 11. Research request | `b8160d0cdb7438f1a71e1c1b7dad3703614520fc55f0b362061648744e456e86` | [Worker](../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py) |
| 12. Durable prediction | `SCORED`, FRAUD-class probability estimate **0.6408900485135742**, contract `incremental-shadow-result-v1` | [Result verification](../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/progress.py) |

The ready ID binds the table, first-visible version, event ID and row fingerprint. The request additionally binds candidate/model and result-contract identity. None of these replaces Kafka coordinates. The prediction is an uncalibrated synthetic research estimate, **not** a risk disposition or approval.

The retained statistical count is not inconsistent with the zero rolling count: the rolling horizon has elapsed, while the EUR statistical inactivity timer has not fired at the preceding watermark. [The state example](correctness-and-recovery.md#real-statistical-post-commit-recovery) explains the observed timer and independent reference values.

### Reinspect this trace

Requires Level 3 artifacts at their original compatible paths, plus the locked Python environment; no broker or model execution is needed. The command reads retained data and prints the trace. It does not regenerate absent evidence.

```sh
make trace-stateful-shadow-event \
  PHASE28_INTEGRATION_PATH="$PWD/.local/phase29/integrations/fecf2fd85c3b6e34bde851ad9165717e8f5ff70cc754fc6c22d19ac3efbdb34f" \
  PHASE28_STREAM_PATH="$PWD/.local/phase29/streams-configured/eba2f04581b9219c943504ff6c4a22f625e7331c6412880cc019008300865653" \
  PHASE28_EVENT_ID=737d2bdb-7a4a-43e7-92bf-ba3cb29e9b52
```

The underlying [trace function](../../model-control-plane/src/sentinelaegisforge_control_plane/stateful_shadow_integration/reconciliation.py) checks plan membership, one row per stage, source acknowledgement agreement, one first-visible notification and one durable result. The full experiment verifier additionally reconciles **all** IDs and independent probabilities. A single successful trace cannot prove whole-population completeness. Fresh-clone readers can inspect this code and [tests](../../model-control-plane/tests/test_stateful_shadow_integration.py); the ignored local files are not repository assets.

## Model lifecycle and governance

1. **Dataset provenance:** a fixed corrected Delta table/version and independent delayed outcomes produce an immutable Phase 12 snapshot. Exact decimals and all audit columns survive; only an explicit 11-feature projection enters the estimator. IDs, private latent profiles, labels and rule dispositions are excluded. [Dataset contract](../../model-control-plane/src/sentinelaegisforge_control_plane/datasets/dataset_contract.py).
2. **Temporal fit:** ingestion-time cohorts are explicit. Only training rows whose selected labels were available by `train_end` enter fitting; preprocessing is fitted there too. Validation/test remain evaluation-only. Independent label maturation is not retroactively inferred from model predictions. [Temporal split](../../model-control-plane/src/sentinelaegisforge_control_plane/training/temporal.py), [baseline](../architecture/offline-ml-baseline.md).
3. **Run and candidate identity:** Phase 13 immutable manifests/metrics/predictions are authoritative; MLflow indexes them. The frozen candidate reconstructs the same Pipeline and must match archived probabilities before publication. skops loading requires matching runtime, file-tree hashes and the static trusted-type policy. This is trusted-local loading, not a malicious-model sandbox. [Candidate loader](../../model-control-plane/src/sentinelaegisforge_control_plane/candidates/run.py).
4. **Registry governance:** canonical training-run ID, candidate ID, MLflow run ID and numeric Registry version are distinct. Registration references original candidate files; it does not back them up. The actual version remains `REGISTERED_RESEARCH_ONLY`, `production_eligible=false`. [Registry](../../model-control-plane/src/sentinelaegisforge_control_plane/registry/run.py).
5. **Validation:** `NOT_ELIGIBLE_FOR_PROMOTION`. VAL001–003 verify artifact integrity, reconstruction and provenance. VAL004–010 do **not** establish real-world labels, representative temporal generalization, discrimination, calibration, segment robustness, deployed operational validation or business approval. Later research shadow/restore evidence does not rewrite the Phase 18 assessment. [All ten gates](../assessments/phase30/capability-matrix.md#model-readiness--all-ten-phase-18-requirements), [policy](../../model-control-plane/src/sentinelaegisforge_control_plane/validation/policy.py).
6. **Research observation:** the separate Phase 19 offline prediction ledger (not an automatic consumer of incremental shadow results) supports delayed-outcome evaluation and label-free distribution measurements. Pending/retracted/unlabeled outcomes are not converted to LEGIT; provider-style fixtures are not authenticated external truth. Drift measurements and investigation packets are evidence, not automatic retraining or promotion triggers. [Feedback](../architecture/durable-outcome-feedback.md), [monitoring](../architecture/offline-distribution-monitoring.md).

Synthetic validation/test Average Precision was **0.11443784507721583 / 0.08116962182591815**; ROC AUC was **0.45287237702098376 / 0.4655847436221269**. These unchanged [recorded evaluation results](../assessments/phase30/executive-assessment.md#production-and-fraud-decision-gates) are integration evidence, not production fraud performance. Correct provenance and successful recovery do not make the model effective.
