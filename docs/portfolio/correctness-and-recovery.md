# Correctness and recovery boundaries

[Portfolio home](README.md) · [recorded event](architecture-walkthrough.md#one-recorded-event) · [evidence index](evidence-index.md)

## Stateful Spark correctness

**Time has several meanings.** `event_time` is the business occurrence, `ingestion_time` is the supplied observation chronology used by the temporal ML split, Kafka partition/offset records transport order, and Delta version records committed visibility. None is interchangeable with host wall-clock execution time. The [wire contract](../architecture/transaction-wire-contract.md) preserves absolute microsecond timestamps; unsupported sub-microsecond precision is rejected rather than silently rounded.

**Deduplication is bounded.** The [transformer](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/deduplication/TransactionDeduplicationTransformer.scala) applies `withWatermark("event_time", delay)` before `dropDuplicatesWithinWatermark("event_id")`. The first accepted occurrence retains its Kafka metadata; `transactions_validated` remains the transport-grain audit history. The local ten-minute delay is a development policy, not a measured lateness SLA. Records older than the active watermark can be dropped; progress exposes `numRowsDroppedByWatermark`. Out-of-order does not automatically mean too late. There is no late-event side output, lifetime uniqueness or payload-conflict analysis. [Tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/deduplication), [ADR-012/013](../adr/ADR-013-event-time-watermark-semantics.md).

**Rolling features are prior event-time history.** The [rolling processor](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/rollingfeatures/RollingFeatureProcessor.scala) keeps customer-keyed events with time, exact amount and currency. For current time `t`, count uses `[t-5m,t)` across currencies; amount sum uses `[t-10m,t)` and current-currency equality. Equal-time peers all read the previous history before any peer is inserted. An already observed future-event-time row cannot influence an earlier-time row. Eligible late arrivals do not rewrite old outputs.

Concrete existing [processor test](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/rollingfeatures/RollingFeatureProcessorSpec.scala), `velocity includes every currency while amount membership matches the current currency`:

```text
12:00 USD 100
12:01 EUR 200
12:02 USD 300 → prior count 2, prior USD sum 100.0000
12:03 EUR 400 → prior count 3, prior EUR sum 200.0000
```

The same suite protects exact inclusive lower bounds, microseconds, equal-time peers, future-state exclusion, stale timers and checked decimal overflow. The current amount remains `decimal(18,4)`; the rolling sum is `decimal(38,4)`. There is no FX conversion. Phase 8's older cross-currency cumulative amount is diagnostic only and is excluded from monetary decisions and model inputs.

**Welford uses prior observed history, not the rolling event-time window.** The [statistical processor](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/CustomerAmountStatisticsProcessor.scala) groups by `CustomerCurrencyKey(customerId, currency)`. It sorts each invocation by Kafka partition/offset, requires the same partition and advancing offsets in an active pair's state, scores from prior count/mean/M2, then updates. Gaps due to other currencies are legitimate. An already observed 12:10 row may influence a subsequently observed 12:05 row; that is intentional online observed-order semantics, not a claim that all features use only earlier event times.

For an observation `x`, the existing update is `delta=x-mean`, `mean'=mean+delta/(n+1)`, `M2'=M2+delta*(x-mean')`. Prior sample variance is `M2/(n-1)`. Money stays exact in storage; these statistics use finite checked Double approximations. Only tiny negative variance within `1e-12` is clamped as numerical roundoff; materially negative/nonfinite values fail.

| Prior state | Output status | z-score |
|---|---|---|
| Count 0, no mean/stddev | `NO_HISTORY` | Absent |
| Count 1, mean but no stddev | `INSUFFICIENT_VARIANCE_HISTORY` | Absent |
| Count ≥2, stddev ≤ `1e-12` | `ZERO_VARIANCE` | Absent |
| Count ≥2, finite mean/stddev above floor | `READY` | `(Double(amount)-prior_mean)/prior_stddev` |

[Processor tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/CustomerAmountStatisticsProcessorSpec.scala), [restart tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/StatisticalFeatureStreamingSpec.scala), and the [Python source gate](../../model-control-plane/src/sentinelaegisforge_control_plane/datasets/dataset_contract.py) protect this contract, including positive prior means and tolerance-based z-score consistency without repairing source data.

**State lifecycle matters.** Rolling MapState schedules its earliest ten-minute expiry; a watermark jump removes all due entries. Welford has constant-sized ValueState per active customer/currency and an independently replaceable inactivity timer at maximum observed event time + configured duration (24 hours locally). Stale timer callbacks cannot erase newer state. Timer expiry clears the pair; a later eligible observation starts `NO_HISTORY`. This is event-time progression, not wall-clock TTL. State-variable TTL is disabled.

Spark 4.2 requires RocksDB for these streaming `transformWithState` applications; it was not selected from a performance benchmark. Dedup retains its existing default store; stateless advisory rules need none. Checkpoints retain source progress, watermark/state/timers and sink coordination. Runtime, key, watermark, state schema or topology changes need compatibility review; fresh checkpoints require fresh sink app IDs. [ADR-017](../adr/ADR-017-rolling-feature-state-retention.md), [ADR-019](../adr/ADR-019-customer-statistical-state-lifecycle.md), [currency refinement ADR-020](../adr/ADR-020-currency-safe-monetary-feature-semantics.md). Bounded time retention does not establish a fixed memory bound for arbitrary active-key cardinality.

## Four cross-system failure windows

### A. Delta commits, publication fails

The statistical table remains authoritative. [Discovery](../../model-control-plane/src/sentinelaegisforge_control_plane/availability/discovery.py) validates append-only retained versions; the [outbox](../../model-control-plane/src/sentinelaegisforge_control_plane/availability/outbox.py) reconstructs deterministic first-visible ready identities. Retry publication from that outbox rather than recomputing features. Required Delta files/history must still exist. This is not a distributed Delta/Kafka transaction.

**Evidence:** [availability tests](../../model-control-plane/tests/test_availability.py) reject missing history/mutation; [reliability tests](../../model-control-plane/tests/test_reliability.py) inspect real failure evidence. Phase 29 D stopped the broker after feature commit, recorded publication failure, restarted the retained broker and completed the same handoff; [recorded result](../../PROJECT_STATE.md#actual-failures-and-recovery). A timeout alone does not prove the broker accepted nothing.

### B. Kafka accepts, publisher crashes before journaling

The [publisher](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyKafkaPublisher.scala) observes broker acknowledgements before persisting a local per-ready-ID journal. A crash in that gap can leave acceptance uncertain and produce another physical Kafka record on retry. Producer-session idempotence cannot atomically commit a local journal. The receiver deduplicates the **logical ready ID**, while retaining physical-coordinate evidence.

**Evidence:** [transport tests](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyTransportSpec.scala), `uncertain broker acknowledgement is retried; replay is explicit`; [ADR-039](../adr/ADR-039-feature-ready-kafka-transport.md); [Phase 25 live transport record](../../PROJECT_STATE.md#phase-25-kafka-feature-ready-transport-verification). The contract permits physical duplicates even where a particular experiment observed zero.

### C. Receipt is durable, offset commit has not happened

The [consumer](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyKafkaConsumer.scala) validates and [persists a receipt](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyReceiptStore.scala) before synchronously committing partition offset + 1. Redelivery verifies the existing receipt, rejects conflicting identity reuse, and advances safely. Committed offsets are not a backup of lost receipts.

**Evidence:** the same [transport suite](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyTransportSpec.scala) explicitly tests `manual next-offset commit follows durable receipt, never precedes it` and `before-receipt crash does not commit; after-receipt crash deduplicates on restart`. These are a bounded single-member local receiver contract, not distributed consumer HA.

### D. Inference returns, durable result is not yet claimed

The [worker](../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py) scores before claiming its logical result. If interrupted between them, scoring can repeat. A deterministic request ID and exclusive result claim preserve an existing identical result and reject conflicts. There is no payment/network side effect associated with invoking the model, and no exactly-once physical invocation guarantee.

**Evidence:** [Phase 26 result tests](../../model-control-plane/tests/test_kafka_shadow.py) test identical/conflicting claims and restart after a durable result; [incremental tests](../../model-control-plane/tests/test_incremental_shadow.py) exercise interruption after a persisted result, reuse and backlog drain. The pre-claim repeat possibility follows from code order; do not relabel these after-result tests as a separately executed kill at every machine instruction. Phase 29 D demonstrates paused-worker recovery, not every conceivable inference crash.

All four boundaries assume retained local authority and cooperating writers. Hashes detect inconsistency, not hostile coherent replacement. File publication is not uniform across artifact families: Phase 24 lacks explicit multi-file fsync, and Phase 27 result publication flushes the file without explicitly forcing its parent directory. [The durability assessment](../assessments/phase30/failure-mode-register.md#file-publication-is-not-one-uniform-durability-contract) distinguishes process-restart evidence from untested host power-loss durability.

## Real statistical post-commit recovery

Phase 29 **Experiment C** exercises the actual statistical query, not merely a mock append:

1. Three source waves contain ten events each. Statistical batch **0** commits **10 rows** to Delta version **0**.
2. The opt-in [post-commit hook](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/persistence/delta/DeltaTransactionSink.scala) throws only after the write returns. [Statistical application](../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/TransactionStatisticalFeatureApp.scala) wires that hook into its own sink.
3. Recovery removes the fault option, retains checkpoint `checkpoints/statistical` under integration `fecf2fd85c3b6e34bde851ad9165717e8f5ff70cc754fc6c22d19ac3efbdb34f`, and retains `txnAppId=phase28-statistical-fecf2fd85c3b6e34bde8`.
4. Repeating `txnVersion=0` produces no second logical append. Before/after restart remain ten rows at version 0, with identical row digest `1b9f6aa052b24db84a15fd31bedd40666f9acf3e8415910f8c970fb1257b8876`.
5. Subsequent waves demonstrate useful state restoration: the [traced EUR event](architecture-walkthrough.md#one-recorded-event) sees prior count **2**, mean **190.01999999999998**, stddev **81.65669109142249**. Independent reference values are **190.02** and **81.6566910914225**. The preceding watermark was `2030-01-04T21:26:00Z`, before that key's expiry `2030-01-05T00:00:15Z`.
6. Final statistical version **2** has **30 unique rows**; all four stage ID sets, notifications, receipts and results reconcile. All **30** scored probabilities match independent inference.

Three proofs are separate: **checkpoint restoration** retains processing/state history; **source replay** may re-execute an unfinished batch; **Delta transaction identity** suppresses its already-committed output. Equal row counts alone cannot prove state survived. [Real Spark restart test](../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/StatisticalFeatureStreamingSpec.scala), [fault/recovery orchestration](../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/operations.py), [independent state checks](../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/state_examples.py), [ADR-043](../adr/ADR-043-stateful-streaming-reliability-and-capacity.md).

C demonstrates retained state. Separately A demonstrates expiry/reactivation: `(customer-0000, EUR)` has expiry `2030-01-02T00:00:15Z`, observed wave-zero watermark `2030-01-03T23:41:21.600000Z`, and a later row with count **0** / `NO_HISTORY`. The evidence combines that key's output with **52** aggregate removals; aggregate removals alone do not identify which key expired. [Recorded state evidence](evidence-index.md#measured-results-and-provenance).

## Isolated restoration

| Scenario | What it asks | Demonstrated status |
|---|---|---|
| Process restart | Can the process resume while original storage survives? | Phase 29 C/D: same state/transaction lineage and retained broker recovery |
| Isolated restore | Can a verified bundle restore authorities in a separate runtime and resume pending work? | Phase 31 Docker simulation: **`VERIFIED_RESTORED`** |
| Independent-media disaster recovery | Can recovery succeed when the original physical storage failure domain is unavailable? | **`PARTIALLY_VERIFIED`**; backup was same-host, same-disk |

Phase 31 quiesced cooperating writers, archived the full offline Kafka volume, captured exact dependency files and used SQLite's backup API. **23 dependency nodes, 5,006 local files and 424 Kafka archive entries** covered selected source/model histories and pending work. Fresh Docker targets preserved the original absolute-path namespace; only restored data, read-only backup and new evidence were mounted. The runtime had no source `.local`, source broker volume or Docker socket. Offline preflight checked bytes before broker startup.

The restored KRaft/topic IDs and application offsets, registered schema ID/version/compatibility, Delta IDs/history, checkpoint/RocksDB files, transaction app IDs, MLflow/Registry identity and model-tree hash were preserved. Two inspected internal Registry NOOP records were separately explained; application offsets were not silently reset. No source replay into brand-new identities was substituted for restore.

The cutoff left **20** validated/deduplicated/rolling rows, **10** statistical rows, **10** accepted notifications and **5** durable results. Resumption completed **30** events through every stage/result, with **30** probability comparisons and **zero mismatches** (maximum absolute difference 0.0, tolerance `1e-12`). Original source bytes and prior durable results were preserved. Restored EUR prior-state values and watermark cleanup were inspected, not inferred from final counts.

The directory `backup_isolated_storage/` is not offsite or independent media. Docker isolation establishes namespace/runtime separation, not survival of source-disk loss. No HA, cloud portability, RPO/RTO or production readiness follows. [Restore procedure](../architecture/recovery-bundle-and-restore.md), [dependency closure](../../model-control-plane/src/sentinelaegisforge_control_plane/recovery/lineage.py), [runtime](../../model-control-plane/src/sentinelaegisforge_control_plane/recovery/runtime.py), [43 recovery tests](../../model-control-plane/tests/test_recovery.py), [ADR-044](../adr/ADR-044-consistent-recovery-bundle-and-isolated-restore.md), [recorded Phase 31 closure](../../PROJECT_STATE.md#phase-31--consistent-recovery-bundle-and-docker-restore-closure).
