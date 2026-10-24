# Incremental local shadow operations

Phase 27 extends, but does not rewrite, the completed Phase 24–26 local research contracts:

```text
New statistical-feature Delta commits
    → contiguous incremental discovery (including empty commits)
    → immutable per-version segments, outboxes and Kafka batches
    → bounded sequential publication on features.ready
    → fenced operational group and durable receipts
    → accepted batch queue
    → bounded trusted-candidate inference
    → stream-level durable logical results
    → exact-set reconciliation and observed local timing
    X payment enforcement, production serving and promotion
```

## Identity and isolation

The operational contract is `incremental-shadow-operations-v1`. Its stream ID binds the source Delta table ID and exact 25-column schema fingerprint, first version, Kafka cluster/topic IDs, stable operational group, candidate/model-tree identity and logic version. `.local/phase27/` holds the isolated fixture and stream. Phase 14 statistical Delta, Phase 24 outbox, Phase 25 receipts, Phase 26 inbox/results, Registry and the historical Phase 19 ledger are untouched. Runtime observations never enter the stream ID.

The existing topic `features.ready` is reused without a wire change. Initialization durably records cluster ID, topic ID, partition count implied by the verified three-partition topic, beginning and end offsets for every partition, then commits a new `sentinel-feature-ready-incremental-shadow-v1` group to the recorded end fence. Older research records are excluded explicitly. Thereafter the group uses its stable committed offsets. Each accepted batch records its exact per-partition start/end range; the next batch must begin at that prior end. An unrelated record in the range fails batch membership, rather than being silently skipped. Retention crossing the required start or a recreated topic fails closed. Receipt persistence precedes each synchronous next-offset commit.

## Version and result progress

`versions/<n>.json` is created only after the version's segment and, for nonempty versions, outbox and batch are durably prepared and reinspected. Version `n+1` cannot be discovered before `n`. Empty metadata-only commits get a segment and record but no Kafka batch. A restart rebuilds progress from source-verified segments, publication journals, receipt roots, acceptance boundaries and result files. Source rows are resolved only from their pinned first-visible Delta versions; an unavailable version blocks scoring rather than falling back to the newest snapshot.

One active publication batch is permitted. An acknowledged batch is not inference-complete; an accepted notification remains pending until a terminal result file exists. The logical request hash binds ready-event ID, candidate ID, model-tree hash and `incremental-shadow-result-v1`, not Kafka offset or packaging. Replayed physical delivery may be observed more than once, but a conflicting logical result cannot overwrite the first durable local claim. Inference loads the exact research-only Registry candidate once per bounded worker tick; restart can load it again. The separate offline scorer compares newly generated fixture probabilities by event ID and pinned row fingerprint. The historical Phase 19 ledger is not extended with invented observations.

## Backpressure and limits

The local defaults bound undiscovered versions (8), prepared events (500), accepted/unprocessed events (200 unless explicitly lowered), one active publication batch, per-tick inference (50 unless lowered) and cached source rows (5,000). The controlled live experiment used accepted limit 20 and tick limit 10. Before publication, the next batch must fit within accepted inference capacity; otherwise it remains a prepared immutable outbox and publication pauses. Worker ticks drain pending results and make capacity available again. These are memory/recovery guards, not throughput targets. New upstream Delta commits may continue while the research path is paused: this is **not** transaction-ingestion or payment-processing admission control.

The read-only inspector separates discovery-version backlog, prepared/unaccepted transport backlog and accepted/unprocessed inference backlog. `IDLE`, `PROCESSING` and `PAUSED_BACKPRESSURE` are derived from durable progress; source/transport/model failures are fail-closed command errors. They are not production incident severities. A missing or corrupt artifact cannot be treated as a completed stage.

## Local commands

After `make infra-up` and `make provision-feature-ready-topic`, initialize a fresh fixture with `make initialize-incremental-fixture`, then `make initialize-incremental-shadow`. Use the returned stream ID as `INCREMENTAL_STREAM_PATH`. For each finite wave, call `make append-incremental-fixture`, `make discover-incremental-features`, `make publish-incremental-shadow-batch`, `make accept-incremental-shadow-batch`, then `make process-incremental-shadow-work`. Supply the batch ID/path returned by discovery; for each batch after the first, set `INCREMENTAL_PRECEDING_BOUNDARY` to the previous batch's acceptance JSON. `make inspect-incremental-shadow` is read-only. Once all waves are complete, `make verify-incremental-shadow` performs exact-set reconciliation and a separate offline-scoring comparison; `make report-incremental-shadow` writes a content-addressed local report. These operator-triggered commands never join ordinary Docker-independent `make verify`.

An intentional model interruption can use `SHADOW_FAIL_AFTER_RESULT`; an intentional receipt-before-offset interruption can use `FEATURE_KAFKA_FAIL_AFTER_RECEIPT_AT`. Reinvoke the same operation and lineage after clearing the fault. `make infra-down` retains Kafka's volume; it does not reset old research data. Do not reset the shared broker for this experiment.

## Timing and limits of evidence

Monotonic elapsed times cover the local Delta write call, discovery/outbox preparation call, Kafka publisher call, Kafka acceptance call, model loading and inference worker tick where observed. UTC observation time, if present, is operational metadata. Cross-process wall clocks are not assumed synchronized, and synthetic `ingestion_time` is never used as an observed delivery latency. A four-batch local sample does not support a p99, production throughput, HA or latency-SLO claim. The fixture is schema-valid but bypasses the real Spark feature processor; it cannot establish original feature materialization time or feature-state correctness. Phase 24–26 and Phase 14 evidence remain separate.
