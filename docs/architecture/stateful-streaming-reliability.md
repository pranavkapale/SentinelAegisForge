# Stateful streaming reliability experiments

## Scope

Phase 29 reuses the Phase 28 real transaction and research path. Its four fresh lineages are A (1,500-event baseline), B (changed bounded workload), C (statistical post-commit fault) and D (downstream interruption). The observed run parameters and outcomes belong in `PROJECT_STATE.md` and the ignored `.local/phase29/` dossier. An incomplete gate is reported as incomplete.

```text
Scala deterministic transaction plan → Kafka + Schema Registry
  → validated Delta → event-ID deduplication
  → customer rolling state/checkpoint
  → customer-currency Welford state/RocksDB checkpoint
  → transactionally appended statistical Delta
       ↳ optional fault after commit → same-checkpoint restart
  → committed-version discovery/outbox → features.ready Kafka
  → durable research receipts → bounded research inference
  → exact-set reconciliation → local reliability evidence
  → production approval/payment action: not implemented
```

## Fault and recovery boundaries

The statistical fault flag selects one nonnegative micro-batch ID. It is absent on ordinary runs and on recovery. The hook executes only after the Delta append returns. The experiment records the failed attempt's log, output table ID/version/rows, checkpoint and transaction application ID before restarting. A restart uses the same lineage and then checks that the committed event IDs and values were not rewritten. Later events must retain the correct prior customer-currency statistics. `txnAppId` plus `txnVersion` protects a Delta batch retry; it does not by itself prove Spark state recovery or cross-system atomicity. Batch ID and Delta version are different identifiers.

The Phase 29 compatibility record is claimed before statistical processing. It binds Spark/Scala and semantic versions, RocksDB provider, watermark, inactivity timeout, source table ID, target path, checkpoint path and transaction application ID. The target table ID is bound after first commit. A mismatch requires a new reviewed lineage; checkpoint files are never edited. Timer-driven expiry/reactivation needs output and state-progress evidence and must not be inferred merely from a long source-event-time gap.

## Measurement contract

The collector reads actual Spark `StreamingQueryProgress` JSON from bounded stage logs, Phase 28 exact-set reconciliation, Delta table metadata and rows, Kafka partition coordinates, checkpoint and Delta file inventories, and `perf_counter` invocation timings. It records available batch IDs, input rows, durations, rates, watermarks and state-operator metrics without filling absent fields. Process invocation duration and Spark-reported trigger execution are distinct measurements. Their difference includes setup, teardown and logging as well as startup; it is not a precise startup measurement. Every invocation starts a new JVM/Spark process. Wave zero initializes new state, while later waves restore a checkpoint; later waves are not automatically warm processes. Partition counts and customer-currency key counts describe the observed synthetic plan, not a general skew or capacity law. Three wave samples retain their raw values, sample count, minimum, median and maximum; they do not support precise tail percentiles.

Kafka retention, Delta history, Spark checkpoint/RocksDB files, transaction application identity, outbox/journal/receipt artifacts and model bytes are recovery prerequisites. A missing required historical offset or Delta version must fail closed; seeking latest or reading the latest Delta snapshot would change the result. A retained outbox can be republished with separate physical Kafka evidence. Research backpressure limits publication and inference only; feature commits continue independently.

The local dossier hashes declared artifacts and excludes latent synthetic truth, labels and payment decisions. Evidence from four controlled runs may characterize bottlenecks directionally, but does not prove production throughput, steady-state latency, availability, state capacity, fraud quality or an end-to-end exactly-once property. No automatic RocksDB/Kafka tuning or continuously running benchmark service is introduced.

## Local operations

Initialize each corpus, transaction fence, integration and research group with the existing Phase 28 targets. Supply absolute paths because module commands execute from their module directories. Normal unit verification does not start infrastructure.

```sh
make run-reliability-wave PHASE29_INTEGRATION_PATH=/absolute/integration PHASE29_WAVE=0
make inject-statistical-postcommit-failure \
  PHASE29_INTEGRATION_PATH=/absolute/fault-integration PHASE29_WAVE=0 PHASE29_FAULT_BATCH_ID=0
make verify-statistical-postcommit-recovery PHASE29_INTEGRATION_PATH=/absolute/fault-integration
make inspect-streaming-state PHASE29_INTEGRATION_PATH=/absolute/integration
make inspect-source-retention \
  PHASE29_INTEGRATION_PATH=/absolute/integration PHASE29_CURRENT_FENCE=/absolute/current-fence.json
make inspect-reliability-experiment \
  PHASE29_INTEGRATION_PATH=/absolute/integration PHASE29_STREAM_PATH=/absolute/research-stream \
  PHASE29_OBSERVATION_OUTPUT=/absolute/A.json
make compare-reliability-experiments \
  PHASE29_BASELINE_OBSERVATION=/absolute/A.json PHASE29_CHANGED_OBSERVATION=/absolute/B.json
make verify-reliability-unit
```

The observation output option exports canonical comparison input. Human-readable terminal JSON is display output. A repeated export verifies identical bytes and refuses changed evidence. The fault target executes and recovers wave zero itself; do not publish that wave again. A successful source wave cannot be rerun through the publication target. Existing attempt logs are preserved and refuse overwrite. Inspect an unexpected failed attempt before running a separately named recovery operation.

For D, discover an outbox from an actual committed statistical version, stop infrastructure without deleting volumes, and capture a nonzero publication attempt. Record it with `record-reliability-broker-failure` before restarting infrastructure and recovering the same outbox. A timeout means delivery is uncertain, not proof that nothing reached Kafka. Pause inference after durable acceptance, then use the existing handoff's explicit backpressure demonstration on the next wave. `verify-reliability-outage` requires the original failed log hash, later acceptance, recorded pause/refusal, and complete final results. The dossier builder revalidates these artifacts rather than trusting a manually supplied VERIFIED label.

The final `build-reliability-dossier` target requires all four integration/stream paths, the outage record, the retained candidate, a current transaction fence and `PHASE29_READY_OFFSETS_LOG` from the existing read-only `feature-kafka-offsets` command. The latter now exports structured topic identity and beginning/end offsets; every research group's original required range is checked against it. `inspect-reliability-dossier` checks its exact file inventory, hashes and semantic identity without rerunning Spark or inference. Reinspection never updates measurements. Directory sizes are sums of logical file lengths, not allocated filesystem blocks; peak RSS, CPU utilization and unexported heap settings remain unavailable.

`record-reliability-backlog` records the controlled `recovered-published`, `research-paused` and `final` boundaries. `record-reliability-backpressure` invokes the real authorization gate and records its refusal before inference drains. These observations separate discovery versions, unacknowledged ready events, acknowledged-but-unaccepted ready events and accepted-but-unprocessed ready events. Their sampled maxima are not continuous peak telemetry.

## Event-time interpretation

The retained-state example requires independently calculated prior count, mean and sample standard deviation. Its expiry deadline must still be ahead of the watermark observed at the end of the prior wave. A subsequent event may itself have an event timestamp beyond that deadline: the authoritative timer fires according to the query's watermark progression, not merely the next row's event-time gap. Expiry/reactivation requires an eligible deadline, observed timer/state removals and a later per-key NO_HISTORY/count-zero output. Aggregate removals do not identify a key on their own; they are checked together with that key's output history.

## Runtime corrections exposed by verification

The handoff drains at the smaller of its existing 500-item tick cap and the research stream's configured inference limit. Previously it always requested 500, which the worker correctly rejected for a ten-item stream. This changes the operator's request size, not inference, feature or model semantics.

The pinned Arrow 25 local Python reader could finish a Delta read and then deadlock during interpreter finalization: a sampled main thread waited for Arrow's thread pool while a worker waited to release a Python filesystem reference under the GIL. The local feature contract now passes a native Arrow `SubTreeFileSystem` rooted at the pinned Delta table instead of the default Python filesystem adapter. Delta still selects the exact version and files; the 25-column validation and exact monetary values are unchanged. A bounded subprocess regression checks clean shutdown. This follows the native-filesystem support in [delta-rs documentation](https://delta-io.github.io/delta-rs/latest/api/delta_table/) and the matching [Arrow shutdown issue](https://github.com/apache/arrow/issues/50482). No dependency was upgraded.
