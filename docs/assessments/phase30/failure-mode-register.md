# Failure modes, durability and recovery boundaries

Read with the [executive evidence definitions](executive-assessment.md#evidence-discipline). **T** = IMPLEMENTED_AND_TESTED, **L** = IMPLEMENTED_LOCALLY, **D** = DESIGNED_NOT_IMPLEMENTED, **N** = NOT_ESTABLISHED, **X** = NOT_APPLICABLE_TO_CURRENT_SCOPE. Source links establish mechanisms; [Phase 29 evidence][evidence] establishes only the retained local executions. Recommendations are proposals, not repairs performed in this assessment.

## Delivery semantics at each boundary

| Boundary | Established contract | Limit and failure window |
|---|---|---|
| Transaction producer → Kafka | **T/L:** validated Avro, customer key, `acks=all`, producer idempotence and acknowledgement accounting [producer][producer-config] | RF 1 has no replica failover. A timeout can leave delivery uncertain; restarting a whole business publication is not covered by producer-session retry idempotence. No lifetime event uniqueness. |
| Kafka → Spark source offsets | **T/L:** checkpointed source progress; `failOnDataLoss=true` [source][source] | Depends on original topic identity and retained offsets. A fresh checkpoint or altered starting offsets is a different input lineage, not recovery. |
| Spark state/checkpoint | **T/L:** source/batch progress, watermark, state and sink coordination survive the demonstrated same-lineage restart [recovery][recovery] | Requires compatible operator/schema/runtime and intact files. Checkpoint presence is not proof a different host can restore it. |
| Spark batch → one Delta table | **T/L:** `txnAppId` + `txnVersion=batchId` suppress repeated append for the same transaction identity [sink][sink] | **Exactly-once logical Delta writes within the established retained-lineage contract**, not an atomic transaction over Kafka, all four tables and Python. New checkpoint needs a new app ID; an intentional fresh replay can add rows. Do not expire required transaction evidence without review. |
| Business deduplication | **T/L:** `event_id` plus event-time watermark state [dedup][dedup] | Distinct from Delta retry suppression. First accepted occurrence wins; no conflicting-payload detection, lifetime uniqueness or late-event side output. Validated audit rows remain untouched. |
| Delta → outbox | **T/L:** deterministic first-visible version/row identities, append-only checks and immutable local manifests [outbox][outbox] | No distributed transaction with Delta. Reconstruction needs exact retained history. A partially claimed directory fails closed; it is not silently repaired. |
| Outbox → publication journal | **T/L:** journal written after every acknowledged Kafka send; existing entries verified [publisher][publisher] | Broker acceptance can precede local journal persistence. Recovery can repeat a physical message; no atomic Kafka/journal commit. |
| Kafka → consumer receipt | **T/L:** bounded expected batch, key/identity/bytes checks, local receipt before commit [consumer][consumer], [receipt][receipt] | Single-member, three-partition contract. Receipt storage is local; no multi-host uniqueness authority. Foreign messages or gaps fail rather than being skipped. |
| Receipt → Kafka offset commit | **T/L:** commit offset + 1 after receipt; retry verifies existing receipt [consumer][consumer] | Crash between receipt and commit allows replay. Advancing offsets does not replicate the receipt or protect against its later loss. |
| Accepted notification → inference | **T/L:** source/candidate verification; score only eligible windows [worker][worker] | Physical model invocation can repeat before a result is durably claimed. No remote side effects, exactly-once invocation or process-isolated cancellation guarantee. |
| Inference → logical result | **T/L:** deterministic request identity, exclusive claim, byte/content conflict detection [incremental results][worker], [Phase 26 results][results] | Local persistent logical deduplication while claims remain available. This is neither a distributed unique constraint nor a host/storage-failure guarantee. |

No cross-system atomic transaction joins these boundaries. The research publisher's backlog gate never throttles upstream transaction production or Delta feature commits. [ADR-039][adr39], [ADR-040][adr40], [ADR-041][adr41].

## Failure-mode register

Priorities refer to the [roadmap](prioritized-roadmap.md): **P0** is conditional on real data/external access/customer impact; **P1** is valuable local reliability work. Detection below means the current inspection or failure path, not an always-on alert. Recovery proposals have not been executed unless explicitly marked L.

| Component / failure | Existing safeguard and evidence | Missing safeguard / impact | Detection | Recovery method and recommended action |
|---|---|---|---|---|
| F01 Kafka broker process stops | **L:** D failed publication then recovered with retained volume; [outage][outage], [Compose][compose] | **N:** broker redundancy; ingestion/publication unavailable during outage | CLI health/admin failure, send timeout, retained failure log | Restart same broker/volume and reconcile actual acknowledgements/receipts. **P1 W1:** prove restoration separately; timeout alone does not prove nondelivery. |
| F02 Kafka volume/KRaft metadata lost | **L:** normal shutdown preserves volume [Compose][compose] | **N:** independent backup/restore; RF-1 logs, topic IDs, offsets and `_schemas` may be lost together | Topic/cluster fences or offset-range checks fail [retention][retention] | Stop dependent processing; restore a consistent tested broker bundle or declare original lineage unrecoverable. **P1 W1**; do not recreate names and pretend offsets survived. |
| F03 Schema Registry unavailable/lost | **T:** schema lookup errors surface; explicit compatibility provisioning [decoder][decoder], [provision][provision] | **N:** redundant service/store restore; new decoding/registration can fail, cached schemas are not universal availability | Registry health, producer/decoder failure | Restart against retained `_schemas`; if lost, restore identities with broker backup and verify old wire schema IDs. **P1 W1**, not arbitrary re-registration. |
| F04 Required Kafka offsets expired | **T:** source `failOnDataLoss`, explicit range/topic fences [source][source], [retention][retention] | **N:** governed retention/admission; required transport history unrecoverable locally | Fresh broker beginning/end offsets versus required ranges | Fail closed; recover original data from approved backup or authorize a new lineage. Never silently seek latest. **P1 W3** retention dependency report. |
| F05 Spark checkpoint corrupt/missing | **T:** retained checkpoint checks; **L:** intact-checkpoint restart [guard][guard], [state evidence][state-json] | **N:** checkpoint backup restoration/new-host proof; stateful prior features may change on a fresh checkpoint | Presence checks then Spark restore; presence alone is insufficient | Preserve failed bytes; recover compatible checkpoint with source/output bundle, or explicitly rebuild into isolated new lineage. **P1 W1**. No manual edits. |
| F06 Stateful config/runtime incompatible | **T:** Phase 29 compatibility manifest rejects mismatches [guard][guard], [tests][tests] | Guard is harness-scoped; **N:** universal command enforcement or migrated-state compatibility | Guard mismatch; Spark restore/type errors | Require reviewed new lineage or a separately tested migration. **P1 W2/W3** ownership/preflight; no automatic upgrade. |
| F07 Delta historical files/log unavailable | **T:** pinned Python source reads and retention gates fail [reader][reader], [retention][retention] | **N:** backup/retention coordination; pending notifications, training provenance and replay can lose their source | Read exact required version/files, not merely latest metadata | Restore the required log+files or fail the lineage. **P1 W1/W3**; do not substitute latest snapshot. Direct Spark streaming behavior on expired logs also needs explicit testing. |
| F08 Statistical failure after Delta commit | **L:** actual C batch 0 failed after 10 rows committed; same-checkpoint/app-ID restart retained row identities/hash and subsequent prior state [recovery][recovery], [state evidence][state-json] | **N:** coverage of every crash boundary, hardware failure or engine upgrade | Failed attempt log + committed table version + checkpoint/app identity | Demonstrated recovery removes fault option and resumes original lineage; do not republish successful source wave. Preserve this test as W1's logical-state acceptance check. |
| F09 Outbox construction interrupted | **T:** exclusive directory and strict inventory reject partial output [publication][publication], [outbox tests][outbox-tests] | **N:** atomic multi-file publication/power-loss durability; incomplete claim blocks progress | `exact_directory`, hashes and source reinspection | Preserve partial evidence; reviewed reconstruction from retained versions in an isolated destination, not blind overwrite. **P1 W2** explicit interrupted-claim procedure. |
| F10 Kafka ack/journal uncertainty | **T:** each ack journal entry verified; replay tests [publisher][publisher], [transport tests][transport-tests] | No transaction between broker and journal; crash can duplicate physical delivery | Compare durable journal, broker coordinates and receipts; absence of journal is not absence of delivery | Explicit replay using same logical ready IDs; receipt/result dedup protects logical processing. **P1 W2** document exact recovery windows. |
| F11 Receipt corrupt/lost after commit | **T:** canonical bytes, immutable exclusive files, sequence/fence checks [receipt][receipt], [consumer][consumer] | **N:** receipt backup; consumer group alone cannot reconstruct accepted provenance | Inspector/reconciliation errors or accepted-range gap | Stop; restore receipts with journals/fences. Replay only if original ranges remain and a reviewed procedure preserves identity. **P1 W1/W3**; no skip-and-continue. |
| F12 Model bytes corrupt/missing | **T:** exact tree/hash/runtime/type checks before trusted load [candidate][candidate], [candidate tests][candidate-tests] | **N:** independent model backup/authenticated source; inference unavailable | Candidate inspection/load fails; no successful result should be claimed | Restore exact verified candidate bytes and provenance; never choose latest/other model silently. **P1 W1**, **P0 W6** for hostile replacement. |
| F13 MLflow SQLite backend lost/contended | **T:** cooperating local `flock`, canonical read-back [Registry][registry] | **N:** database backup/restore and foreign-writer coordination; provider run/version IDs and governance index may be lost | SQLite errors, metadata conflicts, duplicate identities | Restore consistent SQLite plus artifacts and receipts; reconstructing a new index is not preservation of original IDs. **P1 W1/W2**. |
| F14 Feedback case/revision conflicts | **T:** conflicts/missing predecessors/unknown events rejected; no PENDING-as-LEGIT [resolver][feedback], [feedback tests][feedback-tests] | **N:** source authentication/adjudication; plausible false outcomes can pass shape checks | Contract rejection or cross-source conflict report, not truth detection | Keep disputed records; require authorized resolution policy and complete revision history. **P0 W6/W7** before real-label claims. |
| F15 Research worker outage or partial result | **T/L:** bounded ticks, restart skips verified logical results; D accepted backlog drained [worker][worker], [tests][worker-tests] | **N:** supervisor, cross-host coordination, model deadline; upstream can continue growing | Separate accepted/unprocessed counts and failed tick | Resume same candidate/request lineage after verifying retained sources; physical scoring may repeat. **P1 W3**, conditional W5; no enforcement dependency. |
| F16 Local disk exhaustion/host loss | **T:** writes/errors and incomplete claims fail rather than being accepted as success [publication][publication], [files][files] | **N:** free-space alert, durable remote copy, quota/retention plan; many authorities share one disk | OS write error; current collectors report logical bytes after runs, not continuous free space | Stop writers, preserve manifests, recover capacity/consistent bundle; never VACUUM/delete dependencies ad hoc. **P1 W1/W3**. |
| F17 Unexpected input/key topology | **T:** statistical stream enforces partition and increasing offset within customer-currency state [statistics][statistics]; strict topic/group fences | **N:** partition-expansion/state migration and multi-member research group | Fail-closed partition/offset mismatch, subscription ownership checks | Freeze topology for retained lineage; design explicit replay/state migration before expansion. **P1 W3**, deployment-dependent. |
| F18 Privileged local writer changes artifacts or governance | **T:** independent comparisons catch accidental incoherence [inspector][inspector], [Registry][registry] | **N:** authority separation/signatures; coherent replacement of content+hashes defeats local integrity checks | May be undetected by current validators | Protect writers, audited authorization and independent trusted copies. **P0 W6** for real data/external users; hashes alone are not a recovery method. |

## Storage dependencies and disaster recovery

**L: local persistence; N: disaster restoration unless stated otherwise.** The Docker volume and host `.local` paths are not independent failure domains merely because they are separate directories. Docker Desktop's storage is still hosted on the same machine. [Compose][compose], [Makefile][make].

| Persistent authority | Present dependency | What is and is not established |
|---|---|---|
| Kafka log + topic/KRaft metadata | One combined broker/controller and `kafka-data` volume | Normal down/up and process outage recovery passed. Volume-loss restoration preserving cluster/topic identities and offsets did not. |
| Schema Registry metadata | `_schemas` on that broker, replication factor 1 | Container recreation can reuse retained schemas. Kafka volume loss also removes this authority; compatible `.avsc` files alone do not prove restoration of registered schema IDs. |
| Spark checkpoints + RocksDB | Four distinct local lineage paths; rolling/statistical state uses RocksDB | Stateful same-files restart demonstrated. Default provider is retained for dedup. No copied-checkpoint clean-machine restoration or Spark-upgrade compatibility evidence. |
| Delta transaction logs + data | Path-based local tables, all required versions/files | ACID appends and retry identities tested. Table metadata without Parquet files is insufficient. Backups must retain source history and transaction evidence needed by consumers. |
| Outbox/segments | Canonical manifests + exact source versions and absolute paths | Deterministic reconstruction/inspection tested while history survives. Interrupted claims fail closed; no permanently lost-history reconstruction. |
| Kafka acknowledgement journals | Local exclusive claims after send ack | Per-record proof and explicit replay exist. Journal must be recovered with publication lineage; the broker does not atomically store this file. |
| Consumer receipts/accepted ranges | Local files + cluster/topic/group/range identity | Receipt-before-commit crash replay tested. No independent storage replica or committed-offset rollback/restore rehearsal. |
| Logical inference results | Local request/candidate identities, pinned source rows and model | Retained claims prevent duplicate logical results. Their loss can permit repeat invocation; upstream receipts alone are not result bytes. |
| MLflow SQLite + artifact root | Local DB, local file URIs, cooperating lock | Read-back and conflict handling tested. No consistent backend/artifact restore preserving provider IDs demonstrated. |
| Candidate/model + source dataset/training | Exact bytes, runtime versions and often absolute source paths | Hash/runtime verification and reconstruction tested. No independent portable archive/restore. The Registry is an index, not a replacement for these bytes. |
| Feedback ledger + evidence dossiers | Local complete revisions/receipts, fixed source universe, immutable manifests | Reinspection tested. Neither authentic provider truth nor protected off-host retention follows from immutability checks. |

### File publication is not one uniform durability contract

**Repository-proven differences:** [Scala `FeatureReadyFiles.claim`][files] flushes the temporary file, hard-links the exclusive result and forces its parent directory. [Phase 26 `claim_result`][results] similarly flushes file and directory. These are stronger local publication steps than an ordinary buffered write; filesystem/OS support and retained storage still matter.

[Phase 24 `publish`][publication] exclusively creates a directory and writes artifacts with `write_bytes`, without explicit file or directory fsync. [Phase 27 `_claim`][worker] fsyncs the temporary result and hard-links it, but does not explicitly fsync the parent directory. [Registry receipt publication][registry] has its own file/lock sequence. Those differences do **not** negate the observed process restart tests. They do prevent a blanket claim that all artifacts survive abrupt host power loss. This is a source-review finding, not an executed power-loss failure. Partial directories/results, lock ownership and filesystem portability deserve a narrow W2 audit; a generic manifest-framework rewrite is not warranted.

**No measured RPO/RTO.** Replaying a stopped process with its volume intact differs from restoring permanently lost bytes. Required future W1 experiment: quiesce a synthetic lineage, capture a consistent bundle, verify independent hashes, restore into an isolated clean environment with pinned runtimes and preserved logical paths, and prove IDs/rows/state/Registry/receipts/results before resuming. Loss of any required component must fail explicitly. Do not modify old hashed manifests to relocate them or mutate Spark checkpoint files. A new-path portability design would be a separate decision.

SQLite requires a consistent database backup method, not an arbitrary copy during writes ([official backup API](https://www.sqlite.org/backup.html)). Spark state compatibility and Delta history requirements remain runtime-specific ([Spark 4.2 recovery rules](https://spark.apache.org/docs/4.2.0/streaming/apis-on-dataframes-and-datasets.html#recovery-semantics-after-changes-in-a-streaming-query), [Delta streaming](https://docs.delta.io/delta-streaming/)). These are external constraints on the proposed rehearsal, not evidence it already works.

## Stateful upgrades and retention dependencies

**T/L:** the Phase 29 guard binds Spark/Scala and semantic versions, RocksDB provider, watermark, inactivity timeout, source table ID, output/checkpoint paths and sink app ID. It then binds the actual target table ID. **N:** that experiment guard is not installed in every direct application entry point. A matching fingerprint is not proof a future Spark binary can deserialize existing state. [Guard][guard], [applications][apps].

- **State schema, dedup key, watermark and timer changes:** use explicit compatibility review/new lineage as documented. Preserve the source→output mapping and give fresh checkpoints fresh transaction app IDs. Do not reset state while appending silently incompatible features to an old semantic table. [ADR-020][adr20], [ADR-043][adr43].
- **Timer semantics:** C's retained EUR state produced prior count 2, mean `190.01999999999998` (independent expectation `190.02`) and stddev `81.65669109142249`. Its previous observed watermark preceded its expiry. A's expiry/reactivation evidence combines eligible timers, observed removals and the same key's later NO_HISTORY/count-zero row; aggregate removals alone would not prove which key expired. [State evidence][state-json], [state examples][state-examples].
- **Late records:** validity does not imply eligibility after watermark. Dedup drops are diagnostic progress fields, not a retained late-event output. Out-of-order accepted events do not rewrite older feature rows. [Dedup][dedup], [rolling][rolling], [event-time ADR][adr13].
- **Partition changes:** `customer_id` routing is topology-dependent. Per-currency observed-order state records partition and last offset; a customer's remapping can break the active statistical lineage. No repartition/state migration is implemented. [Statistics][statistics], [key ADR][key-adr].
- **Growth:** A/B show bounded cleanup for specific keys/times and modest checkpoint sizes. They do not bound peak state for arbitrary active-cardinality/rates, maximum checkpoint age, catch-up delay or full-snapshot inspection cost. The worker's finite 5,000-row contracts are local guardrails, not scale evidence. [Capacity](capability-matrix.md#capacity-evidence--local-bounded-observations), [worker][worker].

Necessary dependency graph (a proposal for retention ownership; no automatic deletion policy):

```text
unconsumed raw offsets → original Kafka logs/topic identity + Registry schemas
unfinished Spark lineage → source Delta/Kafka history + compatible checkpoint/state
                         + sink transaction evidence + output files
unresolved ready record → first-visible Delta version/files + outbox/segment identity
                       → publication journal + receipts/group fences
uncompleted inference → all above + exact candidate/runtime/source provenance
audit/research reinspection → dataset/training/model/feedback revisions + dossier bytes
```

Kafka retention can erase unprocessed ranges. Delta VACUUM can erase files still needed for historical reads even if the latest table is healthy ([Delta utility documentation](https://docs.delta.io/delta-utility/)). Checkpoint deletion changes recovery identity; model deletion halts verified inference; loss of provider revision history changes label availability; unbounded dossier accumulation competes with processing for local disk. No one-size retention interval is justified by the current experiments. **P1 W3** should report required dependencies and approaching loss, without deleting anything. Historical broker-fence JSON proves what was retained at capture time, not today's offsets. [Retention inspector][retention].

## Operational diagnosis today

| Operator question | Current evidence (**T/L**) | Missing unattended capability (**N**) |
|---|---|---|
| Is ingestion/checkpoint progressing? | Stage logs, source/output offsets, checkpoint files, Spark batch IDs [observer][observer] | Freshness/stall age and active-query supervision across runs |
| Are state/watermarks/drops changing? | Actual `StreamingQueryProgress`, state rows/memory/removals/dropped-by-watermark [progress][progress] | Continuous series, per-key skew diagnosis, alert thresholds and historical baselines |
| Where is research work behind? | Distinct undiscovered versions, unpublished, unaccepted and unprocessed counts [backlog][backlog] | Wall-clock age, automated sampling/alerting and rate/admission coordination with upstream |
| Is model/source unavailable? | Fail-closed candidate/Registry/source inspectors and failed tick [worker][worker] | Supervised bounded retry, failure classification dashboard and process deadline |
| Is retention blocking recovery? | Exact pinned reads and broker-fence comparison [retention][retention] | Fresh scheduled probes, expiry forecasts, free-space monitoring and accountable retention owner |
| What failed and can be replayed? | Named attempt logs, immutable identity inventories and bounded recovery commands [guide][guide] | Consistent incident IDs, automated ownership/escalation, tested host-recovery runbook |

Immutable offline reports support diagnosis after collection. They are not an always-on monitor, health service, traces of contemporaneous online availability or a latency SLO. The future operations scope must first decide who watches and responds; adding a dashboard alone does not create that guarantee.

[evidence]: executive-assessment.md#retained-evidence-inspected
[compose]: ../../../docker-compose.yml
[make]: ../../../Makefile
[producer-config]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/producer/TransactionProducerConfig.scala
[source]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/ingestion/TransactionKafkaSource.scala
[decoder]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/ingestion/RegistryBackedTransactionDecoder.scala
[provision]: ../../../scripts/provision-schema-registry.sh
[sink]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/persistence/delta/DeltaTransactionSink.scala
[dedup]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/deduplication/TransactionDeduplicationTransformer.scala
[outbox]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/availability/outbox.py
[publication]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/availability/contract.py
[outbox-tests]: ../../../model-control-plane/tests/test_availability.py
[publisher]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyKafkaPublisher.scala
[consumer]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyKafkaConsumer.scala
[receipt]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyReceiptStore.scala
[files]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyFiles.scala
[transport-tests]: ../../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyTransportSpec.scala
[worker]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py
[worker-tests]: ../../../model-control-plane/tests/test_incremental_shadow.py
[results]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/kafka_shadow/results.py
[candidate]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/candidates/run.py
[candidate-tests]: ../../../model-control-plane/tests/test_candidates.py
[registry]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/registry/run.py
[feedback]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/feedback/resolution.py
[feedback-tests]: ../../../model-control-plane/tests/test_feedback.py
[reader]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/datasets/feature_source.py
[guard]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/contract.py
[retention]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/retention.py
[recovery]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/operations.py
[outage]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/outage.py
[tests]: ../../../model-control-plane/tests/test_reliability.py
[inspector]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/evidence.py
[state-examples]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/state_examples.py
[observer]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/observation.py
[progress]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/StatisticalFeatureProgress.scala
[backlog]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/progress.py
[statistics]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/CustomerAmountStatisticsProcessor.scala
[rolling]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/rollingfeatures/RollingFeatureProcessor.scala
[apps]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing
[guide]: ../../architecture/stateful-streaming-reliability.md
[adr13]: ../../adr/ADR-013-event-time-watermark-semantics.md
[adr20]: ../../adr/ADR-020-currency-safe-monetary-feature-semantics.md
[key-adr]: ../../adr/ADR-006-kafka-transaction-partition-key.md
[adr39]: ../../adr/ADR-039-feature-ready-kafka-transport.md
[adr40]: ../../adr/ADR-040-kafka-triggered-research-shadow-inference.md
[adr41]: ../../adr/ADR-041-incremental-shadow-operations-and-backpressure.md
[adr43]: ../../adr/ADR-043-stateful-streaming-reliability-and-capacity.md
[state-json]: ../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/statistical_state_evidence.json
