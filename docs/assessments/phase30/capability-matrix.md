# Capability inventory and evidence limits

Read the [classification and inspection method](executive-assessment.md#evidence-discipline) first. In the inventory, **T** = IMPLEMENTED_AND_TESTED, **L** = IMPLEMENTED_LOCALLY, **D** = DESIGNED_NOT_IMPLEMENTED, **N** = NOT_ESTABLISHED, **X** = NOT_APPLICABLE_TO_CURRENT_SCOPE. A mechanism can be T while its deployment guarantee is N. Historical executed-test evidence is the [Phase 29 regression record][state]; the focused Phase 30 run re-executed 72 reliability, policy, feedback, incremental-worker and Registry tests. No remote CI result is asserted.

## Capability matrix

| Domain / finding | Established behavior and classification | Evidence: implementation, test, decision or execution | Boundary / missing capability |
|---|---|---|---|
| C01 Transaction contracts | **T:** candidate validation; canonical Avro UUID, microsecond timestamps and decimal money; schema compatibility and distinct identities | [Validator][validator], [schema][schema], [Avro tests][avro-tests], [ADR-005][avro-adr] | External schema acceptance does not prove honest input. Conflicting same-event-ID payload analysis is **N**, explicitly deferred in [ADR-012][dedup-adr]. |
| C02 Ingestion | **T:** governed Avro serializer, customer key, acknowledgements, Kafka source failOnDataLoss; **L:** recorded source-offset recovery | [Producer][producer], [Kafka source][source], [producer tests][producer-tests], [state][state], [ADR-008][producer-adr] | One RF-1 broker, no authenticated clients/HA; transport retry is not lifetime event uniqueness. |
| C03 Processing | **T:** audit/semantic tables separate; bounded event-ID dedup with event-time watermark; **L:** real late-drop evidence | [Dedup transformer][dedup], [dedup tests][dedup-tests], [ADR-012][dedup-adr], [Phase 7 evidence][phase7] | Global/lifetime uniqueness and late side-output are **N**; late-data policy is not an empirical production SLA. |
| C04 Stateful features | **T:** event-time rolling count/sum; prior-observed customer-currency Welford; **L:** RocksDB restart/expiry | [Rolling processor][rolling], [statistics][statistics], [processor/restart tests][stats-tests], [ADR-020][currency-adr], [state][state] | Whole-state capacity, cross-machine state recovery and version upgrades are **N**. Phase 8 totals remain diagnostics, not monetary model features. |
| C05 Storage | **T:** schema-checked path-based Delta and fixed-version Python reader; **L:** txn retries | [Sink][sink], [reader][reader], [sink tests][sink-tests], [dataset tests][dataset-tests] | Redundant storage, retention ownership, portable restore and disk-full recovery are **N**. |
| C06 Delivery | **T:** exact committed-row notification membership, outbox, ack journal, receipt-before-offset; **L:** real broker replay | [Outbox][outbox], [publisher][ready-pub], [consumer][ready-cons], [transport tests][transport-tests], [ADR-039][transport-adr] | One cooperating local writer and one group member; concurrent rebalance/multi-host ownership are **N**. |
| C07 Recovery | **T:** fault/configuration and retention guards; **L:** statistical post-commit restart and D outage recovery | [Recovery operations][recovery], [reliability tests][reliability-tests], [statistical restart test][stats-tests], [ADR-043][reliability-adr], [dossier index][evidence] | Permanent disk/volume loss recovery, measured RPO/RTO and restore on another machine are **N**. |
| C08 Model lifecycle | **T:** immutable temporal training, tracking index, candidate file/runtime verification, research Registry | [Training][training], [candidate][candidate], [Registry][registry], [candidate tests][candidate-tests], [Registry tests][registry-tests], [ADR-031][registry-adr] | Mutable tags are not a signed approval; metadata is not artifact backup. Serving/promotion are **X** now. |
| C09 Research inference | **T:** accepted-notification validation, exact first-visible rows, bounded ticks and logical results; **L:** 2,393 score parity comparisons | [Worker][worker], [incremental tests][worker-tests], [ADR-040/041][worker-adr], [parity evidence][parity] | Physical invocation can repeat. Process-isolated deadlines, continuous supervision and host failover are **N**. |
| C10 Evaluation | **T:** temporal fit-only preprocessing, delayed-label cutoff and support gates; **L:** synthetic evaluation | [Temporal split][temporal], [training tests][training-tests], [feedback tests][feedback-tests], [ADR-025][temporal-adr] | Representative fraud generalization/calibration is **N**, not a weak pass. |
| C11 Monitoring | **T:** frozen-score quality, label-free distribution distances, immutable investigation packets | [Monitoring package][monitoring], [investigation tests][investigation-tests], [ADR-035][investigation-adr] | A continuous monitor, alert policy and causal diagnosis are **N**. |
| C12 Security | **L:** loopback host endpoints; **T:** strict parsing, hashes, trusted-local type gates | [Compose][compose], [codec][codec], [candidate][candidate], [security review](architecture-threat-model.md#security-control-review) | Authentication, service authorization, encrypted transport and hostile-writer attestation are **N**. |
| C13 Deployment | **T:** two independent build jobs; **L:** local Compose and Make invocation | [CI][ci], [Makefile][make], [build][build] | Release packaging, rollout, environment promotion and rollback automation are **N**; no deployed cloud components. |
| C14 Availability | **L:** restart with retained broker volume; bounded process recovery | [Compose][compose], [state][state], [outage code][outage] | Multi-broker quorum, host redundancy, RPO/RTO and availability objective are **N**. |
| C15 Observability | **T:** Spark progress, watermark/drop/state fields and separate research backlog calculations | [Progress][progress], [research progress][research-progress], [observation][observation], [reliability tests][reliability-tests] | Always-on collection, alerts, stage stall ages, RSS/CPU and causal end-to-end timing are **N**. |
| C16 Operations | **L:** explicit bounded start/stop/fault/recovery instructions and evidence inspection | [Reliability guide][reliability-guide], [incremental guide][incremental-guide], [Makefile][make] | Automated backup/restore, incident ownership and unattended service runbook are **N**. |
| C17 Data governance | **T:** strict label revisions, no missing-as-LEGIT, candidate feature allowlist, synthetic scope | [Label resolver][labels], [feedback resolution][feedback], [dataset tests][dataset-tests], [ADR-036][feedback-adr] | Real-data acquisition authority, authenticated provider, retention/deletion and confidentiality policy are **N**. |
| C18 Model governance | **T:** ten mandatory evidence gates, no approval path, exact Registry version | [Policy][policy], [validation tests][validation-tests], [ADR-032][policy-adr] | Production eligibility remains false. Business accountability and approved enforcement are **N**; enforcement is **X** for this scope. |

## Model readiness — all ten Phase 18 requirements

The preserved [assessment][assessment] and [report][assessment-report] were read and their byte hashes rechecked against the evidence manifest. [Policy source][policy] and [tests][validation-tests] enforce the following. This is a faithful account of the governing assessment, not a new positive-evidence collector.

| Requirement | Phase 18 result | Phase 30 classification and evidence needed |
|---|---|---|
| VAL001 Artifact integrity | VERIFIED | **T / reported local execution:** content tree, runtime and explicit local model source checked. An independent signed artifact origin and backup remain absent. |
| VAL002 Model reconstruction | VERIFIED | **T / reported local execution:** 930 archived validation/test probabilities matched after reconstruction/reload. This is equivalence, not fraud utility. |
| VAL003 Dataset provenance | VERIFIED | **T / reported local execution:** pinned Delta, snapshot, delayed-label bytes, training and tracking identities. Authentic real labels are a separate requirement. |
| VAL004 Real-world label quality | NOT_ESTABLISHED | **N:** acquire authorized independently adjudicated outcomes; attest provider/receipt provenance, revisions/retractions, coverage and selection bias. Local provider strings do not authenticate anyone. |
| VAL005 Temporal generalization | NOT_ESTABLISHED | **N:** representative out-of-time populations and maturity-aware evaluation across periods. Correct synthetic split mechanics alone do not establish this. |
| VAL006 Model discrimination | NOT_ESTABLISHED | **N:** representative fraud discrimination at a business-defined operating point, uncertainty/support and comparison baselines. Do not turn synthetic AP/AUC into acceptance thresholds. |
| VAL007 Probability calibration | NOT_ESTABLISHED | **N:** held-out representative calibration evidence, with a separately governed procedure if correction is needed. No such experiment is run here. |
| VAL008 Segment robustness | NOT_ESTABLISHED | **N:** sufficiently supported customer/currency/transaction populations, rare segments and justified fairness/robustness criteria. Synthetic slice reports are descriptive. |
| VAL009 Operational validation | NOT_ESTABLISHED | **N for deployment:** later local recovery evidence helps design tests but does not amend Phase 18 or establish secured serving, deadlines, availability and unattended monitoring. |
| VAL010 Business governance | NOT_ESTABLISHED | **N:** identified approval authority, permitted use, accountable operator, incident and rollback responsibilities. No override or promotion introduced. |

The fixed policy even refuses promotion for an in-memory all-verified fixture: it has no approval authority. Model `sentinelaegisforge-offline-fraud-logistic` v1 remains `REGISTERED_RESEARCH_ONLY`; `production_eligible=false`. The Phase 18 result is `NOT_ELIGIBLE_FOR_PROMOTION`. [Registry tests][registry-tests], [policy tests][validation-tests].

### Independent outcome source

**T:** Phase 22 validates provider/case/revision identities, unknown transaction rejection, cumulative receipt-time availability and conflicts. PENDING and RETRACTED never become LEGIT; a missing predecessor or contradictory confirmed provider stops evaluation. **N:** the nine-case `LOCAL_PROVIDER_FIXTURE` is not authentic provider truth. [Importer][feedback-import], [resolver][feedback], [ADR-036][feedback-adr], [tests][feedback-tests].

**Proposal:** a genuine source needs authorized acquisition and provider authentication, an authorized source-of-authority mapping, trusted receipt-time capture, stable case/event binding, complete revisions/retractions, governed cross-provider conflicts, secure retention, privacy restrictions and label-quality audits. Sample ascertainment and missingness must be measured on representative scored populations. Authentication proves origin, not adjudication accuracy. The current code supplies revision mechanics, not those external guarantees. See [W6/W7](prioritized-roadmap.md).

## CI/CD and release maturity

**Repository-proven configuration:** [CI][ci] defines push/PR jobs on `ubuntu-latest`, JDK 21 and Python 3.13. Scala executes `sbt "scalafmtCheckAll ; compile ; test"`; Python executes locked uv sync, pytest, Ruff lint/format and mypy. Pytest includes local temporary Delta/SQLite fixtures; Scala tests include local Spark/Delta stateful integration. Broker contract tests use mocks/fixtures, not a CI broker. [Build][build], [Python project][pyproject], [transport tests][transport-tests], [statistical tests][stats-tests].

**NOT_ESTABLISHED in this review:** actual hosted CI run outcomes, branch protections or mandatory merge checks. The workflow is intended enforcement; a repository file cannot prove it ran. `test` may use sbt's incremental test cache locally; the 122-test Phase 29 evidence came from explicit `Test / testOnly *`, not a later zero-change cached invocation. Fresh hosted execution must be read from its logs.

**N from checked workflow scope:** no application container build, image-signing/SBOM gate, dependency vulnerability review, secret scan, real Kafka/Registry matrix, artifact attestation, deploy approval, rollback test or release pipeline. Python packages are pinned in [uv.lock][lock]; Scala coordinates are pinned with documented Jackson overrides but have no equivalent complete dependency lock. Compose images and Actions use version tags rather than immutable digests/commit SHAs. `contents: read` is a useful least-privilege setting; no deployment secret use is present in this workflow. Repository-host secret configuration was not queried. Pinning improves reproducibility but is not a vulnerability scan. [CI][ci], [Compose][compose], [build][build].

**External recommendation:** immutable action SHAs and least-privilege tokens reduce supply-chain exposure; credentials must not be passed to untrusted PR execution. These are future release controls, not evidence they already exist. [GitHub secure-use guidance](https://docs.github.com/en/actions/reference/security/secure-use).

## Capacity evidence — local bounded observations

**IMPLEMENTED_LOCALLY / reported local execution, artifacts inspected:** [capacity JSON][capacity], [state JSON][state-json], [Spark progress JSON][spark-json], [Phase 29 method][reliability-guide]. Values below are from the preserved evidence, not rerun performance measurements.

| Population | A baseline | B changed workload |
|---|---:|---:|
| Transactions; three wave sizes | 1,500; 500/500/500 | 2,000; 667/667/666 |
| Customers; distinct customer-currency keys | 90; 177 | 45; 90 |
| USD / EUR records | 976 / 524 | 1,496 / 504 |
| Records by Kafka partition 0/1/2 | 409 / 723 / 368 | 464 / 848 / 688 |
| Distinct customers by partition | 26 / 41 / 23 | 10 / 20 / 15 |
| Maximum / median records per customer | 29 / 17 | 65 / 45 |
| Max-partition / mean-partition records | 1.446 | 1.272 |
| NO_HISTORY / INSUFFICIENT / READY / ZERO_VARIANCE | 272 / 246 / 982 / 0 | 116 / 112 / 1,772 / 0 |
| Null prior mean / stddev / z-score | 272 / 518 / 518 | 116 / 228 / 228 |
| State rows, input pass → timer pass, each wave | 151→99; 152→100; 168→101 | 82→69; 82→69; 90→77 |
| Reported statistical state memory range, bytes | 660,367–1,140,637 | 570,946–905,919 |
| Statistical checkpoint logical bytes / files | 5,531,460 / 3,464 | 5,042,004 / 2,948 |

Per-stage invocation seconds, rounded to three decimals; **three samples per stage per experiment**, in wave order:

| Stage | A: wave 0 / 1 / 2 | B: wave 0 / 1 / 2 |
|---|---|---|
| Transaction publish | 8.906 / 9.225 / 8.848 | 9.026 / 9.682 / 8.896 |
| Validated | 12.010 / 13.597 / 14.090 | 12.264 / 13.886 / 14.067 |
| Deduplicated | 12.367 / 11.194 / 12.182 | 12.984 / 13.390 / 12.981 |
| Rolling | 28.838 / 24.504 / 23.647 | 28.942 / 22.709 / 22.900 |
| Statistical | 27.445 / 23.412 / 24.190 | 28.048 / 22.496 / 23.569 |
| Statistical Spark trigger totals only | 23.427 / 19.482 / 20.276 | 23.513 / 18.188 / 19.533 |

**Supported inference:** stateful stages were the longest core invocations here; cleanup occurred; B increased observations per customer while reducing keys. B's slightly lower statistical median (23.569 versus 24.190 seconds) does not establish faster scaling: seed, population, currency mix and count changed together. Current locality is arm64 Mac, 10 logical CPUs, `local[*]`, 200 state-store/shuffle instances; no tuning was applied. Each wave launches a new JVM/Spark process. Later waves restore checkpoints, not warm continuously running processes. Invocation-minus-trigger time contains setup/teardown/logging, not an isolated startup measurement. File sizes are logical lengths, not allocated disk blocks or cloud costs. Peak RSS, CPU utilization and precise configured heap measurements are unavailable. [Collector][observation], [capacity JSON][capacity].

D's five controlled backlog snapshots observed maxima of 0 undiscovered versions, 10 unpublished events, 10 acknowledged/unaccepted events and 10 accepted/unprocessed events. Discovery was sampled after discovery; zero is not proof there was never a discovery backlog. A proposed queue of 20 against limit 15 was refused; all 30 eventually completed with zero backlog. No continuous peaks, steady-state throughput, p99, RPO/RTO, SLO, cloud sizing or production fraud-quality conclusion follows. [Outage evidence][outage-json], [backlog evidence][backlog-json], [experiment proposals](prioritized-roadmap.md#bounded-experiments-worth-considering).

[state]: ../../../PROJECT_STATE.md#phase-29-real-stateful-reliability-and-capacity-evidence
[phase7]: ../../../PROJECT_STATE.md#phase-7-verification
[evidence]: executive-assessment.md#retained-evidence-inspected
[validator]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/domain/transaction/TransactionEventValidator.scala
[schema]: ../../../contracts/events/transaction-event-v1.avsc
[avro-tests]: ../../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/serialization/avro
[avro-adr]: ../../adr/ADR-005-transaction-event-serialization.md
[producer]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/producer/TransactionEventPublisher.scala
[source]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/ingestion/TransactionKafkaSource.scala
[producer-tests]: ../../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/kafka/producer/TransactionEventPublisherSpec.scala
[producer-adr]: ../../adr/ADR-008-kafka-producer-delivery-semantics.md
[dedup]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/deduplication/TransactionDeduplicationTransformer.scala
[dedup-tests]: ../../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/deduplication
[dedup-adr]: ../../adr/ADR-012-business-event-deduplication.md
[rolling]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/rollingfeatures/RollingFeatureProcessor.scala
[statistics]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/CustomerAmountStatisticsProcessor.scala
[stats-tests]: ../../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/StatisticalFeatureStreamingSpec.scala
[currency-adr]: ../../adr/ADR-020-currency-safe-monetary-feature-semantics.md
[sink]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/persistence/delta/DeltaTransactionSink.scala
[reader]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/datasets/feature_source.py
[sink-tests]: ../../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/persistence/delta/DeltaTransactionSinkSpec.scala
[dataset-tests]: ../../../model-control-plane/tests/test_datasets.py
[outbox]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/availability/outbox.py
[ready-pub]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyKafkaPublisher.scala
[ready-cons]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyKafkaConsumer.scala
[transport-tests]: ../../../streaming-engine/src/test/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyTransportSpec.scala
[transport-adr]: ../../adr/ADR-039-feature-ready-kafka-transport.md
[recovery]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/operations.py
[reliability-tests]: ../../../model-control-plane/tests/test_reliability.py
[reliability-adr]: ../../adr/ADR-043-stateful-streaming-reliability-and-capacity.md
[training]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/training/run.py
[candidate]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/candidates/run.py
[registry]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/registry/run.py
[candidate-tests]: ../../../model-control-plane/tests/test_candidates.py
[registry-tests]: ../../../model-control-plane/tests/test_registry.py
[registry-adr]: ../../adr/ADR-031-model-registry-candidate-governance.md
[worker]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py
[worker-tests]: ../../../model-control-plane/tests/test_incremental_shadow.py
[worker-adr]: ../../adr/ADR-041-incremental-shadow-operations-and-backpressure.md
[temporal]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/training/temporal.py
[training-tests]: ../../../model-control-plane/tests/test_training.py
[temporal-adr]: ../../adr/ADR-025-temporal-ml-evaluation.md
[monitoring]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/monitoring
[investigation-tests]: ../../../model-control-plane/tests/test_investigation.py
[investigation-adr]: ../../adr/ADR-035-offline-monitoring-investigation-and-runbooks.md
[compose]: ../../../docker-compose.yml
[codec]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/kafka/featureavailability/FeatureReadyCodec.scala
[ci]: ../../../.github/workflows/ci.yml
[make]: ../../../Makefile
[build]: ../../../streaming-engine/build.sbt
[pyproject]: ../../../model-control-plane/pyproject.toml
[lock]: ../../../model-control-plane/uv.lock
[outage]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/outage.py
[progress]: ../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/StatisticalFeatureProgress.scala
[research-progress]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/progress.py
[observation]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/observation.py
[reliability-guide]: ../../architecture/stateful-streaming-reliability.md
[incremental-guide]: ../../architecture/incremental-shadow-operations.md
[labels]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/datasets/labels.py
[feedback]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/feedback/resolution.py
[feedback-import]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/feedback/ingestion.py
[feedback-tests]: ../../../model-control-plane/tests/test_feedback.py
[feedback-adr]: ../../adr/ADR-036-durable-outcome-feedback-ingestion.md
[policy]: ../../../model-control-plane/src/sentinelaegisforge_control_plane/validation/policy.py
[validation-tests]: ../../../model-control-plane/tests/test_validation.py
[policy-adr]: ../../adr/ADR-032-model-validation-evidence-policy.md
[assessment]: ../../../.local/ml/model-validations/6f344ce53a1d16ac5acfe165876d1ce8861a22c6eae6db9e8c409d58a38cfcea/assessment.json
[assessment-report]: ../../../.local/ml/model-validations/6f344ce53a1d16ac5acfe165876d1ce8861a22c6eae6db9e8c409d58a38cfcea/validation_report.md
[capacity]: ../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/capacity_measurements.json
[state-json]: ../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/statistical_state_evidence.json
[spark-json]: ../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/spark_stage_observations.json
[parity]: ../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/model_parity.json
[outage-json]: ../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/kafka_recovery.json
[backlog-json]: ../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/backpressure_observations.json
