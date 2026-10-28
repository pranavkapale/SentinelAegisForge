# Phase 30 — Production readiness assessment

Assessment date: 2026-10-28. Reviewed revision: `484c410` (`Verified stateful spark recovery and characterized local capacity`). The initial worktree was clean. This is a documentation-only assessment, not a deployment approval or a new operational capability.

## Executive decision

**A technically substantial research portfolio: yes. A production-ready deployment: no. A model ready to influence real fraud decisions: no.** These are three separate judgments, not a maturity score.

The strongest contribution is explicit, executable correctness at system boundaries: transaction validation, currency-safe state, retained checkpoint recovery, Delta batch retry suppression, Kafka uncertainty, receipt-before-offset persistence, and independently checked research results. Source and tests support those mechanisms; retained local evidence supports the bounded integration. It does not support availability under permanent storage loss, hostile access, multi-host operation or real fraud efficacy. See the [capability inventory](capability-matrix.md), [failure register](failure-mode-register.md) and [portfolio review](portfolio-readiness.md).

Recommend **Option 2: one focused hardening milestone**, a consistent local recovery bundle and isolated clean-environment restore rehearsal ([W1](prioritized-roadmap.md#w1--consistent-recovery-bundle-and-isolated-restore-rehearsal)). Finalize a concise portfolio walkthrough alongside it. Do not start a cloud deployment, continuous service, model promotion or another framework by default. This recommendation is not implemented in Phase 30.

## Evidence discipline

Each finding uses one of these classifications. Classification applies to the precise claim, not the whole technology:

- **IMPLEMENTED_AND_TESTED**: inspected source plus relevant executed test evidence. Historical full regression evidence is explicitly identified below; it is not a fresh Phase 30 full-suite result.
- **IMPLEMENTED_LOCALLY**: demonstrated in the retained local experiment, with no representative deployed-environment result.
- **DESIGNED_NOT_IMPLEMENTED**: a documented architecture or decision exists without the corresponding capability.
- **NOT_ESTABLISHED**: evidence is absent, insufficient or outside this review's inspection.
- **NOT_APPLICABLE_TO_CURRENT_SCOPE**: intentionally outside this non-enforcing research platform.

Evidence basis is separately identified as **repository-proven** (code/tests), **reported local execution** (retained artifacts or project-state observations), **external recommendation** (official platform guidance), or **inference/proposal**. An inspected hash establishes consistency with the retained manifest, not independent authenticity. Neither an ADR nor a test name alone proves a live outcome.

## What is demonstrated

1. **IMPLEMENTED_AND_TESTED — correctness contracts.** Scala separates untrusted candidates from validated events and Avro mapping; Python accepts only the corrected fixed-version 25-column feature contract. Rolling velocity is customer-wide; monetary rolling sums and Welford baselines are currency-dimensional. Tests protect event-time boundaries, Double consistency and exact persisted decimals. [Validator](../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/domain/transaction/TransactionEventValidator.scala), [statistical processor](../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing/statisticalfeatures/CustomerAmountStatisticsProcessor.scala), [dataset gate](../../../model-control-plane/src/sentinelaegisforge_control_plane/datasets/dataset_contract.py), [ADR-020](../../adr/ADR-020-currency-safe-monetary-feature-semantics.md), [tests/evidence inventory](capability-matrix.md).
2. **IMPLEMENTED_LOCALLY — real integration and state recovery.** Phase 29 A/B/C/D reconcile 3,560 generated and acknowledged events through four Delta stages and research acceptance/results. C recovered a real statistical post-commit failure using the same checkpoint and transaction application ID, then demonstrated continuing prior state. D recovered a real broker outage and an explicitly refused publication during inference backlog. [Phase 29 evidence](#retained-evidence-inspected), [recovery code](../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/operations.py), [ADR-043](../../adr/ADR-043-stateful-streaming-reliability-and-capacity.md).
3. **IMPLEMENTED_AND_TESTED — auditable ML research and fail-closed governance.** Snapshot, training, candidate, Registry and result identities are distinct. Independent labels do not originate from risk rules. Model bytes, runtime and allowed deserialization types are checked. Registration never grants promotion. [Candidate loader](../../../model-control-plane/src/sentinelaegisforge_control_plane/candidates/run.py), [Registry](../../../model-control-plane/src/sentinelaegisforge_control_plane/registry/run.py), [validation policy](../../../model-control-plane/src/sentinelaegisforge_control_plane/validation/policy.py), [Phase 18 requirements](capability-matrix.md#model-readiness--all-ten-phase-18-requirements).

## Three highest technical risks

### R1 — Shared storage failure can erase the recovery authority

**NOT_ESTABLISHED: restoration after permanent storage loss.** Kafka logs/KRaft metadata and Schema Registry's `_schemas` share one RF-1 broker volume. Delta, checkpoints, outboxes, receipts, models, dossiers and SQLite reside on local storage. Process restart with intact files is demonstrated; a consistent off-host backup and restoration preserving identities is not. Local hashes do not replace missing bytes. This is the highest-value next reliability experiment, rather than more throughput measurements. [Compose](../../../docker-compose.yml), [Make paths](../../../Makefile), [ADR-031](../../adr/ADR-031-model-registry-candidate-governance.md), [durability inventory](failure-mode-register.md#storage-dependencies-and-disaster-recovery).

### R2 — Integrity validation is not authorization

**NOT_ESTABLISHED: a secure boundary for external users or real transaction data.** Host ports are loopback-bound, a useful local containment control. Inside that boundary Kafka is plaintext without configured authentication/ACLs, Registry is HTTP without configured authentication, and artifacts rely on host ownership. A writer able to replace both content and its hashes can forge local evidence. Model loading remains trusted-local despite its narrow skops type gate. Do not expose these endpoints or import real sensitive data until identities, least privilege, encrypted transport and data policy are deliberately established. [Threat model](architecture-threat-model.md), [Compose](../../../docker-compose.yml), [loader](../../../model-control-plane/src/sentinelaegisforge_control_plane/candidates/run.py).

### R3 — Recovery depends on compatible state and retained history

**IMPLEMENTED_LOCALLY: fail-closed retention/compatibility checks; NOT_ESTABLISHED: migration and unattended retention management.** Exact Delta versions, Kafka ranges/topic identity and local checkpoint state must survive together. The Phase 29 compatibility guard belongs to its experiment harness, not every direct Spark command. The Python worker's full-snapshot validation and bounded histories are not a continuous CDC system. Pending research work can become unverifiable while upstream commits continue. [Retention checks](../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/retention.py), [guard](../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/contract.py), [worker](../../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py), [state/retention analysis](failure-mode-register.md#stateful-upgrades-and-retention-dependencies).

## Production and fraud decision gates

Production-like research operation would first need an explicit operating scope, secured service identities, storage restoration, retained-history ownership, supervised execution, tested release/rollback, and operator-visible failures. These are gaps, not reasons to deploy every common enterprise technology. [Prioritized work](prioritized-roadmap.md) separates conditional P0 gates from useful local P1 work.

The exact registered model remains `sentinelaegisforge-offline-fraud-logistic`, version `1`, `REGISTERED_RESEARCH_ONLY`, `production_eligible=false`. Phase 18 remains `NOT_ELIGIBLE_FOR_PROMOTION`. VAL001–003 are VERIFIED in the retained assessment; VAL004–010 remain NOT_ESTABLISHED. Phase 29's useful local fault evidence does not retrospectively approve VAL009. Representative authenticated independent outcomes, temporal generalization, discrimination, calibration, segment validation, deployed operational evidence and accountable approval are still missing. [Policy and evidence](capability-matrix.md#model-readiness--all-ten-phase-18-requirements).

The synthetic validation/test AP values are `0.11443784507721583` / `0.08116962182591815`; ROC AUC is `0.45287237702098376` / `0.4655847436221269`. These are integration diagnostics, not production performance. No threshold, model, label or scenario was changed. [Retained Phase 18 report](../../../.local/ml/model-validations/6f344ce53a1d16ac5acfe165876d1ce8861a22c6eae6db9e8c409d58a38cfcea/validation_report.md), [tracked historical evidence](../../../PROJECT_STATE.md#phase-18-validation-evidence-verification).

## Retained evidence inspected

These relative links target ignored local artifacts available during this review. A fresh clone will not contain them; the committed [PROJECT_STATE](../../../PROJECT_STATE.md#phase-29-real-stateful-reliability-and-capacity-evidence) and implementation/tests remain accessible. Do not interpret an unavailable local link in another checkout as independently inspected live evidence there.

- [Phase 29 manifest](../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/reliability_manifest.json): ID `b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c`, manifest SHA-256 `3da8653a6bd6e95429e869f73f456f0f6618ae5aa36fdfa59b266a86659b234a`. Existing `reliability.evidence.inspect` rechecked all **11** artifact hashes and semantic identity. This inspector checks the dossier, not current broker retention.
- [Reconciliation](../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/source_reconciliation.json), [state](../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/statistical_state_evidence.json), [capacity](../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/capacity_measurements.json), and [parity](../../../.local/phase29/reliability/b85a5627a062ea4669c204f4b69cc0e8495121ea24b979ffd59b220b0b90167c/model_parity.json) were read. A fresh read-only comparison of the four plans, detailed acknowledgements and **16 pinned version-2 Delta tables** confirmed table identities, exact event-ID sets and validated Kafka coordinates: **1,500/2,000/30/30**. No Kafka or Spark process was started.
- [Phase 18 assessment](../../../.local/ml/model-validations/6f344ce53a1d16ac5acfe165876d1ce8861a22c6eae6db9e8c409d58a38cfcea/assessment.json) and report matched their retained evidence-manifest hashes. Assessment SHA-256: `865861ff23793b0fdc9d281b276ef29715652d18b8ce168a6880ed019fca3e4b`. This was byte verification, not a new assessment or model reconstruction.
- SQLite was opened with `mode=ro`. The stored Registry contains the single model/version above, creation status READY and unchanged governance tags. Candidate ID is `b4fff4f1f74818165d04bae094b89f7c7b8dc7367d6e3dad10465b1a6160fadb`. READY is catalog status, not approval. No Registry write or model load was needed.

## Review method and commands

Inspected the complete project-state history, READMEs, architecture overview, module inventory/builds, ADR-030–043, relevant earlier state/delivery contracts, Compose/provisioning, CI, producer/source/sinks, rolling/Welford configuration, outbox/journal/receipt/result code, research workers, artifact loader, Registry/policy, feedback, progress collectors and recovery tests. Searches for backup, restore, deployment, access control and operational telemetry found local procedures but no demonstrated disaster-recovery/deployment control plane. GitHub branch protection, remote CI runs, host ACLs, firewall and disk encryption were not inspected and are **NOT_ESTABLISHED**.

Commands included `git status --short`, `git log -1 --format='%h %s'`, `rg --files`, targeted `rg -n`, `cat`/`sed` source reads, and small `model-control-plane/.venv/bin/python -B -` read-only scripts for dossier SHA-256 verification, SQLite SELECTs, file-mode inspection and pinned Delta ID reconciliation.

The focused regression command, run from `model-control-plane`, was:

```sh
PYTHONDONTWRITEBYTECODE=1 MLFLOW_DISABLE_AGENT_HINT=1 .venv/bin/python -B -m pytest -q -p no:cacheprovider tests/test_reliability.py tests/test_validation.py tests/test_feedback.py tests/test_incremental_shadow.py tests/test_registry.py
```

Result: **72 passed**, one existing MLflow/SQLAlchemy `noload` deprecation warning, 14.64 seconds. Temporary fixture writes are isolated from retained evidence. No Spark/Kafka/MLflow UI started. The full **284 Python / 122 Scala in 22 suites** baseline is **reported Phase 29 regression evidence**, not a fresh Phase 30 full-suite execution. Full compile/root checks are unnecessary for documentation-only changes. A pure Markdown audit checked **311 references** across the six documents, including local paths, reference definitions and heading anchors, with no errors; sixteen links require the explicitly disclosed ignored local evidence. Whitespace/fence checks passed. `git diff --check` passed; a scope check confirmed exactly six new assessment documents and an append-only PROJECT_STATE change. Final Phase 29 manifest/artifact hashes remained unchanged.

Official platform references were checked for bounded external recommendations, not as proof of repository implementation: [Spark 4.2 checkpoint changes](https://spark.apache.org/docs/4.2.0/streaming/apis-on-dataframes-and-datasets.html#recovery-semantics-after-changes-in-a-streaming-query), [Delta streaming](https://docs.delta.io/delta-streaming/), [Kafka 4.3 security](https://kafka.apache.org/43/security/security-overview/), [Schema Registry security](https://docs.confluent.io/platform/current/schema-registry/security/index.html), [SQLite backup](https://www.sqlite.org/backup.html), [GitHub Actions secure use](https://docs.github.com/en/actions/reference/security/secure-use). No vendor pricing, legal interpretation, vulnerability certification or invasive security test was attempted.

## Assessment map

- [Capability matrix, model gates, CI and measured capacity](capability-matrix.md)
- [Current architecture, security and threat model](architecture-threat-model.md)
- [Failure modes, delivery semantics, durability and retention](failure-mode-register.md)
- [Prioritized backlog, experiment designs and deployment choices](prioritized-roadmap.md)
- [Portfolio value, technical debt, ADR quality and stop-building decision](portfolio-readiness.md)

Only these six documents and a concise appended project-state assessment entry are changed. No new ADR is justified by an assessment alone. No Phase 31 recommendation is implemented.
