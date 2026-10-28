# Prioritized engineering roadmap

**Recommendation: Option 2 — exactly one focused hardening milestone, W1.** Do not automatically implement this backlog. The present local research stack is already a substantial portfolio; further work should close an identified evidence gap, not accumulate frameworks. [Executive decision](executive-assessment.md), [stop-building analysis](portfolio-readiness.md#stop-building-decision).

All work below is an **inference/proposal**. Current limitations use the [assessment classifications](executive-assessment.md#evidence-discipline). A proposed implementation is not DESIGNED_NOT_IMPLEMENTED merely because this review suggests it; that classification is reserved for existing accepted designs. No date, staffing commitment, vendor price, production approval or new SLO is implied.

## Priority and effort conventions

- **P0 — conditional safety/correctness gate:** must be resolved before real data, externally reachable services or decisions affecting customers. Not an instruction to expand the current synthetic scope.
- **P1 — high-value reliability:** closes a demonstrated local operational weakness or makes recovery independently reproducible.
- **P2 — demonstration/developer experience:** improves evidence accessibility without implying operational maturity.
- **Deferred:** the current use case does not justify implementation.

Relative effort assumes one engineer familiar with this repository and retained local artifacts: **S** = bounded documentation/validation around existing interfaces; **M** = coordinated filesystem/runtime integration with new failure tests; **L** = new deployment or external-data contracts and operational ownership. These are relative complexity labels, not duration estimates. P0 does not outrank useful local work until its scope-trigger exists.

## W1 — Consistent recovery bundle and isolated restore rehearsal

- **Problem / current evidence:** process restart succeeds with retained files, but a single host/storage failure can remove Kafka, schemas, Delta, state, receipts, MLflow and model evidence together. Phase 29 C/D prove retained-state recovery, not restoration. **NOT_ESTABLISHED:** recoverable independent backup. [Failure register F01–F05/F11–F13](failure-mode-register.md), [Compose](../../../docker-compose.yml), [Phase 29 recovery](../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/operations.py).
- **Missing guarantee:** a coherent point-in-time authority set can be restored without changing logical identities or silently losing acknowledged work.
- **Proposed implementation:** one bounded, synthetic-only, quiesced backup procedure and read-only bundle manifest. Include broker logs/KRaft/topic IDs and `_schemas`; exact required Delta logs/data; compatible checkpoints/RocksDB and app IDs; outbox/journals/receipts/results; consistent SQLite/artifacts; candidate/dataset/training/feedback/evidence dependencies. Keep an independent copy and checksum inventory. Restore into a clean isolated environment under the same pinned runtime and logical filesystem paths. Existing absolute-path hashes/file URIs must remain valid; portability/remapping is a separate contract, not manifest editing. Quiescence is a deliberately small first scope; no live distributed snapshot system.
- **Acceptance tests:** verify all captured identities/hashes before/after restore; original environment remains untouched. Resume a known pending synthetic batch and verify exact source/output IDs, Kafka ranges, previous Welford state, no extra logical Delta/result rows, Registry run/version IDs and unchanged research-only governance. Withhold one required component and demonstrate explicit refusal. Record observed backup age/recovery elapsed time and manual steps without declaring an RPO/RTO target. Separate process restart from clean-environment restoration in the report.
- **Dependencies:** inventory of the selected lineage; storage for a genuinely independent copy; clean environment capable of preserving path/runtime contracts; safe quiescence. Existing inspectors and Phase 29 reconciliation are reused. Do not reset shared volumes.
- **Priority / effort:** **P1 / M**; first and only recommended next implementation milestone.
- **Portfolio value:** high—adds a distinct reliability capability instead of repeating fault injection.
- **Production prerequisite status:** necessary evidence for future operations, insufficient for HA, security or model eligibility. No automatic Phase 31 work is authorized by this document.

## W2 — Narrow publication-durability and writer-ownership audit

- **Problem / current evidence:** Scala file claims and Phase 26 results flush file+directory; Phase 24 multi-file publication does not explicitly fsync; Phase 27 result claim omits parent-directory fsync. MLflow locks protect cooperating local callers only. **NOT_ESTABLISHED:** uniform power-loss/foreign-writer semantics. [Publication comparison](failure-mode-register.md#file-publication-is-not-one-uniform-durability-contract), [Registry writer](../../../model-control-plane/src/sentinelaegisforge_control_plane/registry/run.py).
- **Missing guarantee:** each artifact family states its exact atomicity/durability/ownership boundary and interrupted-claim recovery behavior.
- **Proposed implementation:** inventory commit points; define one explicit local writer policy; make only proven necessary publication fixes with filesystem-specific assumptions and no changes to existing identities. Document how an incomplete claim is preserved and recovered. Consolidate helpers only if semantics genuinely match; do not rewrite every historical manifest abstraction.
- **Acceptance tests:** kill a subprocess at selected pre/post-claim steps; readers must see either a verified complete claim or a diagnosable incomplete/conflicting artifact; retries never overwrite completed evidence. Exercise two cooperating writers and a conflicting claim. Do not call process-kill tests power-cut proof; record unsupported filesystem/crash modes separately.
- **Dependencies:** W1 inventory, existing byte-contract and conflict tests; no distributed lock service.
- **Priority / effort:** **P1 / M**. **Portfolio value:** medium/high if it closes concrete differences. **Production prerequisite:** yes for any claimed durability beyond retained-process recovery; still not replicated storage.

## W3 — Read-only retention and recovery-dependency readiness report

- **Problem / current evidence:** pending work requires historical Delta rows, original Kafka offsets, model bytes and checkpoints; existing checks are bounded/manual, and disk growth is only sampled. **NOT_ESTABLISHED:** current retention ownership and advance warning. [Dependencies](failure-mode-register.md#stateful-upgrades-and-retention-dependencies), [retention inspector](../../../model-control-plane/src/sentinelaegisforge_control_plane/reliability/retention.py), [backlogs](../../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/progress.py).
- **Missing guarantee:** an operator can identify the oldest still-required authority and imminent recovery blockers before deletion/expiry.
- **Proposed implementation:** a read-only report of unresolved source versions/ranges, fresh topic identities/offset bounds, missing files, checkpoint lineage, artifact dependencies and disk free space. Assign an owner for each. Make staleness of captured probes explicit. No automatic VACUUM, Kafka retention change or artifact deletion.
- **Acceptance tests:** retained source passes; removed required version/file/model, stale probe, topic recreation and exhausted configured safety margin refuse readiness. Completed work must not be assumed deletable while audit/feedback dependencies remain. Include future-version/change mismatch tests without rewriting checkpoints.
- **Dependencies:** W1 dependency inventory; fresh admin read permissions; an explicit operational retention policy before cleanup is introduced.
- **Priority / effort:** **P1 / M**. **Portfolio value:** high for explaining replay limits. **Production prerequisite:** required before unattended operation; cleanup policy remains separate.

## W4 — Reproducible CI evidence and supply-chain checks

- **Problem / current evidence:** pinned project dependencies and two CI jobs exist, but Actions/image tags are mutable; no release attestation/security scan or automated real-broker evidence exists. Remote branch protection/runs were uninspected. **NOT_ESTABLISHED:** release safety. [CI assessment](capability-matrix.md#cicd-and-release-maturity), [workflow](../../../.github/workflows/ci.yml).
- **Missing guarantee:** reviewers can tie a tested revision/runtime to immutable dependencies and actual CI outcomes.
- **Proposed implementation:** pin reviewed Actions/images immutably with an update process, document transitive Scala resolution, add narrowly scoped dependency/security review, and retain exact test reports. If affordable, add an explicit opt-in bounded Kafka contract/recovery job, separate from ordinary unit verification. Release packaging remains conditional on a deployment objective.
- **Acceptance tests:** a clean runner executes full Scala tests rather than relying on incremental cache; Python locked sync/lint/types/tests pass; reports identify revision/runtime; deliberate dependency/contract violations produce actionable failures; no credentials on untrusted PRs. Root verification still works with infrastructure stopped.
- **Dependencies:** repository-host permissions and vulnerability-triage ownership; no need for a deployment platform.
- **Priority / effort:** **P1 / S–M**. **Portfolio value:** high independent reproducibility. **Production prerequisite:** part of release assurance, not proof of runtime security or rollback.

## W5 — Bounded continuous research operation, only after an explicit need

- **Problem / current evidence:** AvailableNow/Make invocations prove controlled batches; no unattended scheduler, health age, graceful shutdown/restart supervisor or model-call deadline exists. **NOT_ESTABLISHED:** continuous operation. [Applications](../../../streaming-engine/src/main/scala/io/sentinelaegisforge/streaming/processing), [worker](../../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py), [operations guide](../../architecture/incremental-shadow-operations.md).
- **Missing guarantee:** a non-enforcing research loop stays bounded, diagnosable and recoverable without an operator at every invocation.
- **Proposed implementation:** first specify process ownership, termination, bounded retry/backoff, configuration identity, stage health/freshness and research-only isolation. Use existing processes/contracts; do not start with Kubernetes, a workflow engine or microservices. Add supervision only for a justified long-running research use case.
- **Acceptance tests:** controlled SIGTERM/restart, broker outage, stuck/corrupt model, lost dependency and backlog limit; no successful incomplete claim, no silent input skip, no payment interaction. Observe progression while unattended and restore after failure. Latency/deadline choices need separate measured evidence.
- **Dependencies:** W1–W3, explicit operator/operating scope; W6 if shared/exposed. **Priority / effort:** **Deferred / L** today. **Portfolio value:** conditional, lower than restore proof. **Production prerequisite:** a research continuous loop still does not satisfy production inference gates.

## W6 — Data and service authorization boundary before shared/real-data use

- **Problem / current evidence:** loopback containment with plaintext Kafka/Registry, local filesystem access and colocated hashes. Provider names are unauthenticated. **NOT_ESTABLISHED:** confidentiality/authorization. [Security controls S01–S13](architecture-threat-model.md#security-control-review), [feedback source](capability-matrix.md#independent-outcome-source).
- **Missing guarantee:** only authorized principals can publish/read/administer data, supply outcomes or replace artifacts.
- **Proposed implementation:** define data ownership and threat model for the chosen environment; least-privilege service identities, encrypted authenticated transport, topic/group/schema authorization, restricted storage/model/Registry access, secret acquisition/rotation, audit and retention/privacy controls. Keep provisioning/admin separate from runtime identities. Do not configure real credentials in this assessment.
- **Acceptance tests:** unauthorized read/write/admin/model replacement/provider submissions denied; authorized workflows still pass; credential revocation and audit work; logs/artifacts exclude secrets. Obtain appropriate privacy/use authorization rather than claiming certification from tests.
- **Dependencies:** actual deployment/data-source choice and accountable owners; W1 for protected recovery copies. **Priority / effort:** **conditional P0 / L**. **Portfolio value:** useful only if demonstrating a concrete boundary. **Production prerequisite:** yes before external users or real sensitive data; no reason to claim current research exposure meets it.

## W7 — Independent outcome quality and accountable model evaluation

- **Problem / current evidence:** VAL004–010 remain NOT_ESTABLISHED; current synthetic model is not eligible. [Ten requirements](capability-matrix.md#model-readiness--all-ten-phase-18-requirements), [policy](../../../model-control-plane/src/sentinelaegisforge_control_plane/validation/policy.py).
- **Missing guarantee:** authenticated representative fraud labels, generalization, useful discrimination/calibration/segments, operational evidence and business accountability.
- **Proposed implementation:** only with an authorized outcome source, design maturation/revision/retraction/selection-bias audits and representative out-of-time evaluation; establish permitted use, independent approval and operational responsibilities. Reuse existing immutable evaluation machinery. Do not tune this synthetic corpus to manufacture a passing score or silently amend Phase 18.
- **Acceptance tests:** real source authenticity and receipt-time evidence, complete supported evaluation populations, all ten requirements independently evidenced under a reviewed policy, and documented responsible approval. Even that requires a separately authorized promotion process; this work item does not grant promotion.
- **Dependencies:** genuine data access, domain partners, W6 and deployment-specific W1–W5 evidence. **Priority / effort:** **conditional P0 / L with external dependencies**; deferred for current portfolio. **Portfolio value:** not required to demonstrate systems expertise. **Production prerequisite:** mandatory before any model influences customer decisions.

## W8 — One inspectable portfolio walkthrough

- **Problem / current evidence:** 805-line Makefile, 885-line historical state file and 43 ADRs scatter the strongest evidence; ignored dossiers are absent in a fresh clone. **NOT_ESTABLISHED:** easy independent onboarding. [Presentation review](portfolio-readiness.md#technical-debt-and-evidence-accessibility).
- **Missing guarantee:** a reviewer can find the architecture, one failure/recovery proof, model restriction and evidence caveats without reading 29 phase prompts.
- **Proposed implementation:** a short navigation/walkthrough page linking existing contracts and safe inspection commands; explain prerequisites and which artifacts require explicit regeneration. Provide small non-sensitive evidence excerpts only under a deliberate publishing policy; preserve full histories and never commit `.local` corpora/models by default.
- **Acceptance tests:** another reader can follow links, identify exact hashes/lineages and distinguish source proof from unavailable local evidence; read-only steps do not start infrastructure, mutate governance or require private credentials. Any optional live path explicitly identifies writes/resources.
- **Dependencies:** this assessment, retained evidence availability. **Priority / effort:** **P2 / S**. **Portfolio value:** very high per unit effort. **Production prerequisite:** no. This is presentation alongside the chosen milestone, not a second implementation program.

## Ordered horizons

1. **Immediate:** use this assessment to present the existing portfolio (W8). Preserve research-only scope. Approve or decline W1 as a separate task; this phase implements none of it.
2. **Next engineering milestone:** W1 only. Its result determines whether W2/W3 need narrowly scoped corrections; do not preemptively build their entire backlog.
3. **Later local hardening if useful:** W2/W3 in dependency order; W4 for independent verification. Stop once evidence is understandable and recoverable.
4. **Conditional deployment horizon:** choose a deployment purpose first. W6 gates real data/exposure; W5 needs W1–W3 plus operational ownership. W7 gates any claim beyond synthetic research. These are dependencies, not promised future phases.

## Bounded experiments worth considering

**Proposals only; none run in Phase 30.** [Phase 29 observations](capability-matrix.md#capacity-evidence--local-bounded-observations) support these questions, not tuning conclusions. Each uses fresh disposable checkpoints/app IDs and a fixed plan; no modifications to retained evidence or production semantics.

| Experiment / hypothesis | Controlled variable; held constant | Measurements | Correctness guardrails and stop conditions | Decision enabled |
|---|---|---|---|---|
| E1 Fixed workload, fewer shuffle partitions might reduce local overhead | Only `spark.sql.shuffle.partitions` across fresh lineages; same runtime, exact plan/waves, CPUs, state schema, watermark, model, filesystem and output semantics | Full invocation and trigger durations, state rows/memory, checkpoint logical bytes/files; repeated trials with order recorded | Exact IDs/feature values/drop counters and bounded resources unchanged. Stop on state incompatibility, semantic divergence, disk/memory cap or preset small trial count | Whether a **local-only** setting merits a separately reviewed configuration change. Never reuse checkpoints across changed partition/state topology. |
| E2 Process startup may dominate a short bounded wave | New process per wave versus one deliberately scoped long-lived query; same events, batch boundaries where controllable, hardware/runtime and partitions | Startup/teardown measured explicitly, trigger duration, restore time, correctness and shutdown/restart behavior | No wall-clock-derived feature changes; compare watermark/state at equivalent committed boundaries. Stop on inability to align semantics, leak, stalled shutdown or resource cap | Whether continuous research work adds enough value to justify W5; no production p99 claim. |
| E3 Active-key growth increases retained state/storage demand | One variable: customer-currency key cardinality within existing corpus limits; fixed event total/time horizon and other settings as far as possible, document remaining distribution differences | Rows/keys, memory metrics, logical Delta/checkpoint bytes/file distribution, recovery duration | Preserve zero unexplained loss and expected currency/expiry semantics. Stop before documented local event/disk/memory limits or restore failure | Set safe local experiment bounds and prioritize retention/backup; not extrapolate enterprise capacity. |
| E4 Model-load amortization and bounded backlog drain | One variable: supported inference items per tick; fixed accepted inbox/model/runtime/source versions and one writer | Invocation/model-load time, processed count and sampled backlog age/drain; do not combine source throughput with inference | Same logical results/probabilities; no evaluation-window expansion. Stop on configured backlog/resource cap, missing source or mismatch | Choose a measured local tick bound if needed; no upstream admission-control claim. |

RPO/RTO and host restoration belong to W1, not a throughput benchmark. Reject open-ended load generation, synthetic-score optimization, unmeasured RocksDB/GC tuning and cloud sizing from three samples.

## Research worker versus production inference

| Option | Present evidence / prerequisites | Complexity and recommendation |
|---|---|---|
| A — Retain bounded offline research worker | **T/L:** receipts → verified source/candidate → durable logical results; no payment effects [worker](../../../model-control-plane/src/sentinelaegisforge_control_plane/incremental_shadow/worker.py) | Lowest operating burden; **retain now**. W1 improves recovery without a serving architecture. |
| B — Isolated continuous research-only worker | **N:** supervisor/deadlines/continuous telemetry; W1–W3 and W5, W6 for shared use | Moderate-to-high new operational responsibility. Worth considering only for a specific continuous research question; governance must remain research-only. |
| C — Production inference service | **N:** quality, security, HA, deployment/rollback, deadlines, support and approval; W6/W7 plus substantial operational evidence | High; **not justified now**. A low-latency API cannot cure missing real-label quality or authorize this model. |

## Deployment directions — no more than three

| Direction | Effort / cost drivers | Recovery and security | Operating responsibility / portfolio value |
|---|---|---|---|
| Local reproducible portfolio stack | **L current; M for W1.** Existing machine, Docker disk, retained evidence and developer time; no vendor price assumed | Single-host risk; independent consistent backup rehearsal, loopback containment and trusted synthetic inputs | Owner runs bounded commands and reviews failures. Highest current value: show boundaries and evidence without pretending HA. |
| Small cloud research platform | **N; L effort.** Broker/storage/compute uptime, state/checkpoint I/O, retained objects, logs, egress and identity administration | Selected managed or self-managed components need tested connectors/storage semantics, IAM/network controls, backups and restore. No cloud deployment currently exists | Named operator, budgets, patching, incident response and non-enforcement isolation. Useful only for a concrete shared-access or remote-recovery objective; managed services can reduce but not eliminate responsibility. |
| Enterprise distributed deployment | **N; L+ cross-team effort.** Replicas/quorum, multi-failure-domain capacity, private networking, audit retention, release/support infrastructure | Redundant Kafka and durable storage alone do not establish end-to-end recovery; state migration, tested disaster plan, authorized data/service boundaries and release rollback required | Platform/security/data/model/business owners and service objectives. Low incremental portfolio value relative to cost without real users and outcome partners. No automatic Kubernetes/microservices recommendation. |

## Explicitly defer

Keep model promotion/champion automation, production serving, payment actions, real-time retraining, distributed-lock services, external feature stores, Kubernetes, multi-cloud and elaborate observability infrastructure out until a defined problem justifies them. A metrics collector may eventually be useful; a stack of dashboards is not a substitute for ownership, retention or restoration. No schema, feature, model, threshold, dependency, topology or runtime tuning changes are proposed as Phase 30 implementation.
