# Portfolio review and reproduction guide

[Project overview](../../README.md) · [architecture and event trace](architecture-walkthrough.md) · [correctness](correctness-and-recovery.md) · [evidence index](evidence-index.md) · [interview guide](interview-defense.md)

## Choose an entry point

- **Recruiter / hiring manager, three minutes:** the root overview, six engineering highlights and selected results. The contribution is stateful data correctness and auditable recovery; this is not a deployed fraud-prevention product.
- **Senior Data Engineer, 10–15 minutes:** follow the recorded event, then inspect the rolling/Welford state distinction and Delta/Kafka failure windows.
- **Staff / Principal Engineer:** use the evidence index to jump directly to implementation, failure tests, ADRs and recorded outcomes. Finish with the Phase 30 security/operations gaps and Phase 31 restore boundary.

The five portfolio documents are current navigation, not replacement evidence. Historical ADRs, phase reports and immutable local artifacts retain their original meaning.

## Reproducibility levels

| Level | What a reviewer can establish | Required inputs and limits |
|---|---|---|
| **1 — Code and tests** | Read source/contracts, run ordinary module/root checks, exercise temporary local Spark/Delta and SQLite fixtures | Clone, JDK 21, sbt 2.0.9, Python 3.13 (verified 3.13.9), uv, Make and dependency downloads/cache. No broker, Docker or model evidence from `.local` required. |
| **2 — Fresh local integration** | Generate a new bounded synthetic population, publish through Kafka/Registry, materialize four real Scala Delta stages and reconcile event IDs | Docker daemon, local disk/memory for broker + JVM state + Delta/checkpoints; fresh fences, paths and app IDs. The historical full shadow harness additionally requires its preserved candidate and exact Registry/source provenance (Level 3 inputs). A clone does not supply these bytes. |
| **3 — Historical evidence reproduction** | Reinspect the recorded trace/dossiers or repeat a deliberately authorized retained-lineage operation | Original compatible runtime, model/data/manifests, Registry DB, logical absolute paths and required Kafka/Delta/checkpoint histories. A fresh new run cannot recreate the historical broker coordinates/provider IDs by naming directories similarly. |

**Repository-verifiable:** committed code, tests, ADRs and documentation. **Locally reproducible:** supported tests and bounded integration commands. **Retained local evidence:** ignored `.local/` manifests, Delta tables, model artifacts, Kafka volumes and `backup_isolated_storage/` bundles. The last category is absent from Git. Portfolio paths to it are code text, not GitHub download links.

The historical Phase 30 assessment contains explicitly labeled local-only links. Read its committed findings without those files; absence of an ignored artifact does not make its claimed outcome independently verified in your checkout. No raw labels, private truth, model binaries or broker archives are published by this portfolio.

## Level 1: safe verification

From the repository root, select a session-local JDK 21 and put `java`, `sbt`, `uv` and `make` on PATH. If using SDKMAN, inspect `sdk current java` and select an already installed JDK 21 for that shell. The repository's Scala and Python pins are authoritative; do not substitute a newer JDK as evidence for JDK 21.

```sh
java -version
make verify-python
make verify-scala
make verify
```

`verify-python` runs locked uv sync, pytest, Ruff lint/format and mypy. `verify-scala` runs Scalafmt, compilation and tests. Full Scala re-execution, useful when sbt's incremental test cache is warm:

```sh
cd streaming-engine
sbt "Test / testOnly *"
```

These checks write only normal build/cache and isolated test artifacts. They do not start Kafka, Schema Registry or MLflow UI. See the [actual Makefile](../../Makefile), [CI](../../.github/workflows/ci.yml) and [Python module guide](../../model-control-plane/README.md). Report fresh results separately from the historical 327 Python / 122 Scala baseline.

## Level 2: bounded integration preparation

This is an explicit data-producing workflow, not part of quick verification. Follow the [Phase 28 integration contract](../architecture/stateful-streaming-shadow-integration.md), [scenario guide](../architecture/synthetic-fraud-corpus.md) and [incremental research guide](../architecture/incremental-shadow-operations.md). Use absolute paths because Make enters module directories.

1. Inspect available disk/memory and Docker's allocation. Spark currently uses `local[*]`; the recorded runs used 200 shuffle/state-store instances without tuning. The corpus is bounded, but checkpoints, broker history and evidence accumulate. There is no documented minimum-memory guarantee or automatic safe cleanup. Do not infer capacity from the 30-event restore bundle size.
2. Start and verify Kafka/Registry, then explicitly provision the separate research topic if using it:

   ```sh
   make infra-up
   make infra-status
   make verify-infra
   make provision-feature-ready-topic
   ```

   These commands create/reuse persistent broker data, topics and schema metadata. The setup is local RF 1 and plaintext; host endpoints are loopback-bound. Keep other producers idle during a fenced experiment so unrelated new events cannot enter it.
3. Generate and inspect a fresh plan before publication. Example small population (not a training-sufficiency claim):

   ```sh
   make generate-synthetic-corpus SCENARIO_SEED=32001 \
     SCENARIO_BASE_TIME=2030-01-04T00:00:00Z SCENARIO_COUNT=30 \
     SCENARIO_CUSTOMERS=3 SCENARIO_HORIZON_DAYS=3 \
     SCENARIO_OUTPUT_ROOT="$PWD/.local/portfolio-new/corpora"
   # Set CORPUS_PATH to the absolute content-addressed path printed above.
   make inspect-synthetic-corpus SCENARIO_CORPUS_PATH="$CORPUS_PATH"
   ```

4. Capture a **fresh** transaction fence, initialize a fresh integration, and prime its latest-offset checkpoint **before** publishing. This protects against old records in the shared topic; it does not isolate concurrent producers.

   ```sh
   make capture-phase28-transaction-fence \
     PHASE28_TRANSACTION_FENCE="$PWD/.local/portfolio-new/transaction-fence.json"
   make initialize-stateful-shadow-integration \
     PHASE28_CORPUS_PATH="$CORPUS_PATH" \
     PHASE28_TRANSACTION_FENCE="$PWD/.local/portfolio-new/transaction-fence.json" \
     PHASE28_ROOT="$PWD/.local/portfolio-new/integrations"
   # Set INTEGRATION_PATH to the absolute path printed by initialization.
   make prime-stateful-shadow-ingestion PHASE28_INTEGRATION_PATH="$INTEGRATION_PATH"
   make run-stateful-shadow-wave PHASE28_INTEGRATION_PATH="$INTEGRATION_PATH" PHASE28_WAVE=0
   make reconcile-stateful-shadow-wave PHASE28_INTEGRATION_PATH="$INTEGRATION_PATH" PHASE28_WAVE=0
   ```

   The integration manifest owns four separate output/checkpoint paths and stable transaction application IDs. Each successful wave creates persistent Delta/state/publication evidence. Continue waves 1 and 2 in order, completing each stage and reconciliation before the next; never republish an already successful wave to retry downstream work. The three chronological waves respect the existing ten-minute local watermark policy.
5. For the **optional research branch**, retain the candidate/training/snapshot/MLflow Registry dependencies first. The existing harness expects its pinned research candidate, not an arbitrary downloaded model. Initialize a fresh operational group/end-offset fence using `initialize-incremental-shadow` against the now-existing statistical table, with explicit `INCREMENTAL_SOURCE_DELTA_PATH`, `INCREMENTAL_ROOT`, `INCREMENTAL_FENCE_PATH`, `INCREMENTAL_GROUP` matching the integration manifest, and exact `INCREMENTAL_MODEL_VERSION`. Record the returned stream path. Run `handoff-stateful-shadow-wave` with `PHASE28_INTEGRATION_PATH`, `PHASE28_STREAM_PATH` and `PHASE28_WAVE` after each committed wave. `verify-stateful-shadow-integration` requires those two paths and checks full membership. The Phase 28/29 harness binds the preserved candidate ID in its contract. An independently trained new candidate is not a drop-in replacement. A fresh clone can run the transaction-to-feature steps above and use the Phase 12–17 guides for separate research preparation, but this exact full shadow demonstration requires the retained compatible candidate chain (Level 3). Do not fabricate feature rows or override candidate identity.
6. Normal shutdown is non-destructive:

   ```sh
   make infra-down
   ```

   Kafka data and `.local` artifacts remain. `infra-reset` destroys shared broker data and is **not** a quickstart/cleanup step. Deleting checkpoints or changing app IDs is a lineage change, not ordinary restart. Historical restore exercises have their own [explicit procedure](../architecture/recovery-bundle-and-restore.md).

## Developer command map

The [Makefile](../../Makefile) remains the command authority. The guides below retain detailed parameters; this index replaces the old root README's long catalog without changing interfaces.

| Operation | Existing targets (explicit inputs required) | Guide |
|---|---|---|
| Small source/ingestion inspection | `produce-sample`, `consume-sample`, `ingest-delta`, `inspect-delta` | [Producer](../architecture/transaction-producer.md), [Delta](../architecture/delta-ingestion.md) |
| Semantic feature stages | `deduplicate-transactions`, `process-rolling-features`, `process-statistical-features` | [Dedup](../architecture/event-time-deduplication.md), [rolling](../architecture/customer-rolling-features.md), [statistics](../architecture/customer-statistical-features.md) |
| Separate diagnostic/advisory branches | `process-customer-state`, `process-risk-decisions` | [Activity](../architecture/customer-stateful-processing.md), [advisory rules](../architecture/deterministic-risk-engine.md) |
| Immutable research preparation | `build-ml-dataset`, `train-baseline-model`, `track-baseline-run`, `build-model-candidate` | [Dataset](../architecture/offline-ml-dataset.md), [baseline](../architecture/offline-ml-baseline.md), [tracking](../architecture/mlflow-experiment-tracking.md), [candidate](../architecture/model-candidate-artifact.md) |
| Catalog and assess, without promotion | `register-model-candidate`, `inspect-registered-candidate`, `assess-registered-model` | [Registry](../architecture/model-registry-governance.md), [validation](../architecture/model-validation-governance.md) |
| Inspect/evaluate frozen outcomes | `build-prediction-ledger`, `evaluate-delayed-outcomes`, `build-drift-snapshot`, `build-monitoring-investigation`, `build-feedback-ledger` | [Delayed outcomes](../architecture/offline-delayed-outcome-monitoring.md), [drift](../architecture/offline-distribution-monitoring.md), [triage](../architecture/offline-monitoring-investigation.md), [feedback](../architecture/durable-outcome-feedback.md) |
| Replay / commit-aware handoff | `execute-shadow-replay`, `build-feature-ready-outbox`, `publish-feature-ready-kafka`, `execute-feature-ready-shadow` | [Replay](../architecture/controlled-shadow-inference.md), [outbox](../architecture/feature-availability-handoff.md), [Kafka](../architecture/feature-ready-kafka-transport.md), [shadow](../architecture/kafka-research-shadow-inference.md) |
| Bounded operations / fault evidence | `inspect-incremental-shadow`, `inspect-reliability-dossier`, `inspect-recovery-rehearsal` | [Operations](../architecture/incremental-shadow-operations.md), [reliability](../architecture/stateful-streaming-reliability.md), [restore](../architecture/recovery-bundle-and-restore.md) |

Build/publish/process/execute/register/evaluate commands write durable artifacts or external local state. Inspect commands read retained evidence; they cannot recover missing source bytes. Optional `make mlflow-ui` starts only a localhost inspection UI and is unnecessary for headless verification.
