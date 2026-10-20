# SentinelAegisForge model control plane

This Python 3.13 package builds reproducible offline snapshots from the corrected
`transaction_statistical_features_v2` Delta table and independent delayed JSONL labels.
Phase 13 adds one local pandas/sklearn logistic baseline over those verified snapshots.
Phase 14 can consume independent delayed synthetic labels after the Scala-generated
transaction plan has passed through the real Kafka/Spark/Delta feature lineage.
Phase 15 indexes already verified immutable training runs in local SQLite-backed MLflow.
Phase 16 reconstructs the fixed Pipeline from those authoritative artifacts, verifies
archived prediction equivalence, and publishes a technically verified offline-only
MLflow/skops candidate. There is no production approval or serving endpoint.
Phase 17 catalogues verified candidates in the existing SQLite-backed MLflow Registry
as research-only versions; no champion, promotion or deployment is created.
Phase 18 creates immutable evidence dossiers for exact research-only Registry versions;
synthetic results leave promotion readiness `NOT_ELIGIBLE_FOR_PROMOTION`.
Phase 19 freezes label-free offline predictions from a fixed corrected Delta version,
then evaluates independent synthetic outcomes at explicit observation cutoffs. It does
not rescore on label revision or establish historical online model availability.

From the repository root:

```sh
make build-ml-dataset \
  ML_FEATURES_DELTA_PATH=/absolute/path/transaction_statistical_features_v2 \
  ML_LABELS_PATH=/absolute/path/labels.jsonl \
  ML_AS_OF_TIME=2030-01-10T00:00:00Z
make inspect-ml-dataset ML_DATASET_SNAPSHOT_PATH=/absolute/path/<snapshot_id>
make verify-python
```

Train from an already verified, sufficiently labeled snapshot with explicit UTC bounds:

```sh
make train-baseline-model \
  BASELINE_DATASET_SNAPSHOT_PATH=/absolute/path/to/<snapshot_id> \
  BASELINE_TRAIN_END=2030-01-03T00:00:00Z \
  BASELINE_VALIDATION_END=2030-01-05T00:00:00Z \
  BASELINE_TEST_END=2030-01-07T00:00:00Z
make inspect-baseline-run BASELINE_RUN_PATH=/absolute/path/to/<training_run_id>
```

Track that existing run without fitting again; the default local store is under the
repository's ignored `.local/mlflow/` directory:

```sh
make track-baseline-run BASELINE_RUN_PATH=/absolute/path/to/<training_run_id>
make inspect-tracked-run MLFLOW_RUN_ID=<provider-run-id>
make compare-tracked-runs
```

`make mlflow-ui` is an optional localhost-only inspection UI. See the
[tracking contract](../docs/architecture/mlflow-experiment-tracking.md) for
source validation, explicit corpus linkage and retry/conflict behavior.

Reconstruct an offline candidate from an existing run; the optional MLflow ID is
checked read-only and does not create a tracking run:

```sh
make build-model-candidate BASELINE_RUN_PATH=/absolute/path/to/<training_run_id> MLFLOW_RUN_ID=<provider-run-id>
make inspect-model-candidate MODEL_CANDIDATE_PATH=/absolute/path/to/<candidate_id>
make score-model-candidate MODEL_CANDIDATE_PATH=/absolute/path/to/<candidate_id> MODEL_INPUT_JSON=/absolute/path/to/one-row.json
```

The score JSON has exactly 11 feature keys, with exact monetary Decimals encoded as
strings. The output is an uncalibrated offline FRAUD probability estimate, not a
decision. See the [candidate contract](../docs/architecture/model-candidate-artifact.md).

Register and inspect an explicit research-only version without fitting again:

```sh
make register-model-candidate MODEL_CANDIDATE_PATH=/absolute/path/to/<candidate_id>
make inspect-registered-candidate REGISTERED_MODEL_VERSION=<explicit-version>
make verify-registered-model-artifact REGISTERED_MODEL_VERSION=<explicit-version>
```

The Registry references the existing local candidate bytes; it does not make an
independent artifact backup. See [Registry governance](../docs/architecture/model-registry-governance.md).

Assess, inspect or compare immutable validation evidence without Registry mutation:

```sh
make assess-registered-model REGISTERED_MODEL_VERSION=<explicit-version>
make inspect-model-assessment MODEL_ASSESSMENT_PATH=/absolute/path/to/<assessment_id>
```

See [model validation governance](../docs/architecture/model-validation-governance.md).

Use `make build-prediction-ledger` with an exact Registry version, Delta path/version
and UTC scoring window. Then use `make evaluate-delayed-outcomes` with that ledger,
the verified scenario corpus and an explicit UTC cutoff. `make inspect-prediction-ledger`,
`make inspect-monitoring-snapshot` and `make compare-monitoring-snapshots` are read-only.
See [offline delayed-outcome monitoring](../docs/architecture/offline-delayed-outcome-monitoring.md).

Phase 20 compares two non-overlapping UTC windows within one verified prediction ledger, measuring the unchanged 11 model inputs and frozen scores without labels or inference. Use the root `make build-drift-snapshot`, `make inspect-drift-snapshot` and `make compare-drift-snapshots` targets with explicit paths/windows. See [offline distribution monitoring](../docs/architecture/offline-distribution-monitoring.md). This is synthetic-only descriptive evidence, not an alert or production-performance claim.

Phase 21 verifies a selected drift snapshot and optionally compatible T1/T2 delayed-outcome snapshots and the Phase 18 dossier, then writes an immutable investigation packet and manual runbook. Use the root `make build-monitoring-investigation` and read-only `make inspect-monitoring-investigation` targets with explicit paths. See [offline monitoring investigation](../docs/architecture/offline-monitoring-investigation.md). It makes no incident, retraining, promotion or deployment decision.

Phase 22 adds a separate source-neutral local feedback envelope, immutable complete provider-case revision ledgers and cumulative-receipt AS_OF resolution. Confirmed states can be evaluated against the same frozen Phase 19 prediction ledger in a new snapshot contract. The local provider-like fixtures are synthetic and do not establish real-world label quality. See [durable outcome feedback](../docs/architecture/durable-outcome-feedback.md) and the root `generate-local-feedback-fixture`, `build-feedback-ledger`, `resolve-feedback-as-of` and `evaluate-feedback-outcomes` targets. Old Phase 12/19 artifacts remain unchanged.

Phase 23 adds an explicit, bounded local replay plan over a fixed corrected Delta version and research-only Registry version. Its request lifecycle and immutable terminal-result ledger are independent of the Phase 19 batch scorer; parity compares successful results with the frozen Phase 19 ledger. See [controlled shadow inference](../docs/architecture/controlled-shadow-inference.md) and the root `build-shadow-replay-plan`, `execute-shadow-replay` and `verify-shadow-parity` targets. This is not a live model endpoint or payment action.

Phase 24 independently discovers committed feature-row first-visible Delta versions, publishes immutable commit segments and an outbox, and records idempotent durable receipts through a bounded local research-only handoff. It uses the existing strict 25-column validator and no model scorer. See [feature availability handoff](../docs/architecture/feature-availability-handoff.md) and the root `discover-feature-commits`, `build-feature-ready-outbox`, `handoff-ready-events` and `verify-feature-handoff` targets. Historical reconstruction is not an original feature-ready wall-clock timestamp or production Kafka delivery.

The stdlib CLI is `python -m sentinelaegisforge_control_plane.datasets.cli build|inspect`.
`--source-version` / `ML_SOURCE_DELTA_VERSION` pins a historical Delta version; otherwise
the latest version is captured once and recorded. Output defaults to the root's ignored
`.local/ml/datasets/transaction-fraud-v1/` area.

FRAUD/LEGIT labels must have UTC observed-at timestamps, nonblank provenance and complete
revision histories. Unlabeled rows are excluded; risk CLEAR/REVIEW decisions are never labels.
Existing snapshots are verified, not overwritten. See the
[offline dataset contract](../docs/architecture/offline-ml-dataset.md),
[temporal baseline](../docs/architecture/offline-ml-baseline.md),
[synthetic corpus workflow](../docs/architecture/synthetic-fraud-corpus.md),
[ADRs](../docs/adr/README.md) and [project state](../PROJECT_STATE.md).
