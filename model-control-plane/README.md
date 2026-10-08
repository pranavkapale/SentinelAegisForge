# SentinelAegisForge model control plane

This Python 3.13 package builds reproducible offline snapshots from the corrected
`transaction_statistical_features_v2` Delta table and independent delayed JSONL labels.
Phase 13 adds one local pandas/sklearn logistic baseline over those verified snapshots.
Phase 14 can consume independent delayed synthetic labels after the Scala-generated
transaction plan has passed through the real Kafka/Spark/Delta feature lineage.
It does not create a deployable model or serving endpoint.

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
