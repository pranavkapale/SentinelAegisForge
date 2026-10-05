# SentinelAegisForge model control plane

This Python 3.13 package builds reproducible offline snapshots from the corrected
`transaction_statistical_features_v2` Delta table and independent delayed JSONL labels.
It uses delta-rs and Arrow, not PySpark or pandas. No model, training or serving exists.

From the repository root:

```sh
make build-ml-dataset \
  ML_FEATURES_DELTA_PATH=/absolute/path/transaction_statistical_features_v2 \
  ML_LABELS_PATH=/absolute/path/labels.jsonl \
  ML_AS_OF_TIME=2030-01-10T00:00:00Z
make inspect-ml-dataset ML_DATASET_SNAPSHOT_PATH=/absolute/path/<snapshot_id>
make verify-python
```

The stdlib CLI is `python -m sentinelaegisforge_control_plane.datasets.cli build|inspect`.
`--source-version` / `ML_SOURCE_DELTA_VERSION` pins a historical Delta version; otherwise
the latest version is captured once and recorded. Output defaults to the root's ignored
`.local/ml/datasets/transaction-fraud-v1/` area.

FRAUD/LEGIT labels must have UTC observed-at timestamps, nonblank provenance and complete
revision histories. Unlabeled rows are excluded; risk CLEAR/REVIEW decisions are never labels.
Existing snapshots are verified, not overwritten. See the
[offline dataset contract](../docs/architecture/offline-ml-dataset.md),
[ADRs](../docs/adr/README.md) and [project state](../PROJECT_STATE.md).
