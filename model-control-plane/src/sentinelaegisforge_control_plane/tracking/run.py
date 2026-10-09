"""Track verified immutable training runs; never fit, score or serialize a model."""

import fcntl
import hashlib
import json
import math
import tempfile
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq
from mlflow.entities import Run, ViewType
from mlflow.tracking import MlflowClient

from sentinelaegisforge_control_plane.datasets.dataset_builder import inspect_snapshot
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.synthetic.corpus import inspect_corpus
from sentinelaegisforge_control_plane.synthetic.reconcile import reconcile
from sentinelaegisforge_control_plane.training.baseline import ESTIMATOR_CLASS
from sentinelaegisforge_control_plane.training.run import inspect_run

DEFAULT_EXPERIMENT = "sentinelaegisforge-synthetic-fraud-baseline"
REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
TRACKING_CONTRACT_VERSION = "verified-training-run-mlflow-v1"
ARTIFACT_NAMES = frozenset({"manifest.json", "metrics.json", "provenance.json"})
METRIC_NAMES = (
    "average_precision",
    "roc_auc",
    "precision_at_0_5",
    "recall_at_0_5",
    "f1_at_0_5",
    "rows",
    "positives",
    "negatives",
    "positive_prevalence",
)
CONFUSION_NAMES = ("tp", "fp", "tn", "fn")


class TrackingError(ValueError):
    """Verified training evidence and its MLflow index disagree."""


@dataclass(frozen=True)
class TrackingConfig:
    database: Path = REPOSITORY_ROOT / ".local/mlflow/tracking.db"
    artifacts: Path = REPOSITORY_ROOT / ".local/mlflow/artifacts"
    experiment_name: str = DEFAULT_EXPERIMENT

    def __post_init__(self) -> None:
        if not self.experiment_name.strip():
            raise TrackingError("Experiment name must not be blank")
        if not self.database.is_absolute() or not self.artifacts.is_absolute():
            raise TrackingError("Tracking database and artifact root must be absolute paths")
        if self.database.suffix != ".db":
            raise TrackingError("Tracking database must be an explicit .db path")
        if self.database.resolve() == self.artifacts.resolve():
            raise TrackingError("Tracking database and artifact root must differ")

    @property
    def tracking_uri(self) -> str:
        # sqlite:////absolute/path; never dependent on the caller's working directory.
        return f"sqlite:///{self.database.resolve().as_posix()}"

    @property
    def artifact_uri(self) -> str:
        return self.artifacts.resolve().as_uri()


@dataclass(frozen=True)
class CorpusLineage:
    """Explicit Phase 14 paths; no corpus identity is inferred from directory names."""

    corpus: Path
    validated: Path
    deduplicated: Path
    rolling: Path
    statistical: Path


@dataclass(frozen=True)
class VerifiedEvidence:
    source: Path
    manifest: dict[str, object]
    metrics: dict[str, object]
    params: dict[str, str]
    tags: dict[str, str]
    metric_values: dict[str, float]
    manifest_bytes: bytes
    metrics_bytes: bytes
    provenance: bytes


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise TrackingError(f"{name} must be a JSON object")
    return cast(dict[str, object], value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TrackingError(f"{name} must be a nonempty string")
    return value


def _parameter(value: object) -> str:
    if isinstance(value, str):
        return value
    return canonical_json(value).decode("utf-8").strip()


def _metrics(raw: dict[str, object]) -> dict[str, float]:
    values: dict[str, float] = {}
    if set(raw) != {"validation", "test"}:
        raise TrackingError("Metrics must have validation and test cohorts")
    for split in ("validation", "test"):
        cohort = _object(raw[split], f"{split} metrics")
        confusion = _object(cohort.get("confusion_matrix"), f"{split} confusion matrix")
        if set(cohort) != {*METRIC_NAMES, "confusion_matrix"} or set(confusion) != set(
            CONFUSION_NAMES
        ):
            raise TrackingError(f"Unexpected {split} metric structure")
        for key in METRIC_NAMES:
            value = cohort[key]
            if value is None and key in {"roc_auc", "precision_at_0_5"}:
                continue  # A genuinely undefined metric is absent, never replaced by zero.
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise TrackingError(f"Invalid {split}.{key}")
            if key in {"rows", "positives", "negatives"}:
                if type(value) is not int or value < 0:
                    raise TrackingError(f"Invalid {split}.{key} support")
            elif not 0 <= value <= 1:
                raise TrackingError(f"Out-of-range {split}.{key}")
            values[f"{split}.{key}"] = float(value)
        for key in CONFUSION_NAMES:
            value = confusion[key]
            if type(value) is not int or value < 0:
                raise TrackingError(f"Invalid {split} confusion count: {key}")
            values[f"{split}.confusion.{key}"] = float(value)
        if (
            values[f"{split}.rows"] != values[f"{split}.positives"] + values[f"{split}.negatives"]
            or values[f"{split}.confusion.tp"] + values[f"{split}.confusion.fn"]
            != values[f"{split}.positives"]
            or values[f"{split}.confusion.tn"] + values[f"{split}.confusion.fp"]
            != values[f"{split}.negatives"]
        ):
            raise TrackingError(f"{split} support and confusion counts disagree")
    return values


def verify_evidence(source: Path, corpus_lineage: CorpusLineage | None = None) -> VerifiedEvidence:
    """Complete all source validation before any tracking database mutation."""
    manifest = inspect_run(source)
    source = source.resolve(strict=True)
    manifest_bytes = (source / "manifest.json").read_bytes()
    metrics_bytes = (source / "metrics.json").read_bytes()
    if (
        json.loads(manifest_bytes) != manifest
        or _sha256(metrics_bytes) != manifest["metrics_sha256"]
    ):
        raise TrackingError("Training source changed during tracking inspection")
    snapshot_path = Path(_string(manifest["dataset_snapshot_path"], "snapshot path"))
    snapshot = inspect_snapshot(snapshot_path)
    if manifest["estimator_class"] != ESTIMATOR_CLASS:
        raise TrackingError("Only the verified Phase 13 logistic baseline may be tracked")
    label_sources = pq.read_table(snapshot_path / "dataset.parquet", columns=["label_source"])[
        "label_source"
    ].to_pylist()
    if not label_sources or any(
        not isinstance(value, str) or not value.startswith("SYNTHETIC_") for value in label_sources
    ):
        raise TrackingError("This synthetic-only experiment requires synthetic label provenance")
    for run_key, snapshot_key in (
        ("dataset_snapshot_id", "snapshot_id"),
        ("dataset_logical_fingerprint", "logical_dataset_fingerprint"),
        ("dataset_contract_version", "dataset_contract_version"),
        ("source_delta_version", "source_delta_version"),
        ("source_schema_fingerprint", "source_schema_fingerprint"),
        ("dataset_as_of_time", "as_of_time"),
    ):
        if manifest[run_key] != snapshot[snapshot_key]:
            raise TrackingError(f"Training run and snapshot disagree on {run_key}")
    metrics = _object(json.loads(metrics_bytes), "metrics")
    metric_values = _metrics(metrics)
    estimator = _object(manifest["estimator_parameters"], "estimator parameters")
    splits = _object(manifest["split_boundaries"], "split boundaries")
    minimum = _object(manifest["minimum_rows"], "minimum support")
    runtimes = _object(manifest["runtime_versions"], "runtime versions")
    support = _object(manifest["cohort_support"], "cohort support")
    for split in ("validation", "test"):
        group = _object(support[split], f"{split} support")
        for key in ("rows", "positives", "negatives"):
            if group[key] != metric_values[f"{split}.{key}"]:
                raise TrackingError(f"{split} metric and manifest support disagree on {key}")
    training_support = _object(support["training"], "training support")
    for key in ("rows", "positives", "negatives"):
        value = training_support[key]
        if type(value) is not int or value < 0:
            raise TrackingError(f"Invalid training support: {key}")
        metric_values[f"training.{key}"] = float(value)
    if metric_values["training.rows"] != (
        metric_values["training.positives"] + metric_values["training.negatives"]
    ):
        raise TrackingError("Training support counts disagree")

    params = {
        "estimator.class": _parameter(manifest["estimator_class"]),
        "estimator.solver": _parameter(estimator["solver"]),
        "estimator.class_weight": _parameter(estimator["class_weight"]),
        "estimator.C": _parameter(estimator["C"]),
        "estimator.tol": _parameter(estimator["tol"]),
        "estimator.max_iter": _parameter(estimator["max_iter"]),
        "estimator.parameters": _parameter(estimator),
        "classification_threshold": _parameter(manifest["classification_threshold"]),
        "preprocessing_contract": _parameter(manifest["preprocessing_contract"]),
        "candidate_feature_columns": _parameter(manifest["candidate_feature_columns"]),
        "categorical_feature_columns": _parameter(manifest["categorical_feature_columns"]),
        "numeric_feature_columns": _parameter(manifest["numeric_feature_columns"]),
        "split.train_end": _parameter(splits["train_end"]),
        "split.validation_end": _parameter(splits["validation_end"]),
        "split.test_end": _parameter(splits["test_end"]),
        "minimum.train_rows": _parameter(minimum["train"]),
        "minimum.validation_rows": _parameter(minimum["validation"]),
        "minimum.test_rows": _parameter(minimum["test"]),
    }
    tags = {
        "tracking_contract_version": TRACKING_CONTRACT_VERSION,
        "training_run_id": _string(manifest["training_run_id"], "training_run_id"),
        "dataset_snapshot_id": _string(manifest["dataset_snapshot_id"], "dataset_snapshot_id"),
        "dataset_logical_fingerprint": _string(
            manifest["dataset_logical_fingerprint"], "dataset fingerprint"
        ),
        "training_contract_version": _string(
            manifest["training_contract_version"], "training contract"
        ),
        "dataset_contract_version": _string(
            manifest["dataset_contract_version"], "dataset contract"
        ),
        "source_delta_version": _parameter(manifest["source_delta_version"]),
        "source_schema_fingerprint": _string(
            manifest["source_schema_fingerprint"], "schema fingerprint"
        ),
        "dataset_as_of_time": _string(manifest["dataset_as_of_time"], "dataset AS_OF"),
        "runtime.python": _string(runtimes["python"], "Python version"),
        "runtime.scikit_learn": _string(runtimes["scikit_learn"], "sklearn version"),
        "runtime.pandas": _string(runtimes["pandas"], "pandas version"),
        "runtime.mlflow": version("mlflow"),
        "data_origin": "synthetic",
        "evaluation_scope": "integration_verification",
        "production_fraud_quality": "unproven",
        "model_family": "logistic_regression",
        "source.manifest_sha256": _sha256(manifest_bytes),
        "source.metrics_sha256": _string(manifest["metrics_sha256"], "metrics hash"),
        "source.predictions_sha256": _string(
            manifest["predictions_parquet_sha256"], "predictions hash"
        ),
    }
    corpus_id: str | None = None
    if corpus_lineage is not None:
        corpus = inspect_corpus(corpus_lineage.corpus)
        report = reconcile(
            corpus,
            corpus_lineage.validated,
            corpus_lineage.deduplicated,
            corpus_lineage.rolling,
            corpus_lineage.statistical,
        )
        statistical_boundary = _object(
            _object(report["boundaries"], "boundaries")["statistical"], "statistical boundary"
        )
        if (
            Path(_string(snapshot["source_delta_path"], "source Delta path")).resolve()
            != (corpus_lineage.statistical.resolve())
            or snapshot["source_delta_version"] != statistical_boundary["delta_version"]
        ):
            raise TrackingError("Corpus reconciliation does not match snapshot Delta lineage")
        if (
            Path(_string(snapshot["label_source_file"], "label source")).resolve()
            != (corpus.labels.path.resolve())
            or snapshot["label_source_sha256"] != corpus.manifest["labels_sha256"]
        ):
            raise TrackingError("Corpus labels do not match snapshot label provenance")
        corpus_id = _string(report["corpus_id"], "corpus ID")
        tags["scenario_corpus_id"] = corpus_id
    provenance = canonical_json(
        {
            "tracking_contract_version": TRACKING_CONTRACT_VERSION,
            "training_run_id": tags["training_run_id"],
            "training_run_path": str(source),
            "training_manifest_sha256": tags["source.manifest_sha256"],
            "metrics_sha256": tags["source.metrics_sha256"],
            "predictions_parquet_sha256": tags["source.predictions_sha256"],
            "dataset_snapshot_id": tags["dataset_snapshot_id"],
            "dataset_logical_fingerprint": tags["dataset_logical_fingerprint"],
            "source_delta_version": manifest["source_delta_version"],
            "source_schema_fingerprint": tags["source_schema_fingerprint"],
            "scenario_corpus_id": corpus_id,
            "mlflow_version": tags["runtime.mlflow"],
        }
    )
    tags["tracking.provenance_sha256"] = _sha256(provenance)
    return VerifiedEvidence(
        source,
        manifest,
        metrics,
        params,
        tags,
        metric_values,
        manifest_bytes,
        metrics_bytes,
        provenance,
    )


def _client(config: TrackingConfig) -> MlflowClient:
    return MlflowClient(tracking_uri=config.tracking_uri)


def _experiment(client: MlflowClient, config: TrackingConfig, create: bool) -> str:
    experiment = client.get_experiment_by_name(config.experiment_name)
    if experiment is None:
        if not create:
            raise TrackingError(f"Experiment does not exist: {config.experiment_name}")
        return client.create_experiment(
            config.experiment_name, artifact_location=config.artifact_uri
        )
    if experiment.artifact_location.rstrip("/") != config.artifact_uri.rstrip("/"):
        raise TrackingError("Existing experiment uses a different artifact root")
    if experiment.lifecycle_stage != "active":
        raise TrackingError("Experiment is not active")
    return experiment.experiment_id


def _runs_for_identity(client: MlflowClient, experiment_id: str, training_id: str) -> list[Run]:
    # SHA-256 IDs contain no filter metacharacters. Two results are enough to detect conflict.
    return list(
        client.search_runs(
            [experiment_id],
            filter_string=f"tags.training_run_id = '{training_id}'",
            run_view_type=ViewType.ALL,
            max_results=2,
        )
    )


def _read_back(
    client: MlflowClient,
    experiment_id: str,
    run_id: str,
    evidence: VerifiedEvidence,
    *,
    require_finished: bool,
) -> None:
    run = client.get_run(run_id)
    if run.info.experiment_id != experiment_id:
        raise TrackingError("MLflow experiment identity differs")
    if require_finished and run.info.status != "FINISHED":
        raise TrackingError(f"Tracking run is not complete: {run.info.status}")
    if run.data.params != evidence.params:
        raise TrackingError("MLflow parameters differ from verified training manifest")
    for key, value in evidence.tags.items():
        if run.data.tags.get(key) != value:
            raise TrackingError(f"MLflow tag differs: {key}")
    if set(run.data.metrics) != set(evidence.metric_values) or any(
        run.data.metrics[key] != value for key, value in evidence.metric_values.items()
    ):
        raise TrackingError("MLflow metrics differ from authoritative metrics.json")
    artifacts = {entry.path for entry in client.list_artifacts(run_id)}
    if artifacts != ARTIFACT_NAMES:
        raise TrackingError(f"MLflow artifact list differs: {sorted(artifacts)}")
    expected_bytes = {
        "manifest.json": evidence.manifest_bytes,
        "metrics.json": evidence.metrics_bytes,
        "provenance.json": evidence.provenance,
    }
    with tempfile.TemporaryDirectory() as destination:
        for name, expected in expected_bytes.items():
            downloaded = Path(client.download_artifacts(run_id, name, destination))
            if _sha256(downloaded.read_bytes()) != _sha256(expected):
                raise TrackingError(f"MLflow artifact content differs: {name}")


def track_run(
    source: Path, config: TrackingConfig, corpus_lineage: CorpusLineage | None = None
) -> dict[str, str]:
    evidence = verify_evidence(source, corpus_lineage)  # Nothing is logged before this passes.
    config.database.parent.mkdir(parents=True, exist_ok=True)
    config.artifacts.mkdir(parents=True, exist_ok=True)
    lock_path = config.database.with_suffix(".lock")
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            client = _client(config)
            experiment_id = _experiment(client, config, create=True)
            matching = _runs_for_identity(client, experiment_id, evidence.tags["training_run_id"])
            if len(matching) > 1:
                raise TrackingError("Multiple MLflow runs claim the same training_run_id")
            if matching:
                _read_back(
                    client, experiment_id, matching[0].info.run_id, evidence, require_finished=True
                )
                return {
                    "experiment_id": experiment_id,
                    "mlflow_run_id": matching[0].info.run_id,
                    "training_run_id": evidence.tags["training_run_id"],
                    "reused": "true",
                }
            run = client.create_run(
                experiment_id, tags=evidence.tags, run_name=evidence.tags["training_run_id"]
            )
            run_id = run.info.run_id
            try:
                for key, value in evidence.params.items():
                    client.log_param(run_id, key, value)
                for key, metric_value in evidence.metric_values.items():
                    client.log_metric(run_id, key, metric_value)
                with tempfile.TemporaryDirectory() as directory:
                    for name, content in (
                        ("manifest.json", evidence.manifest_bytes),
                        ("metrics.json", evidence.metrics_bytes),
                        ("provenance.json", evidence.provenance),
                    ):
                        artifact = Path(directory) / name
                        artifact.write_bytes(content)
                        client.log_artifact(run_id, str(artifact))
                _read_back(client, experiment_id, run_id, evidence, require_finished=False)
                client.set_terminated(run_id, status="FINISHED")
                _read_back(client, experiment_id, run_id, evidence, require_finished=True)
            except Exception:
                # Preserve an incomplete claim for diagnosis; retries fail closed.
                try:
                    client.set_terminated(run_id, status="FAILED")
                except Exception:
                    pass
                raise
            return {
                "experiment_id": experiment_id,
                "mlflow_run_id": run_id,
                "training_run_id": evidence.tags["training_run_id"],
                "reused": "false",
            }
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def inspect_tracked_run(config: TrackingConfig, run_id: str) -> dict[str, object]:
    if not config.database.is_file():
        raise TrackingError(f"Tracking database does not exist: {config.database}")
    client = _client(config)
    run = client.get_run(run_id)
    experiment = client.get_experiment(run.info.experiment_id)
    if experiment.name != config.experiment_name:
        raise TrackingError("Run belongs to a different experiment")
    return {
        "experiment": experiment.name,
        "experiment_id": experiment.experiment_id,
        "mlflow_run_id": run.info.run_id,
        "status": run.info.status,
        "training_run_id": run.data.tags.get("training_run_id"),
        "dataset_snapshot_id": run.data.tags.get("dataset_snapshot_id"),
        "model_family": run.data.tags.get("model_family"),
        "validation.average_precision": run.data.metrics.get("validation.average_precision"),
        "validation.roc_auc": run.data.metrics.get("validation.roc_auc"),
        "test.average_precision": run.data.metrics.get("test.average_precision"),
        "test.roc_auc": run.data.metrics.get("test.roc_auc"),
        "cohort_support": {
            split: {
                key: run.data.metrics.get(f"{split}.{key}")
                for key in ("rows", "positives", "negatives")
            }
            for split in ("training", "validation", "test")
        },
        "warning": "Synthetic integration verification; production fraud quality unproven",
    }


def compare_tracked_runs(config: TrackingConfig) -> dict[str, object]:
    if not config.database.is_file():
        raise TrackingError(f"Tracking database does not exist: {config.database}")
    client = _client(config)
    experiment_id = _experiment(client, config, create=False)
    runs = client.search_runs(
        [experiment_id],
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=1000,
        order_by=["attributes.start_time ASC"],
    )
    if len(runs) == 1000:
        raise TrackingError("Comparison exceeds the local 1000-run inspection limit")
    summaries: list[dict[str, object]] = []
    for run in runs:
        summaries.append(
            {
                "training_run_id": run.data.tags.get("training_run_id"),
                "mlflow_run_id": run.info.run_id,
                "status": run.info.status,
                "dataset_snapshot_id": run.data.tags.get("dataset_snapshot_id"),
                "dataset_logical_fingerprint": run.data.tags.get("dataset_logical_fingerprint"),
                "split_boundaries": {
                    key: run.data.params.get(f"split.{key}")
                    for key in ("train_end", "validation_end", "test_end")
                },
                "estimator": {
                    key: run.data.params.get(f"estimator.{key}")
                    for key in ("class", "solver", "class_weight", "C", "tol", "max_iter")
                },
                "cohort_support": {
                    split: {
                        key: run.data.metrics.get(f"{split}.{key}")
                        for key in ("rows", "positives", "negatives")
                    }
                    for split in ("training", "validation", "test")
                },
                "metrics": {
                    key: value
                    for key, value in run.data.metrics.items()
                    if key.startswith(("validation.", "test."))
                },
            }
        )
    warning = None
    if (
        len({item["dataset_snapshot_id"] for item in summaries}) > 1
        or len({json.dumps(item["split_boundaries"], sort_keys=True) for item in summaries}) > 1
    ):
        warning = "Different datasets or evaluation periods; metrics are not directly comparable"
    return {
        "experiment": config.experiment_name,
        "runs": summaries,
        "warning": warning,
        "scope": "Synthetic-only; no automatic model ranking or promotion",
    }
