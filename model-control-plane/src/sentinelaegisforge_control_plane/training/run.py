"""Immutable local baseline runs; no model artifact or serving contract."""

import hashlib
import json
import math
import platform
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.datasets.dataset_builder import inspect_snapshot
from sentinelaegisforge_control_plane.datasets.dataset_contract import (
    CANDIDATE_FEATURE_COLUMNS,
    DATASET_CONTRACT_VERSION,
)
from sentinelaegisforge_control_plane.datasets.labels import utc_text
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

from .baseline import (
    ESTIMATOR_CLASS,
    PREPROCESSING_CONTRACT,
    estimator_parameters,
    evaluate,
    fit_baseline,
    probability_estimates,
)
from .errors import TrainingContractError
from .temporal import (
    CATEGORICAL_FEATURE_COLUMNS,
    CLASSIFICATION_THRESHOLD,
    NUMERIC_FEATURE_COLUMNS,
    TRAINING_CONTRACT_VERSION,
    TemporalConfig,
    partition_rows,
)

RUN_IDENTITY_FIELDS = (
    "training_contract_version",
    "dataset_snapshot_path",
    "dataset_snapshot_id",
    "dataset_logical_fingerprint",
    "split_boundaries",
    "minimum_rows",
    "candidate_feature_columns",
    "categorical_feature_columns",
    "numeric_feature_columns",
    "preprocessing_contract",
    "estimator_class",
    "estimator_parameters",
    "classification_threshold",
    "runtime_versions",
)
PREDICTION_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("split", pa.string(), nullable=False),
        pa.field("ingestion_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("fraud_label", pa.int8(), nullable=False),
        pa.field("fraud_probability", pa.float64(), nullable=False),
        pa.field("predicted_label_at_0_5", pa.int8(), nullable=False),
    ]
)


@dataclass(frozen=True)
class TrainingRun:
    path: Path
    manifest: dict[str, object]


def _runtime_versions() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "pandas": version("pandas"),
        "scikit_learn": version("scikit-learn"),
        "pyarrow": version("pyarrow"),
        "deltalake": version("deltalake"),
    }


def _support(rows: tuple[dict[str, object], ...]) -> dict[str, int]:
    positives = sum(cast(int, row["fraud_label"]) for row in rows)
    return {"rows": len(rows), "positives": positives, "negatives": len(rows) - positives}


def _prediction_rows(
    split: str, rows: tuple[dict[str, object], ...], probabilities: list[float]
) -> list[dict[str, object]]:
    return [
        {
            "event_id": row["event_id"],
            "split": split,
            "ingestion_time": row["ingestion_time"],
            "fraud_label": row["fraud_label"],
            "fraud_probability": probability,
            "predicted_label_at_0_5": int(probability >= CLASSIFICATION_THRESHOLD),
        }
        for row, probability in zip(rows, probabilities, strict=True)
    ]


def train_baseline(snapshot_path: Path, config: TemporalConfig, output_root: Path) -> TrainingRun:
    snapshot = snapshot_path.resolve(strict=True)
    dataset_manifest = inspect_snapshot(snapshot)
    if dataset_manifest.get("dataset_contract_version") != DATASET_CONTRACT_VERSION:
        raise TrainingContractError("Only transaction-fraud-v1 snapshots are supported")
    table = pq.read_table(snapshot / "dataset.parquet")
    rows = cast(list[dict[str, object]], table.to_pylist())
    cohorts = partition_rows(rows, dataset_manifest, config)
    candidates = tuple(CANDIDATE_FEATURE_COLUMNS)

    # No validation or test rows are passed to fit or any preprocessing fit operation.
    model = fit_baseline(cohorts.train, candidates)
    validation_probabilities = probability_estimates(model, cohorts.validation, candidates)
    test_probabilities = probability_estimates(model, cohorts.test, candidates)
    metrics: dict[str, object] = {
        "validation": evaluate(cohorts.validation, validation_probabilities),
        "test": evaluate(cohorts.test, test_probabilities),
    }
    predictions = pa.Table.from_pylist(
        _prediction_rows("validation", cohorts.validation, validation_probabilities)
        + _prediction_rows("test", cohorts.test, test_probabilities),
        schema=PREDICTION_SCHEMA,
    )

    identity: dict[str, object] = {
        "training_contract_version": TRAINING_CONTRACT_VERSION,
        "dataset_snapshot_path": str(snapshot),
        "dataset_snapshot_id": dataset_manifest["snapshot_id"],
        "dataset_logical_fingerprint": dataset_manifest["logical_dataset_fingerprint"],
        "split_boundaries": {
            "train_end": utc_text(config.train_end),
            "validation_end": utc_text(config.validation_end),
            "test_end": utc_text(config.test_end),
        },
        "minimum_rows": {
            "train": config.min_train_rows,
            "validation": config.min_validation_rows,
            "test": config.min_test_rows,
        },
        "candidate_feature_columns": list(candidates),
        "categorical_feature_columns": list(CATEGORICAL_FEATURE_COLUMNS),
        "numeric_feature_columns": list(NUMERIC_FEATURE_COLUMNS),
        "preprocessing_contract": PREPROCESSING_CONTRACT,
        "estimator_class": ESTIMATOR_CLASS,
        "estimator_parameters": estimator_parameters(),
        "classification_threshold": CLASSIFICATION_THRESHOLD,
        "runtime_versions": _runtime_versions(),
    }
    run_id = hashlib.sha256(canonical_json(identity)).hexdigest()
    manifest: dict[str, object] = {
        **identity,
        "training_run_id": run_id,
        "dataset_contract_version": dataset_manifest["dataset_contract_version"],
        "source_delta_version": dataset_manifest["source_delta_version"],
        "source_schema_fingerprint": dataset_manifest["source_schema_fingerprint"],
        "dataset_as_of_time": dataset_manifest["as_of_time"],
        "cohort_support": {
            "training": _support(cohorts.train),
            "validation": _support(cohorts.validation),
            "test": _support(cohorts.test),
        },
        "train_period_rows": cohorts.train_period_rows,
        "excluded_from_fit_due_to_label_delay": cohorts.excluded_from_fit_due_to_label_delay,
        "outside_test_window_rows": cohorts.outside_test_window_rows,
        "predictions_rows": predictions.num_rows,
        "primary_ranking_metric": "average_precision",
        "evaluation_scope": "controlled mechanics only; no representative fraud-quality evidence",
    }
    target = output_root.resolve() / run_id
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        target.mkdir()
    except FileExistsError:
        existing = inspect_run(target)
        expected = {
            **manifest,
            "metrics_sha256": existing["metrics_sha256"],
            "predictions_parquet_sha256": existing["predictions_parquet_sha256"],
        }
        if existing != expected:
            raise TrainingContractError(f"Existing run manifest differs: {target}")
        if json.loads((target / "metrics.json").read_bytes()) != metrics:
            raise TrainingContractError(f"Existing run metrics differ: {target}")
        if not pq.read_table(target / "predictions.parquet").equals(
            predictions, check_metadata=False
        ):
            raise TrainingContractError(f"Existing run predictions differ: {target}")
        return TrainingRun(target, existing)

    with (target / "predictions.parquet").open("xb") as output:
        pq.write_table(predictions, output, version="2.6", compression="zstd", use_dictionary=False)
    with (target / "metrics.json").open("xb") as output:
        output.write(canonical_json(metrics))
    manifest["predictions_parquet_sha256"] = hashlib.sha256(
        (target / "predictions.parquet").read_bytes()
    ).hexdigest()
    manifest["metrics_sha256"] = hashlib.sha256((target / "metrics.json").read_bytes()).hexdigest()
    with (target / "manifest.json").open("xb") as output:
        output.write(canonical_json(manifest))
    inspect_run(target)
    return TrainingRun(target, manifest)


def inspect_run(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_dir():
        raise TrainingContractError(f"Expected immutable training-run directory: {path}")
    artifacts = {"manifest.json", "metrics.json", "predictions.parquet"}
    if {item.name for item in path.iterdir()} != artifacts:
        raise TrainingContractError(f"Incomplete/unexpected training-run artifacts: {path}")
    if any((path / name).is_symlink() for name in artifacts):
        raise TrainingContractError("Training-run artifacts must not be symlinks")
    try:
        manifest = cast(dict[str, object], json.loads((path / "manifest.json").read_bytes()))
        metrics = cast(dict[str, object], json.loads((path / "metrics.json").read_bytes()))
        predictions = pq.read_table(path / "predictions.parquet")
        if manifest["training_contract_version"] != TRAINING_CONTRACT_VERSION:
            raise TrainingContractError("Unsupported training contract version")
        identity = {name: manifest[name] for name in RUN_IDENTITY_FIELDS}
        if (
            manifest["training_run_id"] != path.name
            or manifest["training_run_id"] != hashlib.sha256(canonical_json(identity)).hexdigest()
        ):
            raise TrainingContractError("Training-run identity mismatch")
        for name, expected in (
            ("metrics.json", manifest["metrics_sha256"]),
            ("predictions.parquet", manifest["predictions_parquet_sha256"]),
        ):
            if hashlib.sha256((path / name).read_bytes()).hexdigest() != expected:
                raise TrainingContractError(f"Training-run artifact hash differs: {name}")
        if [(f.name, f.type) for f in predictions.schema] != [
            (f.name, f.type) for f in PREDICTION_SCHEMA
        ]:
            raise TrainingContractError("Unexpected predictions schema")
        if predictions.num_rows != manifest["predictions_rows"]:
            raise TrainingContractError("Prediction count differs from manifest")
        if set(metrics) != {"validation", "test"}:
            raise TrainingContractError("Metrics must separate validation and test")
        if any(
            not math.isfinite(value) or not 0 <= value <= 1
            for value in predictions["fraud_probability"].to_pylist()
        ):
            raise TrainingContractError("Invalid persisted probability estimates")
        return manifest
    except TrainingContractError:
        raise
    except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as error:
        raise TrainingContractError(f"Cannot inspect training run {path}: {error}") from error
