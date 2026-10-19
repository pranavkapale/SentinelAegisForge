"""Reconstruct, compare, and immutably publish a trusted offline sklearn candidate."""

import fcntl
import hashlib
import json
import math
import os
import platform
import tempfile
from importlib.metadata import version
from pathlib import Path
from typing import cast

import mlflow.sklearn
import pyarrow.parquet as pq
import skops.io
from mlflow.models import Model
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from sentinelaegisforge_control_plane.datasets.dataset_builder import inspect_snapshot
from sentinelaegisforge_control_plane.datasets.dataset_contract import CANDIDATE_FEATURE_COLUMNS
from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.tracking.run import (
    TrackingConfig,
    verify_evidence,
)
from sentinelaegisforge_control_plane.training.baseline import (
    ESTIMATOR_CLASS,
    PREPROCESSING_CONTRACT,
    estimator_parameters,
    fit_baseline,
    probability_estimates,
)
from sentinelaegisforge_control_plane.training.run import inspect_run
from sentinelaegisforge_control_plane.training.temporal import (
    CATEGORICAL_FEATURE_COLUMNS,
    CLASSIFICATION_THRESHOLD,
    NUMERIC_FEATURE_COLUMNS,
    TemporalCohorts,
    TemporalConfig,
    partition_rows,
)

from .contract import (
    CONTRACT_VERSION,
    FEATURES,
    INPUT_CONTRACT,
    INPUT_FINGERPRINT,
    SERIALIZATION_FORMAT,
    SKOPS_TRUSTED_TYPES,
    CandidateError,
    validate_input,
)

REL_TOL = 1e-12
ABS_TOL = 1e-12
REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_OUTPUT_ROOT = REPOSITORY_ROOT / ".local/ml/model-candidates"
MODEL_DIR = "model"


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _runtime() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "scikit_learn": version("scikit-learn"),
        "pandas": version("pandas"),
        "mlflow": version("mlflow"),
        "skops": version("skops"),
    }


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise CandidateError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _validated_source(source: Path) -> tuple[dict[str, object], dict[str, object], Path]:
    source = source.resolve(strict=True)
    run = inspect_run(source)
    snapshot_path = Path(cast(str, run["dataset_snapshot_path"]))
    snapshot = inspect_snapshot(snapshot_path)
    for run_key, snapshot_key in (
        ("dataset_snapshot_id", "snapshot_id"),
        ("dataset_logical_fingerprint", "logical_dataset_fingerprint"),
        ("dataset_contract_version", "dataset_contract_version"),
        ("source_delta_version", "source_delta_version"),
        ("source_schema_fingerprint", "source_schema_fingerprint"),
        ("dataset_as_of_time", "as_of_time"),
    ):
        if run[run_key] != snapshot[snapshot_key]:
            raise CandidateError(f"Training and snapshot disagree: {run_key}")
    if (
        run["candidate_feature_columns"] != list(FEATURES)
        or snapshot["candidate_feature_columns"] != list(FEATURES)
        or run["categorical_feature_columns"] != list(CATEGORICAL_FEATURE_COLUMNS)
        or run["numeric_feature_columns"] != list(NUMERIC_FEATURE_COLUMNS)
        or run["preprocessing_contract"] != PREPROCESSING_CONTRACT
        or run["estimator_class"] != ESTIMATOR_CLASS
        or run["estimator_parameters"] != estimator_parameters()
        or run["classification_threshold"] != CLASSIFICATION_THRESHOLD
    ):
        raise CandidateError("Archived estimator, preprocessing or feature contract differs")
    prior_runtime = _object(run["runtime_versions"], "training runtime")
    current = _runtime()
    if any(prior_runtime[key] != current[key] for key in ("python", "scikit_learn", "pandas")):
        raise CandidateError("Training/runtime versions differ; reconstruction is not comparable")
    return run, snapshot, snapshot_path


def _cohorts(run: dict[str, object], snapshot: dict[str, object], path: Path) -> TemporalCohorts:
    split = _object(run["split_boundaries"], "split boundaries")
    minimum = _object(run["minimum_rows"], "minimum support")
    config = TemporalConfig(
        parse_utc_timestamp(cast(str, split["train_end"])),
        parse_utc_timestamp(cast(str, split["validation_end"])),
        parse_utc_timestamp(cast(str, split["test_end"])),
        cast(int, minimum["train"]),
        cast(int, minimum["validation"]),
        cast(int, minimum["test"]),
    )
    rows = cast(list[dict[str, object]], pq.read_table(path / "dataset.parquet").to_pylist())
    cohorts = partition_rows(rows, snapshot, config)
    support = _object(run["cohort_support"], "cohort support")
    for name, group in (
        ("training", cohorts.train),
        ("validation", cohorts.validation),
        ("test", cohorts.test),
    ):
        expected = _object(support[name], f"{name} support")
        positives = sum(cast(int, item["fraud_label"]) for item in group)
        if expected != {
            "rows": len(group),
            "positives": positives,
            "negatives": len(group) - positives,
        }:
            raise CandidateError(f"Reconstructed {name} cohort support differs")
    for key, observed in (
        ("train_period_rows", cohorts.train_period_rows),
        ("excluded_from_fit_due_to_label_delay", cohorts.excluded_from_fit_due_to_label_delay),
        ("outside_test_window_rows", cohorts.outside_test_window_rows),
    ):
        if run[key] != observed:
            raise CandidateError(f"Reconstructed chronology differs: {key}")
    return cohorts


def _classes(model: Pipeline) -> None:
    if list(cast(LogisticRegression, model.named_steps["classifier"]).classes_) != [0, 1]:
        raise CandidateError("Expected class order [0=LEGIT, 1=FRAUD]")


def _compare(model: Pipeline, cohorts: TemporalCohorts, source: Path) -> dict[str, object]:
    _classes(model)
    archived = cast(
        list[dict[str, object]], pq.read_table(source / "predictions.parquet").to_pylist()
    )
    archived_by_key = {(row["split"], row["event_id"]): row for row in archived}
    if len(archived_by_key) != len(archived):
        raise CandidateError("Archived prediction identities are duplicated")
    compared = mismatches = 0
    max_abs = max_rel = 0.0
    for split, group in (("validation", cohorts.validation), ("test", cohorts.test)):
        scores = probability_estimates(model, group, FEATURES)
        for row, value in zip(group, scores, strict=True):
            key = (split, row["event_id"])
            prior = archived_by_key.pop(key, None)
            if (
                prior is None
                or prior["fraud_label"] != row["fraud_label"]
                or prior["ingestion_time"] != row["ingestion_time"]
            ):
                raise CandidateError(f"Archived prediction identity/label differs: {key}")
            expected = cast(float, prior["fraud_probability"])
            difference = abs(value - expected)
            max_abs = max(max_abs, difference)
            if expected:
                max_rel = max(max_rel, difference / abs(expected))
            mismatches += not math.isclose(value, expected, rel_tol=REL_TOL, abs_tol=ABS_TOL)
            compared += 1
    if archived_by_key or mismatches:
        raise CandidateError(
            f"Archived prediction mismatch: {mismatches} differences; "
            f"{len(archived_by_key)} extra identities; max abs {max_abs}"
        )
    return {
        "compared_predictions": compared,
        "max_absolute_difference": max_abs,
        "max_relative_difference": max_rel,
        "mismatches": mismatches,
        "positive_class": 1,
        "relative_tolerance": REL_TOL,
        "absolute_tolerance": ABS_TOL,
        "outcome": "EQUIVALENT",
    }


def _verify_tracking(source: Path, run_id: str, config: TrackingConfig) -> None:
    """Read-only check against existing Phase 15 evidence; never create a run/experiment."""
    if not config.database.is_file():
        raise CandidateError("Tracking database does not exist")
    evidence = verify_evidence(source)
    client = MlflowClient(tracking_uri=config.tracking_uri)
    record = client.get_run(run_id)
    experiment = client.get_experiment(record.info.experiment_id)
    if (
        experiment is None
        or experiment.name != config.experiment_name
        or record.info.status != "FINISHED"
    ):
        raise CandidateError("Tracking experiment/status differs")
    if record.data.params != evidence.params or record.data.metrics != evidence.metric_values:
        raise CandidateError("Tracked parameters/metrics differ from authoritative run")
    for key, value in evidence.tags.items():
        if key in {"tracking.provenance_sha256"}:
            continue  # Phase 14's explicitly reconciled corpus adds its own provenance.
        if record.data.tags.get(key) != value:
            raise CandidateError(f"Tracked source tag differs: {key}")
    with tempfile.TemporaryDirectory() as directory:
        for name, expected in (
            ("manifest.json", evidence.manifest_bytes),
            ("metrics.json", evidence.metrics_bytes),
        ):
            actual = Path(client.download_artifacts(run_id, name, directory)).read_bytes()
            if _sha(actual) != _sha(expected):
                raise CandidateError(f"Tracked artifact differs: {name}")
        provenance = Path(
            client.download_artifacts(run_id, "provenance.json", directory)
        ).read_bytes()
        if _sha(provenance) != record.data.tags.get("tracking.provenance_sha256"):
            raise CandidateError("Tracked provenance hash differs")
        recorded = _object(json.loads(provenance), "tracking provenance")
        if (
            recorded["training_run_id"] != evidence.tags["training_run_id"]
            or recorded["dataset_snapshot_id"] != evidence.tags["dataset_snapshot_id"]
        ):
            raise CandidateError("Tracked provenance identities differ")
    if (
        record.data.tags.get("data_origin") != "synthetic"
        or record.data.tags.get("production_fraud_quality") != "unproven"
    ):
        raise CandidateError("Tracked synthetic-only scope differs")


def _files(model_dir: Path) -> dict[str, str]:
    if model_dir.is_symlink() or not model_dir.is_dir():
        raise CandidateError("Model artifact directory is missing or linked")
    files: dict[str, str] = {}
    for item in sorted(model_dir.rglob("*")):
        if item.is_symlink() or (not item.is_file() and not item.is_dir()):
            raise CandidateError("Model artifact contains symlink or unsupported entry")
        if item.is_file():
            files[item.relative_to(model_dir).as_posix()] = _sha(item.read_bytes())
    if not files:
        raise CandidateError("Model artifact is empty")
    return files


def _load(model_dir: Path) -> Pipeline:
    """Only after caller verifies bytes/runtime; no untrusted type auto-allowlisting."""
    flavors = Model.load(str(model_dir / "MLmodel")).flavors
    flavor = _object(flavors.get("sklearn"), "sklearn flavor")
    if (
        flavor.get("serialization_format") != SERIALIZATION_FORMAT
        or flavor.get("skops_trusted_types") != SKOPS_TRUSTED_TYPES
        or flavor.get("sklearn_version") != version("scikit-learn")
    ):
        raise CandidateError("Untrusted/unsupported MLflow serialization configuration")
    relative = flavor.get("pickled_model")
    if relative != "model.skops":
        raise CandidateError("Unexpected sklearn model file")
    model_file = model_dir / cast(str, relative)
    if set(skops.io.get_untrusted_types(file=model_file)) != set(SKOPS_TRUSTED_TYPES):
        raise CandidateError("Serialized model requires untrusted types")
    model = skops.io.load(model_file, trusted=SKOPS_TRUSTED_TYPES)
    if not isinstance(model, Pipeline) or list(model.named_steps) != [
        "preprocessing",
        "classifier",
    ]:
        raise CandidateError("Serialized model is not the expected complete Pipeline")
    if not isinstance(model.named_steps["preprocessing"], ColumnTransformer) or not isinstance(
        model.named_steps["classifier"], LogisticRegression
    ):
        raise CandidateError("Serialized preprocessing/classifier structure differs")
    preprocessing = cast(ColumnTransformer, model.named_steps["preprocessing"])
    if (
        list(model.feature_names_in_) != list(FEATURES)
        or preprocessing.remainder != "drop"
        or preprocessing.sparse_threshold != 1.0
        or [name for name, _, _ in preprocessing.transformers] != ["categorical", "numeric"]
        or list(preprocessing.transformers[0][2]) != list(CATEGORICAL_FEATURE_COLUMNS)
        or list(preprocessing.transformers[1][2]) != list(NUMERIC_FEATURE_COLUMNS)
    ):
        raise CandidateError("Serialized feature/preprocessing contract differs")
    encoder = preprocessing.transformers[0][1]
    numeric = preprocessing.transformers[1][1]
    if not isinstance(encoder, OneHotEncoder) or encoder.handle_unknown != "ignore":
        raise CandidateError("Serialized categorical encoder differs")
    if not isinstance(numeric, Pipeline) or list(numeric.named_steps) != ["imputer", "scaler"]:
        raise CandidateError("Serialized numeric pipeline differs")
    imputer = numeric.named_steps["imputer"]
    scaler = numeric.named_steps["scaler"]
    if (
        not isinstance(imputer, SimpleImputer)
        or imputer.strategy != "constant"
        or imputer.fill_value != 0.0
        or not imputer.keep_empty_features
        or not isinstance(scaler, StandardScaler)
        or not scaler.with_mean
        or not scaler.with_std
    ):
        raise CandidateError("Serialized numeric preprocessing differs")
    if (
        cast(LogisticRegression, model.named_steps["classifier"]).get_params(deep=False)
        != estimator_parameters()
    ):
        raise CandidateError("Serialized estimator configuration differs")
    _classes(model)
    return model


def _identity(run: dict[str, object], runtime: dict[str, str]) -> dict[str, object]:
    return {
        "candidate_contract_version": CONTRACT_VERSION,
        "training_run_id": run["training_run_id"],
        "dataset_snapshot_id": run["dataset_snapshot_id"],
        "dataset_logical_fingerprint": run["dataset_logical_fingerprint"],
        "input_contract_version": INPUT_CONTRACT["version"],
        "input_schema_fingerprint": INPUT_FINGERPRINT,
        "preprocessing_contract": run["preprocessing_contract"],
        "estimator_parameters": run["estimator_parameters"],
        "serialization_format": SERIALIZATION_FORMAT,
        "runtime_versions": runtime,
    }


def build_candidate(
    source: Path,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    *,
    tracking_run_id: str | None = None,
    tracking: TrackingConfig | None = None,
) -> Path:
    run, snapshot, snapshot_path = _validated_source(source)
    if tracking_run_id is not None:
        _verify_tracking(source, tracking_run_id, tracking or TrackingConfig())
    cohorts = _cohorts(run, snapshot, snapshot_path)
    runtime = _runtime()
    identity = _identity(run, runtime)
    candidate_id = _sha(canonical_json(identity))
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / candidate_id
    lock_path = output_root / f".{candidate_id}.lock"
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            if target.exists():
                manifest = inspect_candidate(target, tracking=tracking)
                if manifest["source_mlflow_run_id"] != tracking_run_id:
                    raise CandidateError("Existing candidate tracking provenance differs")
                return target
            model = fit_baseline(cohorts.train, FEATURES)
            comparison = _compare(model, cohorts, source)
            with tempfile.TemporaryDirectory(prefix="candidate-build-", dir=output_root) as scratch:
                stage = Path(scratch)
                model_dir = stage / MODEL_DIR
                mlflow.sklearn.save_model(
                    model,
                    str(model_dir),
                    serialization_format=SERIALIZATION_FORMAT,
                    skops_trusted_types=SKOPS_TRUSTED_TYPES,
                    signature=False,
                    pip_requirements=[
                        f"mlflow=={runtime['mlflow']}",
                        f"scikit-learn=={runtime['scikit_learn']}",
                        f"pandas=={runtime['pandas']}",
                        f"skops=={runtime['skops']}",
                    ],
                )
                files = _files(model_dir)
                reloaded = _load(model_dir)
                round_trip = _compare(reloaded, cohorts, source)
                if round_trip != comparison:
                    raise CandidateError("Saved/reloaded prediction evidence differs")
                contract_bytes = canonical_json(INPUT_CONTRACT)
                comparison_bytes = canonical_json(comparison)
                (stage / "input_contract.json").write_bytes(contract_bytes)
                (stage / "equivalence.json").write_bytes(comparison_bytes)
                candidate_manifest: dict[str, object] = {
                    **identity,
                    "candidate_id": candidate_id,
                    "identity_sha256": candidate_id,
                    "source_training_path": str(source.resolve()),
                    "source_mlflow_run_id": tracking_run_id,
                    "source_delta_version": run["source_delta_version"],
                    "source_schema_fingerprint": run["source_schema_fingerprint"],
                    "dataset_contract_version": run["dataset_contract_version"],
                    "training_contract_version": run["training_contract_version"],
                    "model_class": ESTIMATOR_CLASS,
                    "positive_class": {"label": 1, "meaning": "FRAUD"},
                    "comparison": comparison,
                    "round_trip_comparison": round_trip,
                    "input_contract_sha256": _sha(contract_bytes),
                    "equivalence_sha256": _sha(comparison_bytes),
                    "model_files": files,
                    "model_tree_sha256": _sha(canonical_json(files)),
                    "validation_status": "TECHNICALLY_VERIFIED",
                    "data_origin": "synthetic",
                    "evaluation_scope": "integration_verification",
                    "production_fraud_quality": "unproven",
                    "candidate_purpose": "artifact_contract_verification",
                }
                (stage / "candidate_manifest.json").write_bytes(canonical_json(candidate_manifest))
                # Exclusive claim: an interrupted transfer is visible, never silently repaired.
                target.mkdir()
                for name in (
                    MODEL_DIR,
                    "input_contract.json",
                    "equivalence.json",
                    "candidate_manifest.json",
                ):
                    os.rename(stage / name, target / name)
            inspect_candidate(target, tracking=tracking)
            return target
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _inspect_candidate_loaded(
    path: Path, *, tracking: TrackingConfig | None = None
) -> tuple[dict[str, object], Pipeline]:
    if path.is_symlink() or not path.is_dir():
        raise CandidateError("Expected candidate directory")
    if {entry.name for entry in path.iterdir()} != {
        MODEL_DIR,
        "input_contract.json",
        "equivalence.json",
        "candidate_manifest.json",
    }:
        raise CandidateError("Incomplete/unexpected candidate artifacts")
    if any(
        (path / name).is_symlink()
        for name in ("input_contract.json", "equivalence.json", "candidate_manifest.json")
    ):
        raise CandidateError("Candidate artifact symlink is forbidden")
    try:
        manifest = _object(
            json.loads((path / "candidate_manifest.json").read_bytes()), "candidate manifest"
        )
        if (
            manifest["candidate_contract_version"] != CONTRACT_VERSION
            or manifest["validation_status"] != "TECHNICALLY_VERIFIED"
        ):
            raise CandidateError("Candidate contract/status differs")
        runtime = _runtime()
        if manifest["runtime_versions"] != runtime:
            raise CandidateError("Candidate runtime differs from pinned local environment")
        source = Path(cast(str, manifest["source_training_path"]))
        run, snapshot, snapshot_path = _validated_source(source)
        tracked_id = manifest["source_mlflow_run_id"]
        if tracked_id is not None:
            if not isinstance(tracked_id, str) or not tracked_id:
                raise CandidateError("Invalid source MLflow run ID")
            _verify_tracking(source, tracked_id, tracking or TrackingConfig())
        identity = _identity(run, runtime)
        candidate_id = _sha(canonical_json(identity))
        if (
            manifest["candidate_id"] != candidate_id
            or path.name != candidate_id
            or any(manifest[key] != value for key, value in identity.items())
        ):
            raise CandidateError("Candidate identity differs")
        if manifest["input_contract_sha256"] != _sha(
            (path / "input_contract.json").read_bytes()
        ) or (path / "input_contract.json").read_bytes() != canonical_json(INPUT_CONTRACT):
            raise CandidateError("Candidate input contract differs")
        comparison = _object(json.loads((path / "equivalence.json").read_bytes()), "equivalence")
        if (
            manifest["equivalence_sha256"] != _sha((path / "equivalence.json").read_bytes())
            or manifest["comparison"] != comparison
            or manifest["round_trip_comparison"] != comparison
            or comparison["outcome"] != "EQUIVALENT"
            or comparison["mismatches"] != 0
        ):
            raise CandidateError("Candidate equivalence evidence differs")
        files = _files(path / MODEL_DIR)
        if manifest["model_files"] != files or manifest["model_tree_sha256"] != _sha(
            canonical_json(files)
        ):
            raise CandidateError("Model artifact hash differs")
        model = _load(path / MODEL_DIR)  # Only after all content and runtime checks pass.
        cohorts = _cohorts(run, snapshot, snapshot_path)
        observed = _compare(model, cohorts, source)
        if observed != comparison:
            raise CandidateError("Reloaded model and archived prediction evidence differ")
        return manifest, model
    except CandidateError:
        raise
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise CandidateError(f"Cannot inspect candidate: {error}") from error


def inspect_candidate(path: Path, *, tracking: TrackingConfig | None = None) -> dict[str, object]:
    return _inspect_candidate_loaded(path, tracking=tracking)[0]


def load_verified_candidate(
    path: Path, *, tracking: TrackingConfig | None = None
) -> tuple[dict[str, object], Pipeline]:
    """Verify bytes, provenance and archived parity before returning one trusted model load."""
    return _inspect_candidate_loaded(path, tracking=tracking)


def score_candidate(
    path: Path,
    rows: list[dict[str, object]],
    *,
    tracking: TrackingConfig | None = None,
) -> list[float]:
    inspect_candidate(path, tracking=tracking)  # Hash/runtime validation before deserialization.
    if not rows:
        raise CandidateError("Offline scoring requires at least one input row")
    validated = tuple(validate_input(row) for row in rows)
    model = _load(path / MODEL_DIR)
    _classes(model)
    return probability_estimates(model, validated, tuple(CANDIDATE_FEATURE_COLUMNS))
