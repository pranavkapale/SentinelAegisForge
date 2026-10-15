"""Immutable, label-free scoring of one fixed corrected statistical Delta version."""

import json
import math
import platform
from datetime import datetime
from importlib.metadata import version
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.candidates.contract import FEATURES, INPUT_FINGERPRINT
from sentinelaegisforge_control_plane.candidates.run import score_candidate
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp, utc_text
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.registry.run import (
    RegistryConfig,
    _source_path,
    inspect_registered_candidate,
)
from sentinelaegisforge_control_plane.training.run import inspect_run

from .contract import (
    LEDGER_SCHEMA,
    LEDGER_VERSION,
    SCORING_VERSION,
    MonitoringError,
    artifact_directory,
    identity,
    object_value,
    row_fingerprint,
    rows_fingerprint,
    sha256,
    utc_window,
)

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_LEDGER_ROOT = ROOT / ".local/ml/prediction-ledgers"
LEDGER_FILES = {"predictions.parquet", "manifest.json"}
ROW_COLUMNS = tuple(LEDGER_SCHEMA.names)
IDENTITY_FIELDS = (
    "ledger_contract_version",
    "scoring_contract_version",
    "candidate_id",
    "registered_model_name",
    "registered_model_version",
    "model_tree_sha256",
    "input_contract_fingerprint",
    "source_delta_path",
    "source_delta_table_id",
    "source_delta_version",
    "source_schema_fingerprint",
    "score_start",
    "score_end",
    "scored_input_fingerprint",
    "runtime_versions",
)


def _runtime() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "mlflow": version("mlflow"),
        "scikit_learn": version("scikit-learn"),
        "pandas": version("pandas"),
        "deltalake": version("deltalake"),
        "pyarrow": version("pyarrow"),
        "skops": version("skops"),
    }


def _archived_comparison(rows: list[dict[str, object]], training_path: Path) -> dict[str, object]:
    training = inspect_run(training_path)
    splits = object_value(training["split_boundaries"], "training split")
    train_end = parse_utc_timestamp(cast(str, splits["train_end"]))
    validation_end = parse_utc_timestamp(cast(str, splits["validation_end"]))
    test_end = parse_utc_timestamp(cast(str, splits["test_end"]))
    archived = cast(
        list[dict[str, object]], pq.read_table(training_path / "predictions.parquet").to_pylist()
    )
    by_key = {(row["split"], row["event_id"]): row for row in archived}
    if len(by_key) != len(archived):
        raise MonitoringError("Archived prediction identity is duplicated")
    overlap = mismatches = 0
    maximum = 0.0
    for row in rows:
        when = cast(datetime, row["ingestion_time"])
        split = (
            "validation"
            if train_end <= when < validation_end
            else "test"
            if validation_end <= when < test_end
            else None
        )
        if split is None:
            continue
        prior = by_key.get((split, row["event_id"]))
        if prior is None:
            continue  # Archived predictions contain only Phase 12 labeled rows.
        if prior["ingestion_time"] != when:
            raise MonitoringError("Archived prediction ingestion identity differs")
        actual = cast(float, row["fraud_probability_estimate"])
        expected = cast(float, prior["fraud_probability"])
        maximum = max(maximum, abs(actual - expected))
        mismatches += not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
        overlap += 1
    scored_ids = {row["event_id"] for row in rows}
    expected_keys = {
        (row["split"], row["event_id"]) for row in archived if row["event_id"] in scored_ids
    }
    if overlap != len(expected_keys):
        raise MonitoringError("Archived prediction overlap has missing identities")
    if mismatches:
        raise MonitoringError(f"Archived prediction equivalence failed: {mismatches} mismatches")
    return {
        "overlap_count": overlap,
        "matched_identities": overlap,
        "max_absolute_difference": maximum,
        "mismatches": mismatches,
        "relative_tolerance": 1e-12,
        "absolute_tolerance": 1e-12,
    }


def build_ledger(
    registry: RegistryConfig,
    model_version: str,
    features_delta: Path,
    source_version: int,
    score_start: datetime,
    score_end: datetime,
    output_root: Path = DEFAULT_LEDGER_ROOT,
) -> Path:
    """The API has no label input and never reads an outcome source."""
    start, end = utc_window(score_start, score_end)
    if type(source_version) is not int or source_version < 0:
        raise MonitoringError("An explicit nonnegative Delta version is required")
    registration = inspect_registered_candidate(registry, model_version, deep=True)
    candidate = _source_path(cast(str, registration["source_uri"])).parent
    candidate_manifest = object_value(
        json.loads((candidate / "candidate_manifest.json").read_bytes()), "candidate"
    )
    training_path = Path(cast(str, candidate_manifest["source_training_path"]))
    training = inspect_run(training_path)
    train_end = parse_utc_timestamp(
        cast(str, object_value(training["split_boundaries"], "splits")["train_end"])
    )
    if start < train_end:
        raise MonitoringError("Scoring starts before the frozen model training cutoff")
    snapshot_path = Path(cast(str, training["dataset_snapshot_path"]))
    from sentinelaegisforge_control_plane.datasets.dataset_builder import inspect_snapshot

    snapshot = inspect_snapshot(snapshot_path)
    source = StatisticalFeatureSource(features_delta, source_version).read()
    if (
        str(source.delta_path) != snapshot["source_delta_path"]
        or source.delta_table_id != snapshot["source_delta_table_id"]
        or source.delta_version != snapshot["source_delta_version"]
        or source.source_schema_fingerprint != snapshot["source_schema_fingerprint"]
    ):
        raise MonitoringError("Requested feature Delta lineage differs from the trained candidate")
    all_rows = cast(list[dict[str, object]], source.table.to_pylist())
    selected = sorted(
        (row for row in all_rows if start <= cast(datetime, row["ingestion_time"]) < end),
        key=lambda row: (cast(datetime, row["ingestion_time"]), cast(str, row["event_id"])),
    )
    if not selected:
        raise MonitoringError("Scoring window has no eligible feature rows")
    projections = [{name: row[name] for name in FEATURES} for row in selected]
    probabilities = score_candidate(candidate, projections, tracking=registry.tracking)
    candidate_id = cast(str, registration["candidate_id"])
    source_columns = tuple(source.table.column_names)
    predictions = [
        {
            "event_id": row["event_id"],
            "ingestion_time": row["ingestion_time"],
            "candidate_id": candidate_id,
            "fraud_probability_estimate": probability,
            "currency": row["currency"],
            "country": row["country"],
            "transaction_type": row["transaction_type"],
            "source_feature_row_fingerprint": row_fingerprint(row, source_columns),
        }
        for row, probability in zip(selected, probabilities, strict=True)
    ]
    if {cast(str, row["event_id"]) for row in selected} != {
        cast(str, row["event_id"]) for row in predictions
    }:
        raise MonitoringError("Source-to-ledger event IDs differ")
    comparison = _archived_comparison(predictions, training_path)
    basis: dict[str, object] = {
        "ledger_contract_version": LEDGER_VERSION,
        "scoring_contract_version": SCORING_VERSION,
        "candidate_id": candidate_id,
        "registered_model_name": registry.model_name,
        "registered_model_version": model_version,
        "model_tree_sha256": registration["model_tree_sha256"],
        "input_contract_fingerprint": INPUT_FINGERPRINT,
        "source_delta_path": str(source.delta_path),
        "source_delta_table_id": source.delta_table_id,
        "source_delta_version": source.delta_version,
        "source_schema_fingerprint": source.source_schema_fingerprint,
        "score_start": utc_text(start),
        "score_end": utc_text(end),
        "scored_input_fingerprint": rows_fingerprint(selected, source_columns),
        "runtime_versions": _runtime(),
    }
    ledger_id = identity(basis)
    manifest: dict[str, object] = {
        **basis,
        "ledger_id": ledger_id,
        "source_rows": len(all_rows),
        "eligible_rows": len(selected),
        "scored_rows": len(predictions),
        "excluded_before_window": sum(
            cast(datetime, row["ingestion_time"]) < start for row in all_rows
        ),
        "excluded_after_window": sum(
            cast(datetime, row["ingestion_time"]) >= end for row in all_rows
        ),
        "minimum_ingestion_time": utc_text(cast(datetime, selected[0]["ingestion_time"])),
        "maximum_ingestion_time": utc_text(cast(datetime, selected[-1]["ingestion_time"])),
        "positive_class": {"label": 1, "meaning": "FRAUD"},
        "prediction_fingerprint": rows_fingerprint(predictions, ROW_COLUMNS),
        "archived_prediction_comparison": comparison,
        "scope": (
            "Offline Synthetic Historical Replay; not historical online availability or serving"
        ),
    }
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / ledger_id
    if target.exists():
        existing = inspect_ledger(target)
        if {k: v for k, v in existing.items() if k != "predictions_parquet_sha256"} != manifest:
            raise MonitoringError("Existing ledger conflicts with verified replay")
        return target
    target.mkdir()  # Exclusive claim; incomplete publication remains visible.
    table = pa.Table.from_pylist(predictions, schema=LEDGER_SCHEMA)
    with (target / "predictions.parquet").open("xb") as stream:
        pq.write_table(table, stream, version="2.6", compression="zstd", use_dictionary=False)
    manifest["predictions_parquet_sha256"] = sha256((target / "predictions.parquet").read_bytes())
    with (target / "manifest.json").open("xb") as stream:
        stream.write(canonical_json(manifest))
    inspect_ledger(target)
    return target


def inspect_ledger(path: Path) -> dict[str, object]:
    artifact_directory(path, LEDGER_FILES)
    try:
        raw = (path / "manifest.json").read_bytes()
        manifest = object_value(json.loads(raw), "ledger manifest")
        if raw != canonical_json(manifest):
            raise MonitoringError("Ledger manifest is not canonical")
        if manifest["predictions_parquet_sha256"] != sha256(
            (path / "predictions.parquet").read_bytes()
        ):
            raise MonitoringError("Ledger Parquet hash differs")
        table = pq.read_table(path / "predictions.parquet")
        if [(f.name, f.type) for f in table.schema] != [(f.name, f.type) for f in LEDGER_SCHEMA]:
            raise MonitoringError("Ledger schema differs")
        rows = cast(list[dict[str, object]], table.to_pylist())
        ids = [cast(str, row["event_id"]) for row in rows]
        if len(ids) != len(set(ids)) or len(rows) != manifest["scored_rows"] or not rows:
            raise MonitoringError("Ledger grain/count differs")
        if rows != sorted(
            rows,
            key=lambda row: (cast(datetime, row["ingestion_time"]), cast(str, row["event_id"])),
        ):
            raise MonitoringError("Ledger ordering differs")
        start = parse_utc_timestamp(cast(str, manifest["score_start"]))
        end = parse_utc_timestamp(cast(str, manifest["score_end"]))
        for row in rows:
            if (
                not start <= cast(datetime, row["ingestion_time"]) < end
                or row["candidate_id"] != manifest["candidate_id"]
                or not isinstance(row["fraud_probability_estimate"], float)
                or not math.isfinite(row["fraud_probability_estimate"])
                or not 0 <= row["fraud_probability_estimate"] <= 1
            ):
                raise MonitoringError("Ledger row violates scoring contract")
        if (
            manifest["ledger_contract_version"] != LEDGER_VERSION
            or manifest["scoring_contract_version"] != SCORING_VERSION
        ):
            raise MonitoringError("Unsupported ledger contract")
        if manifest["ledger_id"] != path.name or manifest["ledger_id"] != identity(
            {key: manifest[key] for key in IDENTITY_FIELDS}
        ):
            raise MonitoringError("Ledger identity differs")
        if manifest["prediction_fingerprint"] != rows_fingerprint(rows, ROW_COLUMNS):
            raise MonitoringError("Ledger logical predictions differ")
        source = StatisticalFeatureSource(
            Path(cast(str, manifest["source_delta_path"])),
            cast(int, manifest["source_delta_version"]),
        ).read()
        if (
            source.delta_table_id != manifest["source_delta_table_id"]
            or source.source_schema_fingerprint != manifest["source_schema_fingerprint"]
        ):
            raise MonitoringError("Pinned ledger source identity differs")
        full = cast(list[dict[str, object]], source.table.to_pylist())
        selected = sorted(
            (row for row in full if start <= cast(datetime, row["ingestion_time"]) < end),
            key=lambda row: (cast(datetime, row["ingestion_time"]), cast(str, row["event_id"])),
        )
        if (
            len(full) != manifest["source_rows"]
            or len(selected) != len(rows)
            or [row["event_id"] for row in selected] != ids
            or manifest["scored_input_fingerprint"]
            != rows_fingerprint(selected, tuple(source.table.column_names))
            or any(
                predicted["source_feature_row_fingerprint"]
                != row_fingerprint(original, tuple(source.table.column_names))
                for original, predicted in zip(selected, rows, strict=True)
            )
        ):
            raise MonitoringError("Source-to-ledger event reconciliation differs")
        return manifest
    except MonitoringError:
        raise
    except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as error:
        raise MonitoringError(f"Cannot inspect prediction ledger: {error}") from error
