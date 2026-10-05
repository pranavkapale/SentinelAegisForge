"""Local, in-memory snapshot construction; no training, balancing or split assignment."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from .dataset_contract import (
    CANDIDATE_FEATURE_COLUMNS,
    DATASET_CONTRACT_VERSION,
    DEFERRED_IDENTIFIER_COLUMNS,
    NON_FEATURE_COLUMNS,
    SOURCE_SCHEMA,
    feature_rows,
)
from .errors import LabelContractError, SnapshotConflictError
from .feature_source import StatisticalFeatureSource
from .labels import LABEL_RESOLUTION_VERSION, load_labels, require_utc, resolve_labels, utc_text
from .manifest import (
    SNAPSHOT_IDENTITY_VERSION,
    canonical_json,
    logical_dataset_fingerprint,
    runtime_versions,
)

LABEL_FIELDS = (
    pa.field("fraud_label", pa.int8(), nullable=False),
    pa.field("label_observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("label_source", pa.string(), nullable=False),
    pa.field("label_revision", pa.int64(), nullable=False),
)
IDENTITY_FIELDS = (
    "snapshot_identity_version",
    "dataset_contract_version",
    "source_delta_path",
    "source_delta_table_id",
    "source_delta_version",
    "source_schema_fingerprint",
    "as_of_time",
    "label_source_file",
    "label_source_sha256",
    "label_resolution_contract_version",
    "logical_dataset_fingerprint",
    "runtime_versions",
)


@dataclass(frozen=True)
class DatasetSnapshot:
    path: Path
    manifest: dict[str, object]


def build_dataset(
    features_delta: Path,
    labels_path: Path,
    as_of: datetime,
    output_root: Path,
    source_version: int | None = None,
) -> DatasetSnapshot:
    as_of = require_utc(as_of)
    source = StatisticalFeatureSource(features_delta, source_version).read()
    rows = feature_rows(source.table)
    labels = load_labels(labels_path)
    by_id = {cast(str, row["event_id"]): row for row in rows}
    # Validate even future revisions of known events; corrupt histories are never repaired.
    for label in labels.records:
        feature = by_id.get(str(label.event_id))
        if feature is not None and label.label_observed_at < cast(
            datetime, feature["ingestion_time"]
        ):
            raise LabelContractError(f"{label.event_id}: label_observed_at precedes ingestion_time")
    available = [row for row in rows if cast(datetime, row["ingestion_time"]) <= as_of]
    available_ids = {cast(str, row["event_id"]) for row in available}
    selected = resolve_labels(labels, as_of)
    unmatched = selected.keys() - available_ids
    if unmatched:
        raise LabelContractError(f"Eligible labels have no available feature: {sorted(unmatched)}")
    ordered = sorted(
        (row for row in available if row["event_id"] in selected),
        key=lambda row: (cast(datetime, row["event_time"]), cast(str, row["event_id"])),
    )
    joined: list[dict[str, object]] = []
    for row in ordered:
        label = selected[cast(str, row["event_id"])]
        joined.append(
            {
                **row,
                "fraud_label": label.outcome.fraud_label,
                "label_observed_at": label.label_observed_at,
                "label_source": label.label_source,
                "label_revision": label.label_revision,
            }
        )
    output_schema = pa.schema([*source.table.schema, *LABEL_FIELDS])
    table = pa.Table.from_pylist(joined, schema=output_schema)
    logical_fingerprint = logical_dataset_fingerprint(table)
    runtimes = runtime_versions()
    identity: dict[str, object] = {
        "snapshot_identity_version": SNAPSHOT_IDENTITY_VERSION,
        "dataset_contract_version": DATASET_CONTRACT_VERSION,
        "source_delta_path": str(source.delta_path),
        "source_delta_table_id": source.delta_table_id,
        "source_delta_version": source.delta_version,
        "source_schema_fingerprint": source.source_schema_fingerprint,
        "as_of_time": utc_text(as_of),
        "label_source_file": str(labels.path),
        "label_source_sha256": labels.sha256,
        "label_resolution_contract_version": LABEL_RESOLUTION_VERSION,
        "logical_dataset_fingerprint": logical_fingerprint,
        "runtime_versions": runtimes,
    }
    snapshot_id = hashlib.sha256(canonical_json(identity)).hexdigest()
    positive = sum(label.outcome.fraud_label for label in selected.values())
    manifest: dict[str, object] = {
        **identity,
        "snapshot_id": snapshot_id,
        "source_delta_protocol": source.protocol,
        "source_schema": [
            {"name": f.name, "logical_type": str(f.type)} for f in source.table.schema
        ],
        "feature_rows_in_delta_version": source.row_count,
        "feature_rows_available_as_of": len(available),
        "duplicate_source_ids": 0,
        "label_records_total": len(labels.records),
        "eligible_label_records": sum(label.label_observed_at <= as_of for label in labels.records),
        "selected_labels": len(selected),
        "labeled_dataset_rows": len(joined),
        "positive_rows": positive,
        "negative_rows": len(joined) - positive,
        "unlabeled_feature_rows_excluded": len(available) - len(joined),
        "unmatched_label_rows": 0,
        "min_event_time": utc_text(cast(datetime, ordered[0]["event_time"])) if ordered else None,
        "max_event_time": utc_text(cast(datetime, ordered[-1]["event_time"])) if ordered else None,
        "candidate_feature_columns": list(CANDIDATE_FEATURE_COLUMNS),
        "non_feature_columns": list(NON_FEATURE_COLUMNS),
        "deferred_identifier_columns": list(DEFERRED_IDENTIFIER_COLUMNS),
        "row_order": ["event_time", "event_id"],
    }
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / snapshot_id
    try:
        target.mkdir()  # Exclusive claim; never overwrite an existing or incomplete directory.
    except FileExistsError:
        existing = inspect_snapshot(target)
        expected = {**manifest, "dataset_parquet_sha256": existing["dataset_parquet_sha256"]}
        if existing != expected:
            raise SnapshotConflictError(f"Existing snapshot manifest differs: {target}")
        existing_table = pq.read_table(target / "dataset.parquet")
        if not existing_table.equals(table, check_metadata=False):
            raise SnapshotConflictError(f"Existing snapshot logical data differs: {target}")
        return DatasetSnapshot(target, existing)

    with (target / "dataset.parquet").open("xb") as handle:
        pq.write_table(table, handle, version="2.6", compression="zstd", use_dictionary=False)
    manifest["dataset_parquet_sha256"] = hashlib.sha256(
        (target / "dataset.parquet").read_bytes()
    ).hexdigest()
    with (target / "manifest.json").open("xb") as handle:
        handle.write(canonical_json(manifest))
    # Interrupted writes remain visibly incomplete, not silently repaired on the next build.
    inspect_snapshot(target)
    return DatasetSnapshot(target, manifest)


def inspect_snapshot(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_dir():
        raise SnapshotConflictError(f"Expected an immutable snapshot directory: {path}")
    if {entry.name for entry in path.iterdir()} != {"dataset.parquet", "manifest.json"}:
        raise SnapshotConflictError(f"Incomplete/unexpected snapshot artifacts: {path}")
    if any((path / name).is_symlink() for name in ("dataset.parquet", "manifest.json")):
        raise SnapshotConflictError("Snapshot artifacts must not be symlinks")
    try:
        value: object = json.loads((path / "manifest.json").read_bytes())
        if not isinstance(value, dict):
            raise SnapshotConflictError("Manifest must be a JSON object")
        manifest = cast(dict[str, object], value)
        table = pq.read_table(path / "dataset.parquet")
        if manifest["dataset_contract_version"] != DATASET_CONTRACT_VERSION:
            raise SnapshotConflictError("Unsupported dataset contract version")
        expected_types = [(f.name, f.type) for f in [*SOURCE_SCHEMA, *LABEL_FIELDS]]
        if [(f.name, f.type) for f in table.schema] != expected_types:
            raise SnapshotConflictError("Unexpected dataset schema")
        if (
            manifest["dataset_parquet_sha256"]
            != hashlib.sha256((path / "dataset.parquet").read_bytes()).hexdigest()
        ):
            raise SnapshotConflictError("Parquet content hash differs from manifest")
        if manifest["logical_dataset_fingerprint"] != logical_dataset_fingerprint(table):
            raise SnapshotConflictError("Dataset logical fingerprint differs from manifest")
        if manifest["labeled_dataset_rows"] != table.num_rows:
            raise SnapshotConflictError("Dataset row count differs from manifest")
        outcomes = cast(list[int], table["fraud_label"].to_pylist())
        if any(type(value) is not int or value not in (0, 1) for value in outcomes):
            raise SnapshotConflictError("Invalid persisted fraud_label")
        if manifest["positive_rows"] != sum(outcomes) or manifest["negative_rows"] != len(
            outcomes
        ) - sum(outcomes):
            raise SnapshotConflictError("Dataset class counts differ from manifest")
        if manifest["snapshot_id"] != path.name:
            raise SnapshotConflictError("Snapshot directory identity differs from manifest")
        identity = {name: manifest[name] for name in IDENTITY_FIELDS}
        if manifest["snapshot_id"] != hashlib.sha256(canonical_json(identity)).hexdigest():
            raise SnapshotConflictError("Snapshot canonical identity differs from manifest")
        return manifest
    except SnapshotConflictError:
        raise
    except (OSError, ValueError, KeyError, pa.ArrowException) as error:
        raise SnapshotConflictError(f"Cannot verify snapshot {path}: {error}") from error
