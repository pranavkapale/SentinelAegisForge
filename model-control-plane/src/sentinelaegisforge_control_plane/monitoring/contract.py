"""Small, versioned Phase 19 artifact and support contracts."""

import hashlib
import math
from datetime import datetime
from pathlib import Path
from typing import cast

import pyarrow as pa

from sentinelaegisforge_control_plane.datasets.labels import require_utc, utc_text
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

LEDGER_VERSION = "offline-prediction-ledger-v1"
SCORING_VERSION = "trusted-candidate-replay-v1"
MONITORING_VERSION = "delayed-outcome-monitoring-v1"
METRICS_VERSION = "delayed-outcome-metrics-v1"
SEGMENT_VERSION = "descriptive-segments-v1"
LEDGER_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("ingestion_time", pa.timestamp("us", tz="UTC")),
        ("candidate_id", pa.string()),
        ("fraud_probability_estimate", pa.float64()),
        ("currency", pa.string()),
        ("country", pa.string()),
        ("transaction_type", pa.string()),
        ("source_feature_row_fingerprint", pa.string()),
    ]
)
OUTCOME_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("outcome", pa.string()),
        ("label_observed_at", pa.timestamp("us", tz="UTC")),
        ("label_source", pa.string()),
        ("label_revision", pa.int64()),
    ]
)
SEGMENTS = ("currency", "country", "transaction_type")


class MonitoringError(ValueError):
    """A replay, artifact, outcome, or source contract is invalid."""


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def identity(value: object) -> str:
    return sha256(canonical_json(value))


def logical(value: object) -> object:
    """Canonical logical values; exact decimals and float bits survive hashing."""
    from decimal import Decimal

    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, datetime):
        return {"utc_microseconds": utc_text(value)}
    if isinstance(value, Decimal):
        return {"decimal": format(value, "f")}
    if isinstance(value, float) and math.isfinite(value):
        return {"float64_hex": value.hex()}
    raise MonitoringError(f"Unsupported logical value: {value!r}")


def row_fingerprint(row: dict[str, object], columns: tuple[str, ...]) -> str:
    return identity([logical(row[name]) for name in columns])


def rows_fingerprint(rows: list[dict[str, object]], columns: tuple[str, ...]) -> str:
    return identity(
        {"columns": columns, "rows": [[logical(row[name]) for name in columns] for row in rows]}
    )


def utc_window(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start, end = require_utc(start), require_utc(end)
    if start >= end:
        raise MonitoringError("score_start must precede score_end")
    return start, end


def artifact_directory(path: Path, names: set[str]) -> None:
    if path.is_symlink() or not path.is_dir() or {item.name for item in path.iterdir()} != names:
        raise MonitoringError(f"Incomplete or unexpected immutable artifacts: {path}")
    if any((path / name).is_symlink() for name in names):
        raise MonitoringError("Immutable artifacts may not be symlinks")


def object_value(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise MonitoringError(f"{name} must be an object")
    return cast(dict[str, object], value)


def support_config(min_rows: int, min_per_class: int) -> dict[str, int]:
    if (
        type(min_rows) is not int
        or type(min_per_class) is not int
        or min_rows < 2
        or min_per_class < 1
    ):
        raise MonitoringError("Metric support requires at least two rows and one per class")
    return {"minimum_rows": min_rows, "minimum_per_class": min_per_class}
