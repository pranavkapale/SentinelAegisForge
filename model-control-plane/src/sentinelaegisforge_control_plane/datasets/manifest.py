"""Deterministic snapshot metadata and logical (not cross-version Parquet-byte) identity."""

import hashlib
import json
import math
import platform
from datetime import datetime
from decimal import Decimal
from importlib.metadata import version
from typing import cast

import pyarrow as pa

from .dataset_contract import schema_fingerprint
from .errors import SnapshotConflictError
from .labels import utc_text

SNAPSHOT_IDENTITY_VERSION = "offline-snapshot-v1"
LOGICAL_FINGERPRINT_VERSION = "ordered-arrow-rows-v1"


def canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        + "\n"
    ).encode("utf-8")


def runtime_versions() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "deltalake": version("deltalake"),
        "pyarrow": version("pyarrow"),
    }


def _logical_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, datetime):
        return {"utc_microseconds": utc_text(value)}
    if isinstance(value, Decimal):
        return {"decimal": format(value, "f")}
    if isinstance(value, float) and math.isfinite(value):
        return {"float64_hex": value.hex()}
    raise SnapshotConflictError(f"Unsupported/corrupt dataset logical value: {value!r}")


def logical_dataset_fingerprint(table: pa.Table) -> str:
    digest = hashlib.sha256()
    digest.update(
        canonical_json(
            {
                "version": LOGICAL_FINGERPRINT_VERSION,
                "schema_fingerprint": schema_fingerprint(table.schema),
            }
        )
    )
    rows = cast(list[dict[str, object]], table.to_pylist())
    for row in rows:
        # Ordered values include every source column and all four label/provenance columns.
        digest.update(canonical_json([_logical_value(row[name]) for name in table.column_names]))
    return digest.hexdigest()
