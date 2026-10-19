"""Versioned identities and strict artifact helpers for local shadow execution."""

import hashlib
import json
import math
from pathlib import Path
from typing import cast

import pyarrow as pa

from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

PLAN_VERSION = "shadow-replay-plan-v1"
EXECUTION_VERSION = "shadow-execution-v1"
RUNTIME_VERSION = "bounded-local-scorer-v1"
PARITY_VERSION = "shadow-parity-v1"
SCOPE = "LOCAL_SYNTHETIC_SHADOW_REPLAY"
AVAILABILITY = "NOT_ESTABLISHED"
RESULT_SCHEMA = pa.schema(
    [
        ("request_id", pa.string()),
        ("event_id", pa.string()),
        ("replay_sequence", pa.int64()),
        ("candidate_id", pa.string()),
        ("model_version", pa.string()),
        ("execution_status", pa.string()),
        ("fraud_probability_estimate", pa.float64()),
        ("failure_code", pa.string()),
        ("source_feature_row_fingerprint", pa.string()),
    ]
)
TERMINAL = {"SCORED", "INPUT_REJECTED", "EXECUTION_FAILED"}


class ShadowError(ValueError):
    """Plan, trusted runtime, immutable artifact or parity contract failed."""


class RequestInferenceError(Exception):
    """Known request-local inference failure, safe to record and continue."""


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_object(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
        if not isinstance(value, dict) or raw != canonical_json(value):
            raise ShadowError(f"Noncanonical object: {path.name}")
        return cast(dict[str, object], value)
    except (OSError, ValueError, TypeError) as error:
        raise ShadowError(f"Invalid artifact {path.name}: {error}") from error


def exact_directory(path: Path, names: set[str]) -> None:
    if path.is_symlink() or not path.is_dir() or {item.name for item in path.iterdir()} != names:
        raise ShadowError(f"Incomplete or unexpected immutable artifact directory: {path}")
    if any((path / name).is_symlink() or not (path / name).is_file() for name in names):
        raise ShadowError("Immutable artifact contains linked/non-file entries")


def probability(value: object) -> float:
    if type(value) is not float or not math.isfinite(value) or not 0 <= value <= 1:
        raise ShadowError("Probability must be a finite float in [0,1]")
    return value
