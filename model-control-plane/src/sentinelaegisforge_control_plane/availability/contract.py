"""Canonical identities and strict artifact parsing for local feature handoff."""

import hashlib
import json
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

READY_VERSION = "feature-availability-v1"
SEGMENT_VERSION = "feature-commit-segment-v1"
OUTBOX_VERSION = "feature-ready-outbox-v1"
DISCOVERY_VERSION = "delta-snapshot-difference-v1"
RECEIPT_VERSION = "feature-ready-receipt-v1"
MAX_READY_EVENTS = 5000


class AvailabilityError(ValueError):
    """Committed source, immutable projection, or local receipt violates its contract."""


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def lines(records: list[dict[str, object]]) -> bytes:
    return b"".join(canonical_json(record) for record in records)


def read_object(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
        if not isinstance(value, dict) or raw != canonical_json(value):
            raise AvailabilityError(f"Noncanonical object: {path}")
        return cast(dict[str, object], value)
    except (OSError, ValueError, TypeError) as error:
        raise AvailabilityError(f"Invalid artifact {path}: {error}") from error


def read_lines(path: Path) -> list[dict[str, object]]:
    try:
        raw = path.read_bytes()
        values = [json.loads(line) for line in raw.splitlines()]
        if any(not isinstance(value, dict) for value in values) or raw != lines(values):
            raise AvailabilityError(f"Noncanonical JSONL: {path}")
        return cast(list[dict[str, object]], values)
    except (OSError, ValueError, TypeError) as error:
        raise AvailabilityError(f"Invalid JSONL {path}: {error}") from error


def exact_directory(path: Path, names: set[str]) -> None:
    if path.is_symlink() or not path.is_dir() or {item.name for item in path.iterdir()} != names:
        raise AvailabilityError(f"Incomplete or unexpected immutable directory: {path}")
    if any((path / name).is_symlink() or not (path / name).is_file() for name in names):
        raise AvailabilityError(f"Linked or non-file artifact in {path}")


def publish(path: Path, artifacts: dict[str, bytes]) -> None:
    """Claim exclusively; a crash leaves visible incomplete output, never an overwrite."""
    if path.exists():
        raise AvailabilityError(f"Immutable artifact already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.mkdir()
    for name, content in artifacts.items():
        (path / name).write_bytes(content)
