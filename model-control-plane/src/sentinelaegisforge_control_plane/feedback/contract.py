"""Strict normalized feedback envelope; source names are provenance, not trust proof."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from sentinelaegisforge_control_plane.datasets.labels import (
    parse_utc_timestamp,
    require_utc,
    utc_text,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

ENVELOPE_VERSION = "outcome-feedback-envelope-v1"
LEDGER_VERSION = "outcome-feedback-ledger-v1"
FIELDS = frozenset(
    {
        "provider_id",
        "provider_record_id",
        "provider_case_id",
        "source_revision",
        "event_id",
        "status",
        "source_updated_at",
        "received_at",
    }
)


class FeedbackError(ValueError):
    """Invalid feedback, source lineage, or immutable artifact."""


class FeedbackStatus(StrEnum):
    PENDING = "PENDING"
    CONFIRMED_FRAUD = "CONFIRMED_FRAUD"
    CONFIRMED_LEGIT = "CONFIRMED_LEGIT"
    RETRACTED = "RETRACTED"


@dataclass(frozen=True)
class FeedbackRecord:
    provider_id: str
    provider_record_id: str
    provider_case_id: str
    source_revision: int
    event_id: UUID
    status: FeedbackStatus
    source_updated_at: datetime
    received_at: datetime

    def __post_init__(self) -> None:
        for name in ("provider_id", "provider_record_id", "provider_case_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise FeedbackError(f"{name} must be a nonblank, trimmed string")
        if type(self.source_revision) is not int or not 1 <= self.source_revision <= 2**63 - 1:
            raise FeedbackError("source_revision must be a positive int64")
        if not isinstance(self.event_id, UUID) or not isinstance(self.status, FeedbackStatus):
            raise FeedbackError("event_id/status must use UUID/FeedbackStatus types")
        try:
            updated = require_utc(self.source_updated_at)
            received = require_utc(self.received_at)
        except ValueError as error:
            raise FeedbackError(str(error)) from error
        if updated > received:
            raise FeedbackError("source_updated_at must not follow received_at")
        object.__setattr__(self, "source_updated_at", updated)
        object.__setattr__(self, "received_at", received)

    def as_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "provider_record_id": self.provider_record_id,
            "provider_case_id": self.provider_case_id,
            "source_revision": self.source_revision,
            "event_id": str(self.event_id),
            "status": self.status.value,
            "source_updated_at": utc_text(self.source_updated_at),
            "received_at": utc_text(self.received_at),
        }


def _unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise FeedbackError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def parse_record(value: object) -> FeedbackRecord:
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise FeedbackError("Expected exactly the eight feedback envelope fields")
    if any(type(value[name]) is not str for name in FIELDS - {"source_revision"}):
        raise FeedbackError("Feedback identity/status/timestamp fields must be strings")
    if type(value["source_revision"]) is not int:
        raise FeedbackError("source_revision must be an integer, not bool/float")
    try:
        event_id = UUID(value["event_id"])
        if str(event_id) != value["event_id"]:
            raise FeedbackError("event_id must be a canonical UUID")
        return FeedbackRecord(
            value["provider_id"],
            value["provider_record_id"],
            value["provider_case_id"],
            value["source_revision"],
            event_id,
            FeedbackStatus(value["status"]),
            parse_utc_timestamp(value["source_updated_at"]),
            parse_utc_timestamp(value["received_at"]),
        )
    except (TypeError, ValueError) as error:
        if isinstance(error, FeedbackError):
            raise
        raise FeedbackError(f"Invalid feedback envelope: {error}") from error


def parse_jsonl(raw: bytes) -> tuple[FeedbackRecord, ...]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise FeedbackError("Feedback batch must be UTF-8 JSONL") from error
    if not text or not text.endswith("\n"):
        raise FeedbackError("Feedback JSONL must be nonempty and newline-terminated")
    records: list[FeedbackRecord] = []
    for number, line in enumerate(text.splitlines(), 1):
        try:
            records.append(parse_record(json.loads(line, object_pairs_hook=_unique_fields)))
        except (ValueError, TypeError) as error:
            raise FeedbackError(f"Invalid feedback line {number}: {error}") from error
    return tuple(records)


def load_batch(path: Path) -> tuple[tuple[FeedbackRecord, ...], str, Path]:
    if path.is_symlink():
        raise FeedbackError("Feedback batch must not be a symlink")
    try:
        resolved = path.resolve(strict=True)
        raw = resolved.read_bytes()
    except OSError as error:
        raise FeedbackError(f"Cannot read feedback batch: {error}") from error
    if not resolved.is_file():
        raise FeedbackError("Feedback batch must be a regular file")
    return parse_jsonl(raw), hashlib.sha256(raw).hexdigest(), resolved


def records_bytes(records: tuple[FeedbackRecord, ...]) -> bytes:
    return b"".join(canonical_json(record.as_dict()) for record in records)
