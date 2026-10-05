"""Independent labels, not deterministic risk-policy outputs."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from .errors import LabelContractError

LABEL_RESOLUTION_VERSION = "delayed-outcome-v1"
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)")


def require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise LabelContractError("Timestamp must be timezone-aware UTC")
    return value.astimezone(UTC)


def parse_utc_timestamp(value: str) -> datetime:
    """Strict UTC seconds with at most microsecond precision; never truncate nanos."""
    if not _UTC_TIMESTAMP.fullmatch(value):
        raise LabelContractError(
            f"Expected UTC RFC3339 timestamp (up to 6 fractional digits): {value}"
        )
    try:
        return require_utc(datetime.fromisoformat(value))
    except ValueError as error:
        raise LabelContractError(f"Invalid UTC timestamp: {value}") from error


def utc_text(value: datetime) -> str:
    return require_utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


class Outcome(StrEnum):
    FRAUD = "FRAUD"
    LEGIT = "LEGIT"

    @property
    def fraud_label(self) -> int:
        return 1 if self is Outcome.FRAUD else 0


@dataclass(frozen=True)
class TransactionOutcomeLabel:
    event_id: UUID
    outcome: Outcome
    label_observed_at: datetime
    label_source: str
    label_revision: int

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, UUID) or not isinstance(self.outcome, Outcome):
            raise LabelContractError("Label identity/outcome must use UUID/Outcome types")
        object.__setattr__(self, "label_observed_at", require_utc(self.label_observed_at))
        if not self.label_source.strip():
            raise LabelContractError("label_source must not be blank")
        if type(self.label_revision) is not int or not 1 <= self.label_revision <= 2**63 - 1:
            raise LabelContractError("label_revision must be an integer in [1, 2^63-1]")


@dataclass(frozen=True)
class LabelInput:
    path: Path
    sha256: str
    records: tuple[TransactionOutcomeLabel, ...]


def _unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise LabelContractError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def load_labels(path: Path) -> LabelInput:
    resolved = path.resolve(strict=True)
    raw = resolved.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise LabelContractError("Labels must be UTF-8 JSONL") from error
    records: list[TransactionOutcomeLabel] = []
    fields = {"event_id", "outcome", "label_observed_at", "label_source", "label_revision"}
    for line_number, line in enumerate(text.splitlines(), start=1):
        try:
            data: object = json.loads(line, object_pairs_hook=_unique_fields)
            if not isinstance(data, dict) or set(data) != fields:
                raise LabelContractError("Expected exactly the five label contract fields")
            if not all(isinstance(data[field], str) for field in fields - {"label_revision"}):
                raise LabelContractError("Identity/outcome/timestamp/source must be strings")
            if type(data["label_revision"]) is not int:
                raise LabelContractError("label_revision must be an integer, not boolean/float")
            records.append(
                TransactionOutcomeLabel(
                    UUID(data["event_id"]),
                    Outcome(data["outcome"]),
                    parse_utc_timestamp(data["label_observed_at"]),
                    data["label_source"],
                    data["label_revision"],
                )
            )
        except (ValueError, TypeError, KeyError) as error:
            raise LabelContractError(f"Invalid label at line {line_number}: {error}") from error
    _validate_revisions(records)
    return LabelInput(resolved, hashlib.sha256(raw).hexdigest(), tuple(records))


def _validate_revisions(records: list[TransactionOutcomeLabel]) -> None:
    histories: dict[UUID, list[TransactionOutcomeLabel]] = {}
    for record in records:
        histories.setdefault(record.event_id, []).append(record)
    for event_id, history in histories.items():
        ordered = sorted(history, key=lambda label: label.label_revision)
        revisions = [label.label_revision for label in ordered]
        if revisions != list(range(1, len(ordered) + 1)):
            raise LabelContractError(
                f"{event_id}: revisions must be unique, contiguous and start at 1"
            )
        if any(
            later.label_observed_at < earlier.label_observed_at
            for earlier, later in zip(ordered, ordered[1:])
        ):
            raise LabelContractError(f"{event_id}: observation times contradict revision order")


def resolve_labels(labels: LabelInput, as_of: datetime) -> dict[str, TransactionOutcomeLabel]:
    as_of = require_utc(as_of)
    selected: dict[str, TransactionOutcomeLabel] = {}
    for label in labels.records:
        key = str(label.event_id)
        if label.label_observed_at <= as_of:
            previous = selected.get(key)
            if previous is None or label.label_revision > previous.label_revision:
                selected[key] = label
    return selected
