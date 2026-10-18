"""Build and verify immutable, fixed-Delta-source outcome feedback ledgers."""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.monitoring.contract import (
    artifact_directory,
    identity,
    sha256,
)
from sentinelaegisforge_control_plane.monitoring.ledger import ROOT

from .contract import (
    ENVELOPE_VERSION,
    LEDGER_VERSION,
    FeedbackError,
    FeedbackRecord,
    load_batch,
    parse_jsonl,
    records_bytes,
)

DEFAULT_FEEDBACK_ROOT = ROOT / ".local/ml/outcome-feedback"
FILES = {"feedback.jsonl", "manifest.json"}
IDENTITY_FIELDS = (
    "ledger_contract_version",
    "envelope_contract_version",
    "source_delta_path",
    "source_delta_table_id",
    "source_delta_version",
    "source_schema_fingerprint",
    "event_universe_fingerprint",
    "base_ledger_id",
    "source_batches",
    "logical_records_fingerprint",
)


def _order(record: FeedbackRecord) -> tuple[str, str, int, str]:
    return (
        record.provider_id,
        record.provider_case_id,
        record.source_revision,
        record.provider_record_id,
    )


def _source(delta_path: Path, delta_version: int) -> tuple[dict[str, object], dict[str, datetime]]:
    if type(delta_version) is not int or delta_version < 0:
        raise FeedbackError("Explicit nonnegative Delta version is required")
    try:
        source = StatisticalFeatureSource(delta_path, delta_version).read()
    except ValueError as error:
        raise FeedbackError(f"Invalid fixed feature source: {error}") from error
    universe = {
        cast(str, row["event_id"]): cast(datetime, row["ingestion_time"])
        for row in source.table.to_pylist()
    }
    fields: dict[str, object] = {
        "source_delta_path": str(source.delta_path),
        "source_delta_table_id": source.delta_table_id,
        "source_delta_version": source.delta_version,
        "source_schema_fingerprint": source.source_schema_fingerprint,
        "event_universe_fingerprint": identity(
            [(event_id, ingestion.isoformat()) for event_id, ingestion in sorted(universe.items())]
        ),
    }
    return fields, universe


def _validate(
    records: tuple[FeedbackRecord, ...], universe: dict[str, datetime]
) -> tuple[tuple[FeedbackRecord, ...], int]:
    deliveries: dict[tuple[str, str], FeedbackRecord] = {}
    revisions: dict[tuple[str, str, int], FeedbackRecord] = {}
    cases: dict[tuple[str, str], list[FeedbackRecord]] = defaultdict(list)
    duplicates = 0
    for record in records:
        ingestion = universe.get(str(record.event_id))
        if ingestion is None:
            raise FeedbackError(f"Unknown source event_id: {record.event_id}")
        if record.received_at < ingestion:
            raise FeedbackError(f"received_at precedes transaction ingestion: {record.event_id}")
        delivery = (record.provider_id, record.provider_record_id)
        previous = deliveries.get(delivery)
        if previous is not None:
            if previous != record:
                raise FeedbackError(f"Conflicting provider delivery identity: {delivery}")
            duplicates += 1
            continue
        revision = (record.provider_id, record.provider_case_id, record.source_revision)
        if revision in revisions:
            raise FeedbackError(f"Duplicate provider case revision: {revision}")
        deliveries[delivery] = record
        revisions[revision] = record
        cases[(record.provider_id, record.provider_case_id)].append(record)
    for case, history in cases.items():
        ordered = sorted(history, key=lambda item: item.source_revision)
        if [item.source_revision for item in ordered] != list(range(1, len(ordered) + 1)):
            raise FeedbackError(f"Missing/noncontiguous source revision: {case}")
        if len({item.event_id for item in ordered}) != 1:
            raise FeedbackError(f"Provider case changes event_id: {case}")
    return tuple(sorted(deliveries.values(), key=_order)), duplicates


def build_feedback_ledger(
    batch_path: Path,
    source_delta: Path,
    source_version: int,
    output_root: Path = DEFAULT_FEEDBACK_ROOT,
    *,
    base_ledger: Path | None = None,
) -> Path:
    """Validate all records before exclusive publication; an old ledger is never edited."""
    source_fields, universe = _source(source_delta, source_version)
    base_records: tuple[FeedbackRecord, ...] = ()
    batches: list[dict[str, str]] = []
    base_id: str | None = None
    if base_ledger is not None:
        base = inspect_feedback_ledger(base_ledger)
        if any(base[key] != value for key, value in source_fields.items()):
            raise FeedbackError("Base ledger uses an incompatible fixed Delta source")
        base_id = cast(str, base["feedback_ledger_id"])
        base_records = parse_jsonl((base_ledger / "feedback.jsonl").read_bytes())
        batches = cast(list[dict[str, str]], base["source_batches"]).copy()
    incoming, raw_hash, batch_file = load_batch(batch_path)
    ordered, duplicates = _validate((*base_records, *incoming), universe)
    batches.append({"path": str(batch_file), "sha256": raw_hash})
    logical_hash = sha256(records_bytes(ordered))
    basis: dict[str, object] = {
        "ledger_contract_version": LEDGER_VERSION,
        "envelope_contract_version": ENVELOPE_VERSION,
        **source_fields,
        "base_ledger_id": base_id,
        "source_batches": batches,
        "logical_records_fingerprint": logical_hash,
    }
    ledger_id = identity(basis)
    case_revisions: dict[tuple[str, str], int] = defaultdict(int)
    for record in ordered:
        case_revisions[(record.provider_id, record.provider_case_id)] += 1
    manifest: dict[str, object] = {
        **basis,
        "feedback_ledger_id": ledger_id,
        "record_count": len(ordered),
        "case_count": len(case_revisions),
        "provider_count": len({record.provider_id for record in ordered}),
        "duplicate_deliveries_suppressed_in_build": duplicates,
        "case_revision_coverage": [
            {
                "provider_id": provider_id,
                "provider_case_id": case_id,
                "revisions": count,
            }
            for (provider_id, case_id), count in sorted(case_revisions.items())
        ],
        "feedback_jsonl_sha256": logical_hash,
        "scope": "Normalized local provider-like fixture; source authenticity is unverified",
    }
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / ledger_id
    if target.exists():
        existing = inspect_feedback_ledger(target)
        if existing != manifest:
            raise FeedbackError("Existing feedback ledger conflicts with verified input")
        return target
    target.mkdir()  # An incomplete exclusive claim remains visible and fails inspection.
    with (target / "feedback.jsonl").open("xb") as stream:
        stream.write(records_bytes(ordered))
    with (target / "manifest.json").open("xb") as stream:
        stream.write(canonical_json(manifest))
    inspect_feedback_ledger(target)
    return target


def inspect_feedback_ledger(path: Path) -> dict[str, object]:
    try:
        artifact_directory(path, FILES)
        raw = (path / "manifest.json").read_bytes()
        value: object = json.loads(raw)
        if not isinstance(value, dict) or raw != canonical_json(value):
            raise FeedbackError("Feedback manifest is not canonical")
        manifest = cast(dict[str, object], value)
        if (
            manifest["ledger_contract_version"] != LEDGER_VERSION
            or manifest["envelope_contract_version"] != ENVELOPE_VERSION
        ):
            raise FeedbackError("Unsupported feedback ledger/envelope version")
        if (
            path.name != manifest["feedback_ledger_id"]
            or identity({name: manifest[name] for name in IDENTITY_FIELDS}) != path.name
        ):
            raise FeedbackError("Feedback ledger identity differs")
        records_raw = (path / "feedback.jsonl").read_bytes()
        records = parse_jsonl(records_raw)
        source_fields, universe = _source(
            Path(cast(str, manifest["source_delta_path"])),
            cast(int, manifest["source_delta_version"]),
        )
        if any(manifest[key] != val for key, val in source_fields.items()):
            raise FeedbackError("Pinned feedback source identity differs")
        ordered, duplicates = _validate(records, universe)
        if duplicates or records != ordered or records_raw != records_bytes(ordered):
            raise FeedbackError("Feedback records are duplicated, unordered or noncanonical")
        actual_hash = sha256(records_raw)
        if (
            actual_hash != manifest["feedback_jsonl_sha256"]
            or actual_hash != manifest["logical_records_fingerprint"]
        ):
            raise FeedbackError("Feedback artifact/logical hash differs")
        cases: dict[tuple[str, str], int] = defaultdict(int)
        for record in records:
            cases[(record.provider_id, record.provider_case_id)] += 1
        if (
            len(records) != manifest["record_count"]
            or len(cases) != manifest["case_count"]
            or len({record.provider_id for record in records}) != manifest["provider_count"]
            or [
                {
                    "provider_id": provider_id,
                    "provider_case_id": case_id,
                    "revisions": count,
                }
                for (provider_id, case_id), count in sorted(cases.items())
            ]
            != manifest["case_revision_coverage"]
        ):
            raise FeedbackError("Feedback count/revision coverage differs")
        batches = manifest["source_batches"]
        if not isinstance(batches, list) or not batches:
            raise FeedbackError("Feedback source batches are absent")
        for batch in batches:
            if not isinstance(batch, dict) or set(batch) != {"path", "sha256"}:
                raise FeedbackError("Feedback source batch provenance differs")
        return manifest
    except FeedbackError:
        raise
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise FeedbackError(f"Cannot inspect feedback ledger: {error}") from error
