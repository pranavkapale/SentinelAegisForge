"""Verified Phase 24 outbox to immutable, bounded Kafka publication batch."""

import hashlib
from pathlib import Path
from typing import cast

from .contract import (
    MAX_READY_EVENTS,
    AvailabilityError,
    exact_directory,
    lines,
    publish,
    read_object,
)
from .outbox import canonical, inspect_outbox

WIRE_VERSION = "feature-ready-kafka-wire-v1"
BATCH_VERSION = "feature-ready-kafka-publish-batch-v1"
TOPIC = "features.ready"
MAX_RECORD_BYTES = 4096
BATCH_FILES = {"publish_batch_manifest.json", "records.jsonl"}


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _expected(outbox: Path, topic: str, maximum: int) -> tuple[dict[str, object], bytes]:
    if topic != TOPIC:
        raise AvailabilityError(f"Unsupported local feature-ready topic: {topic}")
    if type(maximum) is not int or not 1 <= maximum <= MAX_READY_EVENTS:
        raise AvailabilityError(f"maximum must be in [1,{MAX_READY_EVENTS}]")
    outbox_manifest, events = inspect_outbox(outbox)
    if not events or len(events) > maximum:
        raise AvailabilityError("Verified outbox is empty or exceeds publication maximum")
    raw = lines(events)
    if any(len(canonical(event)) > MAX_RECORD_BYTES for event in events):
        raise AvailabilityError("Feature-ready Kafka envelope exceeds maximum record bytes")
    basis: dict[str, object] = {
        "publication_batch_contract_version": BATCH_VERSION,
        "wire_contract_version": WIRE_VERSION,
        "topic": topic,
        "outbox_path": str(outbox.resolve()),
        "outbox_id": outbox_manifest["outbox_id"],
        "outbox_manifest_sha256": _sha((outbox / "outbox_manifest.json").read_bytes()),
        "source_delta_table_id": outbox_manifest["source_delta_table_id"],
        "source_schema_fingerprint": outbox_manifest["source_schema_fingerprint"],
        "start_delta_version": outbox_manifest["start_delta_version"],
        "end_delta_version": outbox_manifest["end_delta_version"],
        "ready_event_count": len(events),
        "ready_event_ids_sha256": _sha(
            canonical(sorted(cast(str, event["ready_event_id"]) for event in events))
        ),
        "records_sha256": _sha(raw),
        "maximum_record_bytes": MAX_RECORD_BYTES,
    }
    return {**basis, "publication_batch_id": _sha(canonical(basis))}, raw


def prepare_batch(outbox: Path, root: Path, *, maximum: int, topic: str = TOPIC) -> Path:
    if outbox.is_symlink():
        raise AvailabilityError("Outbox path must not be a symlink")
    manifest, raw = _expected(outbox.resolve(), topic, maximum)
    target = root.resolve() / cast(str, manifest["publication_batch_id"])
    if target.exists():
        inspected = inspect_batch(target)
        if inspected != manifest:
            raise AvailabilityError("Existing publication batch conflicts")
        return target
    publish(
        target,
        {"publish_batch_manifest.json": canonical(manifest), "records.jsonl": raw},
    )
    inspect_batch(target)
    return target


def inspect_batch(path: Path) -> dict[str, object]:
    exact_directory(path, BATCH_FILES)
    manifest = read_object(path / "publish_batch_manifest.json")
    try:
        outbox = Path(cast(str, manifest["outbox_path"]))
        expected, raw = _expected(
            outbox,
            cast(str, manifest["topic"]),
            cast(int, manifest["ready_event_count"]),
        )
        if (
            path.name != expected["publication_batch_id"]
            or manifest != expected
            or (path / "records.jsonl").read_bytes() != raw
        ):
            raise AvailabilityError("Publication batch differs from verified Phase 24 outbox")
        return manifest
    except (KeyError, TypeError, OSError) as error:
        raise AvailabilityError(f"Invalid publication batch: {error}") from error
