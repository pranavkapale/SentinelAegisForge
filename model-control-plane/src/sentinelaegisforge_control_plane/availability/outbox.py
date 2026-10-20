"""Immutable commit segments and a reconciled, reconstructible local outbox."""

from pathlib import Path
from typing import cast

from .contract import (
    DISCOVERY_VERSION,
    MAX_READY_EVENTS,
    OUTBOX_VERSION,
    READY_VERSION,
    SEGMENT_VERSION,
    AvailabilityError,
    digest,
    exact_directory,
    file_hash,
    lines,
    publish,
    read_lines,
    read_object,
)
from .discovery import MAX_VERSIONS, VersionDifference, discover_versions

SEGMENT_FILES = {"segment_manifest.json", "ready_events.jsonl"}
OUTBOX_FILES = {"outbox_manifest.json", "ready_events.jsonl"}


def _ready_event(diff: VersionDifference, event_id: str) -> dict[str, object]:
    fingerprint = diff.new_rows[event_id]
    return {
        "availability_contract_version": READY_VERSION,
        "ready_event_id": digest(
            [READY_VERSION, diff.current.delta_table_id, event_id, diff.version, fingerprint]
        ),
        "event_id": event_id,
        "source_delta_table_id": diff.current.delta_table_id,
        "first_visible_delta_version": diff.version,
        "source_schema_fingerprint": diff.current.source_schema_fingerprint,
        "source_feature_row_fingerprint": fingerprint,
        "availability_state": "DELTA_COMMITTED",
    }


def _segment(diff: VersionDifference) -> tuple[dict[str, object], list[dict[str, object]]]:
    ready = [_ready_event(diff, event_id) for event_id in sorted(diff.new_rows)]
    basis: dict[str, object] = {
        "segment_contract_version": SEGMENT_VERSION,
        "availability_contract_version": READY_VERSION,
        "discovery_algorithm_version": DISCOVERY_VERSION,
        "source_delta_path": str(diff.current.delta_path),
        "source_delta_table_id": diff.current.delta_table_id,
        "source_delta_version": diff.version,
        "predecessor_delta_version": diff.version - 1 if diff.previous is not None else None,
        "source_schema_fingerprint": diff.current.source_schema_fingerprint,
        "source_protocol": diff.current.protocol,
        "previous_event_count": len(diff.previous_rows),
        "current_event_count": len(diff.current_rows),
        "new_event_count": len(ready),
        "unchanged_event_count": len(diff.previous_rows),
        "previous_snapshot_fingerprint": digest(sorted(diff.previous_rows.items())),
        "current_snapshot_fingerprint": digest(sorted(diff.current_rows.items())),
        "new_event_ids_fingerprint": digest(sorted(diff.new_rows)),
        "new_event_rows_fingerprint": digest(sorted(diff.new_rows.items())),
        "ready_events_fingerprint": digest(ready),
    }
    segment_id = digest(basis)
    return {
        **basis,
        "segment_id": segment_id,
        "ready_events_sha256": digest_bytes(lines(ready)),
    }, ready


def digest_bytes(raw: bytes) -> str:
    import hashlib

    return hashlib.sha256(raw).hexdigest()


def _segment_path(root: Path, segment: dict[str, object]) -> Path:
    return root.resolve() / cast(str, segment["segment_id"])


def build_segments(source: Path, start: int, end: int, root: Path) -> list[Path]:
    paths: list[Path] = []
    for diff in discover_versions(source, start, end):
        manifest, ready = _segment(diff)
        target = _segment_path(root, manifest)
        if target.exists():
            existing, existing_ready = inspect_segment(target)
            if existing != manifest or existing_ready != ready:
                raise AvailabilityError(f"Existing commit segment conflicts: {target}")
        else:
            publish(
                target,
                {
                    "ready_events.jsonl": lines(ready),
                    "segment_manifest.json": canonical(manifest),
                },
            )
            inspect_segment(target)
        paths.append(target)
    return paths


def canonical(value: object) -> bytes:
    from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

    return canonical_json(value)


def inspect_segment(path: Path) -> tuple[dict[str, object], list[dict[str, object]]]:
    exact_directory(path, SEGMENT_FILES)
    manifest = read_object(path / "segment_manifest.json")
    ready = read_lines(path / "ready_events.jsonl")
    try:
        version = cast(int, manifest["source_delta_version"])
        if type(version) is not int or version < 0:
            raise AvailabilityError("Invalid segment Delta version")
        diff = discover_versions(Path(cast(str, manifest["source_delta_path"])), version, version)[
            0
        ]
        expected, expected_ready = _segment(diff)
        if (
            path.name != expected["segment_id"]
            or manifest != expected
            or ready != expected_ready
            or file_hash(path / "ready_events.jsonl") != manifest["ready_events_sha256"]
        ):
            raise AvailabilityError(f"Commit segment differs from retained Delta version: {path}")
        return manifest, ready
    except (KeyError, TypeError) as error:
        raise AvailabilityError(f"Invalid commit segment manifest: {error}") from error


def _outbox(
    segments: list[tuple[Path, dict[str, object], list[dict[str, object]]]],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    if not segments:
        raise AvailabilityError("Outbox requires at least one commit segment")
    paths, manifests, lists = zip(*segments, strict=True)
    versions = [cast(int, manifest["source_delta_version"]) for manifest in manifests]
    if len(versions) > MAX_VERSIONS or versions != list(range(versions[0], versions[-1] + 1)):
        raise AvailabilityError("Outbox segments must cover a contiguous ordered version range")
    table_ids = {manifest["source_delta_table_id"] for manifest in manifests}
    sources = {manifest["source_delta_path"] for manifest in manifests}
    schemas = {manifest["source_schema_fingerprint"] for manifest in manifests}
    if len(table_ids) != 1 or len(sources) != 1 or len(schemas) != 1:
        raise AvailabilityError("Outbox segment source identities differ")
    ready = [event for batch in lists for event in batch]
    if len(ready) > MAX_READY_EVENTS:
        raise AvailabilityError("Outbox exceeds local ready-event limit")
    ids = [event["event_id"] for event in ready]
    ready_ids = [event["ready_event_id"] for event in ready]
    if len(set(ids)) != len(ids) or len(set(ready_ids)) != len(ready_ids):
        raise AvailabilityError("Outbox contains repeated source or ready-event identities")
    basis: dict[str, object] = {
        "outbox_contract_version": OUTBOX_VERSION,
        "segment_contract_version": SEGMENT_VERSION,
        "availability_contract_version": READY_VERSION,
        "source_delta_path": manifests[0]["source_delta_path"],
        "source_delta_table_id": manifests[0]["source_delta_table_id"],
        "source_schema_fingerprint": manifests[0]["source_schema_fingerprint"],
        "start_delta_version": versions[0],
        "end_delta_version": versions[-1],
        "segment_ids": [manifest["segment_id"] for manifest in manifests],
        "ready_event_count": len(ready),
        "unique_event_count": len(ids),
        "source_event_ids_fingerprint": digest(sorted(ids)),
        "logical_outbox_fingerprint": digest(ready),
    }
    outbox_id = digest(basis)
    return {
        **basis,
        "outbox_id": outbox_id,
        "segment_paths": [str(path.resolve()) for path in paths],
        "ready_events_sha256": digest_bytes(lines(ready)),
    }, ready


def build_outbox(segment_paths: list[Path], output_root: Path) -> Path:
    segments = [(path.resolve(), *inspect_segment(path)) for path in segment_paths]
    manifest, ready = _outbox(segments)
    target = output_root.resolve() / cast(str, manifest["outbox_id"])
    if target.exists():
        existing, existing_ready = inspect_outbox(target)
        if existing != manifest or existing_ready != ready:
            raise AvailabilityError(f"Existing outbox conflicts: {target}")
        return target
    publish(
        target,
        {"ready_events.jsonl": lines(ready), "outbox_manifest.json": canonical(manifest)},
    )
    inspect_outbox(target)
    return target


def inspect_outbox(path: Path) -> tuple[dict[str, object], list[dict[str, object]]]:
    exact_directory(path, OUTBOX_FILES)
    manifest = read_object(path / "outbox_manifest.json")
    ready = read_lines(path / "ready_events.jsonl")
    try:
        paths = [Path(value) for value in cast(list[str], manifest["segment_paths"])]
        segments = [(item.resolve(), *inspect_segment(item)) for item in paths]
        expected, expected_ready = _outbox(segments)
        if (
            path.name != expected["outbox_id"]
            or manifest != expected
            or ready != expected_ready
            or file_hash(path / "ready_events.jsonl") != manifest["ready_events_sha256"]
        ):
            raise AvailabilityError("Outbox differs from verified commit segments")
        differences = discover_versions(
            Path(cast(str, manifest["source_delta_path"])),
            cast(int, manifest["start_delta_version"]),
            cast(int, manifest["end_delta_version"]),
        )
        source_ids = {event_id for diff in differences for event_id in diff.new_rows}
        if source_ids != {cast(str, event["event_id"]) for event in ready}:
            raise AvailabilityError("Source-to-outbox event-ID reconciliation failed")
        return manifest, ready
    except (KeyError, TypeError) as error:
        raise AvailabilityError(f"Invalid outbox manifest: {error}") from error
