"""Immutable, exact-path recovery bundle. File restoration is not resumed-work proof."""

from __future__ import annotations

import shutil
import tarfile
import time
from pathlib import Path
from typing import cast

from .archive import extract_archive, inspect_archive
from .contract import (
    BUNDLE_VERSION,
    CUTOFF_VERSION,
    LOGIC_VERSION,
    RecoveryError,
    backup_sqlite,
    digest,
    inventory,
    read,
    sha,
    sqlite_identity,
    unlinked,
    validate_graph,
    write,
)


def _basis(cutoff: dict[str, object], entries: list[dict[str, object]]) -> dict[str, object]:
    return {
        "contract_version": BUNDLE_VERSION,
        "logic_version": LOGIC_VERSION,
        "source_lineage_id": cutoff["lineage_id"],
        "source_git_revision": cutoff["git_revision"],
        "cutoff_identity": cutoff["cutoff_id"],
        "inventory_fingerprint": digest(entries),
    }


def build(cutoff_path: Path, kafka_archive: Path, destination: Path) -> dict[str, object]:
    """Source remains stopped; source and cutoff bytes rechecked before publication."""
    started = time.perf_counter()
    cutoff = read(cutoff_path)
    if cutoff.get("contract_version") != CUTOFF_VERSION:
        raise RecoveryError("Unsupported recovery cutoff")
    basis = {k: v for k, v in cutoff.items() if k not in {"cutoff_id", "captured_at", "timings"}}
    if digest(basis) != cutoff.get("cutoff_id"):
        raise RecoveryError("Cutoff identity differs")
    if cutoff.get("quiescence") != "OBSERVED_STOPPED_WITH_STABLE_INVENTORY":
        raise RecoveryError("Writer quiescence is not established")
    from .environment import assert_source_stopped

    assert_source_stopped(str(cutoff["source_compose_project"]), str(cutoff["source_volume"]))
    nodes = cast(list[dict[str, object]], cutoff["graph"])
    validate_graph(nodes)
    paths = [Path(str(node["path"])) for node in nodes]
    before = inventory(paths)
    if before != cutoff["inventory"]:
        raise RecoveryError("Source changed since cutoff; do not label bundle consistent")
    if sha(kafka_archive) != cutoff["kafka_archive_sha256"]:
        raise RecoveryError("Kafka archive differs from quiesced cutoff")
    archive_entries = inspect_archive(kafka_archive)
    if archive_entries != cutoff["kafka_inventory"]:
        raise RecoveryError("Kafka storage inventory changed")
    unlinked(destination)
    if destination.exists() or any(destination.is_relative_to(path) for path in paths):
        raise RecoveryError("Bundle destination exists or overlaps a source dependency")
    destination.mkdir(parents=True)
    payload = destination / "payload"
    payload.mkdir()
    database = Path(str(cutoff["sqlite_path"]))
    sqlite_copy = payload / "tracking.db"
    database_evidence = backup_sqlite(database, sqlite_copy)
    # Backup API may change SQLite physical layout; its logical identity is checked separately.
    entries = [dict(item) for item in before]
    for item in entries:
        if item["path"] == str(database):
            item["sha256"] = sha(sqlite_copy)
            item["size"] = sqlite_copy.stat().st_size
    with tarfile.open(payload / "files.tar", "x", format=tarfile.PAX_FORMAT) as archive:
        for item in entries:
            original = Path(str(item["path"]))
            source = sqlite_copy if original == database else original
            info = archive.gettarinfo(str(source), arcname=str(original).lstrip("/"))
            info.mode = cast(int, item["mode"])
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0  # Capture time is operational evidence, never corpus identity.
            with source.open("rb") as stream:
                archive.addfile(info, stream)
    shutil.copyfile(kafka_archive, payload / "kafka-volume.tar")
    inspect_archive(payload / "files.tar")
    if before != inventory(paths):
        raise RecoveryError("Source writes detected during capture; incomplete bundle retained")
    assert_source_stopped(str(cutoff["source_compose_project"]), str(cutoff["source_volume"]))
    bundle_basis = _basis(cutoff, entries)
    write(destination / "source_cutoff.json", cutoff)
    write(destination / "source_inventory.json", entries)
    write(destination / "dependency_graph.json", {"nodes": nodes})
    write(destination / "runtime_contract.json", cutoff["runtime"])
    write(
        destination / "restore_plan.json",
        {
            "logical_paths": [item["path"] for item in entries],
            "source_repository": cutoff["selection"],
            "source_only_sentinel": cutoff["source_only_sentinel"],
            "storage_independence": "SIMULATED_SAME_HOST_DISK",
            "isolation_required": "private network; restored-only mounts; no host ports/socket",
            "sqlite_backup": database_evidence,
        },
    )
    names = [str(p.relative_to(destination)) for p in sorted(destination.rglob("*")) if p.is_file()]
    manifest = {
        **bundle_basis,
        "bundle_id": digest(bundle_basis),
        "artifact_sha256": {name: sha(destination / name) for name in names},
        "build_seconds": time.perf_counter() - started,
        "storage_independence": "SIMULATED_SAME_HOST_DISK",
    }
    write(destination / "bundle_manifest.json", manifest)
    return verify(destination)


def verify(bundle: Path) -> dict[str, object]:
    manifest = read(bundle / "bundle_manifest.json")
    if (
        manifest.get("contract_version") != BUNDLE_VERSION
        or manifest.get("logic_version") != LOGIC_VERSION
    ):
        raise RecoveryError("Unsupported recovery bundle")
    artifacts = cast(dict[str, str], manifest["artifact_sha256"])
    required = {
        "source_cutoff.json",
        "source_inventory.json",
        "dependency_graph.json",
        "runtime_contract.json",
        "restore_plan.json",
        "payload/files.tar",
        "payload/kafka-volume.tar",
        "payload/tracking.db",
    }
    actual = {str(p.relative_to(bundle)) for p in bundle.rglob("*") if p.is_file()}
    if set(artifacts) != required or actual != required | {"bundle_manifest.json"}:
        raise RecoveryError("Incomplete/unexpected bundle inventory")
    for name, expected in artifacts.items():
        if sha(bundle / name) != expected:
            raise RecoveryError(f"Corrupt bundle component: {name}")
    cutoff = read(bundle / "source_cutoff.json")
    cutoff_basis = {
        k: v for k, v in cutoff.items() if k not in {"cutoff_id", "captured_at", "timings"}
    }
    if digest(cutoff_basis) != cutoff.get("cutoff_id"):
        raise RecoveryError("Cutoff content identity differs")
    import json

    entries = cast(
        list[dict[str, object]], json.loads((bundle / "source_inventory.json").read_bytes())
    )
    if len({item["path"] for item in entries}) != len(entries):
        raise RecoveryError("Duplicate logical source file")
    source_entries = {
        str(item["path"]): item for item in cast(list[dict[str, object]], cutoff["inventory"])
    }
    if {str(item["path"]) for item in entries} != set(source_entries):
        raise RecoveryError("Required dependency omitted from backup")
    for item in entries:
        unlinked(Path(str(item["path"])))
        expected_item = dict(source_entries[str(item["path"])])
        if item["path"] == cutoff["sqlite_path"]:
            expected_item["sha256"] = sha(bundle / "payload/tracking.db")
            expected_item["size"] = (bundle / "payload/tracking.db").stat().st_size
        if item != expected_item:
            raise RecoveryError("File inventory differs from cutoff")
    plan = read(bundle / "restore_plan.json")
    if (
        plan["logical_paths"] != [item["path"] for item in entries]
        or read(bundle / "runtime_contract.json") != cutoff["runtime"]
        or read(bundle / "dependency_graph.json")["nodes"] != cutoff["graph"]
    ):
        raise RecoveryError("Recovery plan/dependencies differ from cutoff")
    sqlite_proof = cast(dict[str, object], plan["sqlite_backup"])
    if (
        sqlite_identity(bundle / "payload/tracking.db") != sqlite_proof["backup"]
        or cast(dict[str, object], sqlite_proof["backup"])["logical_sha256"]
        != cast(dict[str, object], sqlite_proof["source"])["logical_sha256"]
    ):
        raise RecoveryError("SQLite logical backup identity differs")
    basis = _basis(cutoff, entries)
    if (
        any(manifest.get(k) != v for k, v in basis.items())
        or digest(basis) != manifest["bundle_id"]
    ):
        raise RecoveryError("Bundle semantic identity differs")
    validate_graph(cast(list[dict[str, object]], read(bundle / "dependency_graph.json")["nodes"]))
    archived = inspect_archive(bundle / "payload/files.tar")
    expected_files = {str(item["path"]).lstrip("/"): item["sha256"] for item in entries}
    if {
        str(item["path"]): item["sha256"] for item in archived if not item["directory"]
    } != expected_files:
        raise RecoveryError("Archive does not contain exact dependency inventory")
    if inspect_archive(bundle / "payload/kafka-volume.tar") != cutoff["kafka_inventory"]:
        raise RecoveryError("Kafka archive inventory differs")
    return manifest


def copy_bundle(source: Path, destination: Path) -> dict[str, object]:
    expected = verify(source)
    unlinked(destination)
    if destination.exists() or destination.is_relative_to(source):
        raise RecoveryError("Independent-copy destination must be fresh and outside bundle")
    started = time.perf_counter()
    shutil.copytree(source, destination, symlinks=False)
    if verify(destination) != expected:
        raise RecoveryError("Destination backup copy differs")
    return {
        "bundle_id": expected["bundle_id"],
        "copy_seconds": time.perf_counter() - started,
        "same_filesystem": source.stat().st_dev == destination.stat().st_dev,
        "storage_independence": "SIMULATED_SAME_HOST_DISK",
    }


def restore_files(bundle: Path, target: Path) -> dict[str, object]:
    """Stage exact absolute hierarchy under a fresh root; no original path is opened."""
    manifest = verify(bundle)
    started = time.perf_counter()
    extract_archive(bundle / "payload/files.tar", target)
    return {
        "bundle_id": manifest["bundle_id"],
        "target": str(target),
        "status": "PARTIALLY_VERIFIED",
        "restore_seconds": time.perf_counter() - started,
        "note": "Files verified; broker identity, isolated inspection and resume still required",
    }
