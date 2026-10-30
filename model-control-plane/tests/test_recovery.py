"""Docker-independent recovery refusal, integrity, closure and consistent-copy tests."""

import io
import json
import sqlite3
import tarfile
from pathlib import Path
from typing import cast

import pytest

from sentinelaegisforge_control_plane.recovery import bundle, environment, runtime
from sentinelaegisforge_control_plane.recovery.archive import extract_archive, inspect_archive
from sentinelaegisforge_control_plane.recovery.contract import (
    CUTOFF_VERSION,
    RecoveryError,
    backup_sqlite,
    digest,
    inventory,
    read,
    sha,
    sqlite_identity,
    validate_graph,
    write,
)
from sentinelaegisforge_control_plane.recovery.docker_restore import check_mounts, storage_identity
from sentinelaegisforge_control_plane.recovery.lineage import compare_identity, delta_history


def tar(path: Path, members: list[tuple[str, bytes, bytes]]) -> None:
    with tarfile.open(path, "w") as archive:
        for name, data, kind in members:
            info = tarfile.TarInfo(name)
            info.type = kind
            info.size = len(data) if kind == tarfile.REGTYPE else 0
            info.linkname = "../../escape" if kind in {tarfile.SYMTYPE, tarfile.LNKTYPE} else ""
            archive.addfile(info, io.BytesIO(data))


@pytest.mark.parametrize(
    "name,kind",
    [
        ("../escape", tarfile.REGTYPE),
        ("/absolute", tarfile.REGTYPE),
        ("dir/../../escape", tarfile.REGTYPE),
        ("x\\y", tarfile.REGTYPE),
        ("link", tarfile.SYMTYPE),
        ("hard", tarfile.LNKTYPE),
        ("device", tarfile.CHRTYPE),
        ("fifo", tarfile.FIFOTYPE),
    ],
)
def test_unsafe_archive_refused_before_any_extraction(
    tmp_path: Path, name: str, kind: bytes
) -> None:
    source = tmp_path / "bad.tar"
    tar(source, [(name, b"data", kind)])
    with pytest.raises(RecoveryError):
        extract_archive(source, tmp_path / "out")
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("names", [("x", "x"), ("x", "x/child")])
def test_archive_duplicates_and_file_directory_conflicts(
    tmp_path: Path, names: tuple[str, str]
) -> None:
    source = tmp_path / "bad.tar"
    tar(source, [(name, b"x", tarfile.REGTYPE) for name in names])
    with pytest.raises(RecoveryError):
        inspect_archive(source)


def test_archive_limits_and_nonempty_target(tmp_path: Path) -> None:
    source = tmp_path / "safe.tar"
    tar(source, [("sub/file", b"1234", tarfile.REGTYPE)])
    with pytest.raises(RecoveryError, match="limit"):
        inspect_archive(source, max_bytes=3)
    target = tmp_path / "target"
    target.mkdir()
    (target / "keep").write_text("preserve")
    with pytest.raises(RecoveryError, match="exists"):
        extract_archive(source, target)
    assert (target / "keep").read_text() == "preserve"
    extract_archive(source, tmp_path / "fresh")
    assert (tmp_path / "fresh/sub/file").read_bytes() == b"1234"


def test_symlink_restore_root_refused(tmp_path: Path) -> None:
    source = tmp_path / "safe.tar"
    tar(source, [("f", b"x", tarfile.REGTYPE)])
    (tmp_path / "alias").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(RecoveryError, match="Symlink"):
        extract_archive(source, tmp_path / "alias/new")


def test_sqlite_backup_copies_committed_wal_and_preserves_logical_identity(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    with sqlite3.connect(source) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value TEXT)")
        db.execute("INSERT INTO evidence VALUES (1,'committed')")
        db.commit()
        target = tmp_path / "copy.db"
        result = backup_sqlite(source, target)
        assert (
            cast(dict[str, object], result["source"])["logical_sha256"]
            == sqlite_identity(target)["logical_sha256"]
        )
        with pytest.raises(FileExistsError):
            backup_sqlite(source, target)


def test_corrupt_sqlite_refused(tmp_path: Path) -> None:
    path = tmp_path / "bad.db"
    path.write_bytes(b"not a database")
    with pytest.raises(RecoveryError, match="SQLite"):
        sqlite_identity(path)


def test_dependency_closure_refuses_missing_node_and_cycles(tmp_path: Path) -> None:
    node: dict[str, object] = {
        "id": "a",
        "path": str(tmp_path / "a"),
        "role": "model",
        "reason": "provenance",
        "requires": ["missing"],
    }
    with pytest.raises(RecoveryError, match="omitted"):
        validate_graph([node])
    node["requires"] = ["a"]
    with pytest.raises(RecoveryError, match="cycle"):
        validate_graph([node])


COMPONENTS = [
    "delta/_delta_log/00000000000000000000.json",
    "delta/part.parquet",
    "checkpoint/offsets/0",
    "checkpoint/state/rocksdb/1.zip",
    "model/model.skops",
    "research/journals/published.json",
    "research/receipts/accepted.json",
    "research/results/complete.json",
]


@pytest.fixture
def recovery_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    for name in COMPONENTS:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
    db_path = source / "tracking.db"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE model(name TEXT)")
        db.execute("INSERT INTO model VALUES ('research only')")
    kafka = tmp_path / "kafka.tar"
    tar(
        kafka,
        [
            ("meta.properties", b"cluster.id=original", tarfile.REGTYPE),
            ("_schemas-0/log", b"original-schema-id", tarfile.REGTYPE),
        ],
    )
    cutoff: dict[str, object] = {
        "contract_version": CUTOFF_VERSION,
        "lineage_id": "lineage",
        "git_revision": "revision",
        "graph": [
            {
                "id": "source",
                "path": str(source),
                "role": "lineage",
                "reason": "test closure",
                "requires": [],
            }
        ],
        "inventory": inventory([source]),
        "kafka_archive_sha256": sha(kafka),
        "kafka_inventory": inspect_archive(kafka),
        "sqlite_path": str(db_path),
        "runtime": {"spark": "4.2.0"},
        "selection": {"repository": str(tmp_path)},
        "source_only_sentinel": str(tmp_path / "original-only"),
        "source_compose_project": "source",
        "source_volume": "source-volume",
        "quiescence": "OBSERVED_STOPPED_WITH_STABLE_INVENTORY",
    }
    cutoff["cutoff_id"] = digest(cutoff)
    cutoff["captured_at"] = "operational-time-only"
    write(tmp_path / "cutoff.json", cutoff)
    monkeypatch.setattr(environment, "assert_source_stopped", lambda *_: None)
    bundle.build(tmp_path / "cutoff.json", kafka, tmp_path / "bundle")
    return tmp_path / "bundle"


def test_bundle_copy_restore_and_timestamp_independent_identity(
    recovery_bundle: Path, tmp_path: Path
) -> None:
    original = bundle.verify(recovery_bundle)
    result = bundle.copy_bundle(recovery_bundle, tmp_path / "backup")
    assert result["bundle_id"] == original["bundle_id"]
    assert result["storage_independence"] == "SIMULATED_SAME_HOST_DISK"
    restored = bundle.restore_files(tmp_path / "backup", tmp_path / "restore")
    assert restored["status"] == "PARTIALLY_VERIFIED"  # File copy alone is never complete.
    cutoff = read(recovery_bundle / "source_cutoff.json")
    entries = json.loads((recovery_bundle / "source_inventory.json").read_bytes())
    before = digest(bundle._basis(cutoff, entries))
    cutoff["captured_at"] = "different operational time"
    assert digest(bundle._basis(cutoff, entries)) == before


@pytest.mark.parametrize("name", COMPONENTS)
def test_missing_or_corrupt_restored_authority_blocks_preflight(
    recovery_bundle: Path, name: str
) -> None:
    path = recovery_bundle.parent / "source" / name
    path.write_bytes(b"corrupted")
    with pytest.raises(RecoveryError, match="restored dependency"):
        runtime.inspect_restored(recovery_bundle)


@pytest.mark.parametrize(
    "name",
    [
        "bundle_manifest.json",
        "payload/files.tar",
        "payload/kafka-volume.tar",
        "payload/tracking.db",
    ],
)
def test_corrupt_bundle_blocks_restore(recovery_bundle: Path, name: str, tmp_path: Path) -> None:
    path = recovery_bundle / name
    path.write_bytes(path.read_bytes() + b"corrupt")
    with pytest.raises(RecoveryError):
        bundle.restore_files(recovery_bundle, tmp_path / "restore")
    assert not (tmp_path / "restore").exists()


def test_missing_kafka_archive_refused(recovery_bundle: Path) -> None:
    (recovery_bundle / "payload/kafka-volume.tar").unlink()
    with pytest.raises(RecoveryError, match="inventory"):
        bundle.verify(recovery_bundle)


def test_original_namespace_sentinel_refused(recovery_bundle: Path) -> None:
    (recovery_bundle.parent / "original-only").write_text("source")
    with pytest.raises(RecoveryError, match="Original source"):
        runtime.inspect_restored(recovery_bundle)


@pytest.mark.parametrize(
    "component",
    ["Kafka cluster", "topic ID", "Schema Registry ID", "state configuration", "model-tree hash"],
)
def test_identity_mismatch_refuses_equivalence(component: str) -> None:
    with pytest.raises(RecoveryError, match="differs"):
        compare_identity({"identity": "original"}, {"identity": "replacement"}, component)


def test_no_original_mount_fallback() -> None:
    actual: list[dict[str, object]] = [
        {"Destination": "/logical/.local", "Source": "/original/.local", "Type": "bind", "RW": True}
    ]
    with pytest.raises(RecoveryError, match="fallback"):
        check_mounts(actual, {"/logical/.local": "/restore/.local"})


def test_directory_time_is_operational_but_file_time_and_owner_are_preserved() -> None:
    directory: dict[str, object] = {"path": "logs", "directory": True, "mtime": 1, "uid": 1000}
    assert storage_identity([directory]) == storage_identity([{**directory, "mtime": 2}])
    assert storage_identity([directory]) != storage_identity([{**directory, "uid": 0}])
    file = {**directory, "directory": False}
    assert storage_identity([file]) != storage_identity([{**file, "mtime": 2}])


def test_missing_retained_delta_data_is_not_reconstructed(tmp_path: Path) -> None:
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    path = tmp_path / "delta"
    write_deltalake(path, pa.table({"event_id": ["a"]}))
    assert len(delta_history(path)) == 1
    Path(DeltaTable(path).file_uris()[0]).unlink()
    with pytest.raises(Exception):
        delta_history(path)


def test_broker_preflight_allows_only_verified_internal_noop_suffix() -> None:
    from copy import deepcopy

    from sentinelaegisforge_control_plane.recovery.docker_restore import check_broker_identity

    expected: dict[str, object] = {
        "cluster_id": "original",
        "schemas": {"transactions.raw-value": {"id": 1}},
        "topics": {
            "_schemas": {"topic_id": "schemas", "end": {"0": 50}},
            "transactions.raw": {"topic_id": "raw", "end": {"0": 30}},
        },
    }
    observed = deepcopy(expected)
    cast(dict[str, dict[str, object]], observed["topics"])["_schemas"]["end"] = {"0": 51}
    noops: list[dict[str, object]] = [{"key": {"keytype": "NOOP", "magic": 0}, "value": None}]
    check_broker_identity(expected, observed, noops)
    with pytest.raises(RecoveryError, match="Unexplained"):
        check_broker_identity(expected, observed, [])
    with pytest.raises(RecoveryError, match="Unexplained"):
        check_broker_identity(expected, observed, [{"key": {"keytype": "SCHEMA"}, "value": None}])
    for field in ("cluster_id", "schemas", "topics"):
        broken = {**observed, field: {} if field != "cluster_id" else "new"}
        with pytest.raises((RecoveryError, KeyError)):
            check_broker_identity(expected, broken, noops)


def test_missing_source_volume_and_running_writer_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(environment, "command", lambda *_: "")
    monkeypatch.setattr(environment, "docker_object", lambda *_: [])
    with pytest.raises(RecoveryError, match="Missing source Kafka volume"):
        environment.assert_source_stopped("source", "volume")
    monkeypatch.setattr(environment, "command", lambda *_: "running-container")
    with pytest.raises(RecoveryError, match="running"):
        environment.assert_source_stopped("source", "volume")


def test_rehearsal_inspection_is_repeatable_and_detects_corrupt_evidence(tmp_path: Path) -> None:
    from sentinelaegisforge_control_plane.recovery.cli import inspect_rehearsal

    write(tmp_path / "proof.json", {"verified": "fixture"})
    result: dict[str, object] = {
        "contract_version": "synthetic-recovery-rehearsal-v1",
        "artifact_sha256": {"proof.json": sha(tmp_path / "proof.json")},
    }
    result["rehearsal_id"] = digest(result)
    write(tmp_path / "rehearsal_manifest.json", result)
    before = inventory([tmp_path])
    assert inspect_rehearsal(tmp_path) == inspect_rehearsal(tmp_path) == result
    assert inventory([tmp_path]) == before
    (tmp_path / "proof.json").write_bytes(b"changed")
    with pytest.raises(RecoveryError, match="evidence differs"):
        inspect_rehearsal(tmp_path)


def test_source_mutation_after_cutoff_refuses_bundle(recovery_bundle: Path, tmp_path: Path) -> None:
    (tmp_path / "source/model/model.skops").write_bytes(b"new-model")
    with pytest.raises(RecoveryError, match="Source changed"):
        bundle.build(tmp_path / "cutoff.json", tmp_path / "kafka.tar", tmp_path / "new")
    assert not (tmp_path / "new").exists()


def test_missing_delta_log_refuses_history(tmp_path: Path) -> None:
    import pyarrow as pa
    from deltalake import write_deltalake

    path = tmp_path / "delta"
    write_deltalake(path, pa.table({"event_id": ["a"]}))
    (path / "_delta_log/00000000000000000000.json").unlink()
    with pytest.raises(Exception):
        delta_history(path)
