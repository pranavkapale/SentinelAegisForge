"""Read-only source-retention gates for resuming an isolated integration."""

import hashlib
import json
from pathlib import Path
from typing import cast

import pyarrow.fs as pa_fs
from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import read_object
from sentinelaegisforge_control_plane.incremental_shadow.operations import inspect as inspect_stream
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import (
    STAGES,
    inspect_identity,
)

from .contract import ReliabilityError, inspect_state_compatibility


def feature_ready_range(stream: Path, offset_log: Path) -> dict[str, object]:
    """Check original required research offsets against a captured read-only broker probe."""
    manifest = inspect_stream(stream)
    required = read_object(Path(cast(str, manifest["fence_path"])))
    raw = offset_log.read_bytes()
    marker = "Feature-ready retention JSON: "
    lines = [line.split(marker, 1)[1] for line in raw.decode().splitlines() if marker in line]
    if len(lines) != 1:
        raise ReliabilityError("Missing unambiguous feature-ready retention probe")
    try:
        current = json.loads(lines[0])
    except json.JSONDecodeError as error:
        raise ReliabilityError("Malformed feature-ready retention probe") from error
    if not isinstance(current, dict):
        raise ReliabilityError("Invalid feature-ready retention probe")
    result = kafka_range({**required, "partition_count": 3}, current)
    return {
        **result,
        "probe_log_sha256": hashlib.sha256(raw).hexdigest(),
        "group_id": manifest["group_id"],
    }


def kafka_range(required: dict[str, object], current: dict[str, object]) -> dict[str, object]:
    """Reject a lost source range; physical Kafka offsets cannot be guessed."""
    for field in ("topic", "topic_id", "cluster_id", "partition_count"):
        if required.get(field) != current.get(field):
            raise ReliabilityError(f"Kafka topic identity changed: {field}")
    if required.get("partition_count") != 3:
        raise ReliabilityError("Expected three local transaction partitions")
    ranges: dict[str, dict[str, int]] = {}
    for part in range(3):
        requested = required.get(f"end_{part}")
        beginning = current.get(f"beginning_{part}")
        end = current.get(f"end_{part}")
        if not all(type(value) is int and value >= 0 for value in (requested, beginning, end)):
            raise ReliabilityError("Malformed Kafka offset boundary")
        assert isinstance(requested, int)
        assert isinstance(beginning, int)
        assert isinstance(end, int)
        if beginning > requested:
            raise ReliabilityError(
                f"Kafka retention gap in partition {part}: "
                f"required {requested}, beginning {beginning}"
            )
        if end < requested:
            raise ReliabilityError(f"Kafka end offset retreated in partition {part}")
        ranges[str(part)] = {"required": requested, "beginning": beginning, "end": end}
    return {"contract_version": "phase29-kafka-retention-v1", "ranges": ranges}


def delta_version(path: Path, version: int) -> dict[str, object]:
    if version < 0:
        raise ReliabilityError("Delta version cannot be negative")
    try:
        table = DeltaTable(path, version=version)
        filesystem = pa_fs.SubTreeFileSystem(str(path.resolve()), pa_fs.LocalFileSystem())
        table.to_pyarrow_table(columns=["event_id"], filesystem=filesystem)
        return {
            "path": str(path.resolve()),
            "version": version,
            "table_id": str(table.metadata().id),
        }
    except Exception as error:
        raise ReliabilityError(
            f"Required Delta history version {version} is unavailable: {error}"
        ) from error


def inspect(integration: Path, current_fence: Path) -> dict[str, object]:
    identity = inspect_identity(integration)
    inspect_state_compatibility(integration)
    recorded = read_object(Path(cast(str, identity["transaction_fence_path"])))
    broker = read_object(current_fence)
    ranges = kafka_range(recorded, broker)
    paths = cast(dict[str, str], identity["paths"])
    checkpoints = cast(dict[str, str], identity["checkpoints"])
    versions: dict[str, list[dict[str, object]]] = {stage: [] for stage in STAGES}
    for wave in range(3):
        saved = read_object(integration / "waves" / f"{wave}.json")
        for stage in STAGES:
            stage_record = cast(dict[str, object], cast(dict[str, object], saved["stages"])[stage])
            observed = delta_version(Path(paths[stage]), cast(int, stage_record["version"]))
            if observed["table_id"] != stage_record["table_id"]:
                raise ReliabilityError(f"Delta history table identity changed for {stage}")
            versions[stage].append(observed)
    for stage in STAGES:
        checkpoint = Path(checkpoints[stage])
        if not checkpoint.is_dir() or checkpoint.is_symlink():
            raise ReliabilityError(f"Missing Spark checkpoint: {stage}")
    statistical = Path(checkpoints["statistical"])
    if not (statistical / "state").is_dir() or not any(
        item.suffix in {".sst", ".zip"} for item in (statistical / "state").rglob("*")
    ):
        raise ReliabilityError("Statistical checkpoint lacks RocksDB state files")
    return {
        "contract_version": "phase29-source-retention-readiness-v1",
        "integration_id": identity["integration_id"],
        "transaction_kafka": ranges,
        "delta_versions": versions,
        "checkpoints_present": sorted(STAGES),
        "statistical_rocksdb_present": True,
    }
