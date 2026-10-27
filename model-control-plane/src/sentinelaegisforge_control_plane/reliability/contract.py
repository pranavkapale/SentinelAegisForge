"""Versioned experiment and stateful-checkpoint compatibility contracts."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import digest, read_object
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import (
    STAGES,
    inspect_identity,
)

CONTRACT = "stateful-streaming-reliability-v1"
STATE_CONTRACT = "statistical-state-compatibility-v1"
STATE_SEMANTICS = "customer-currency-welford-v2"
STATE_PROVIDER = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
SPARK_VERSION = "4.2.0"
SCALA_VERSION = "2.13.18"
WATERMARK = "10 minutes"
INACTIVITY = "24 hours"


class ReliabilityError(ValueError):
    """An experiment, checkpoint or measured evidence violates the contract."""


def matrix(integrations: dict[str, Path]) -> dict[str, object]:
    """Validate independently fenced Phase 28 lineages for A, B, C and D."""
    if set(integrations) != {"A", "B", "C", "D"}:
        raise ReliabilityError("Experiments A, B, C and D are required")
    identities = {name: inspect_identity(path) for name, path in integrations.items()}
    ids = [identity["integration_id"] for identity in identities.values()]
    if len(set(ids)) != 4:
        raise ReliabilityError("Experiment integration IDs overlap")
    paths: list[str] = []
    for identity in identities.values():
        for key in ("paths", "checkpoints", "transaction_app_ids"):
            paths.extend(cast(dict[str, str], identity[key]).values())
        paths.append(cast(str, identity["group_id"]))
    if len(paths) != len(set(paths)):
        raise ReliabilityError(
            "Experiment Delta, checkpoint, transaction or group lineages overlap"
        )
    if identities["A"]["plan_sha256"] == identities["B"]["plan_sha256"]:
        raise ReliabilityError("Changed workload has the same transaction plan as baseline")
    return {
        "contract_version": CONTRACT,
        "experiments": {name: identity["integration_id"] for name, identity in identities.items()},
        "source_corpora": {name: identity["corpus_id"] for name, identity in identities.items()},
    }


def _table_id(path: Path) -> str:
    try:
        return str(DeltaTable(path).metadata().id)
    except Exception as error:
        raise ReliabilityError(f"Required Delta table is unavailable at {path}: {error}") from error


def _semantic_record(identity: dict[str, object]) -> dict[str, object]:
    paths = cast(dict[str, str], identity["paths"])
    checkpoints = cast(dict[str, str], identity["checkpoints"])
    apps = cast(dict[str, str], identity["transaction_app_ids"])
    source = Path(paths["rolling"])
    return {
        "contract_version": STATE_CONTRACT,
        "spark_version": SPARK_VERSION,
        "scala_version": SCALA_VERSION,
        "scala_application": "TransactionStatisticalFeatureApp",
        "state_semantics": STATE_SEMANTICS,
        "state_provider": STATE_PROVIDER,
        "watermark_delay": WATERMARK,
        "inactivity_timeout": INACTIVITY,
        "source_path": str(source.resolve()),
        "source_table_id": _table_id(source),
        "target_path": str(Path(paths["statistical"]).resolve()),
        "checkpoint_path": str(Path(checkpoints["statistical"]).resolve()),
        "txn_app_id": apps["statistical"],
    }


def state_compatibility(integration: Path) -> dict[str, object]:
    """Claim a fresh state lineage or reject changes before a retry.

    The record is experiment-scoped and must precede the first statistical query.
    A pre-existing checkpoint without a record is never silently adopted.
    """
    identity = inspect_identity(integration)
    record = _semantic_record(identity)
    record["semantic_fingerprint"] = digest(record)
    path = integration / "statistical-state-compatibility.json"
    checkpoint = Path(cast(dict[str, str], identity["checkpoints"])["statistical"])
    if path.exists():
        stored = read_object(path)
        if stored != record:
            raise ReliabilityError("Statistical checkpoint configuration fingerprint changed")
        return stored
    if checkpoint.exists():
        raise ReliabilityError("Existing statistical checkpoint lacks a compatibility record")
    path.write_bytes(canonical_json(record))
    return record


def target_identity(integration: Path) -> dict[str, object]:
    """Bind the committed target table ID before a retry or downstream handoff."""
    identity = inspect_identity(integration)
    path = Path(cast(dict[str, str], identity["paths"])["statistical"])
    value: dict[str, object] = {
        "contract_version": "statistical-target-identity-v1",
        "target_path": str(path.resolve()),
        "table_id": _table_id(path),
    }
    location = integration / "statistical-target-identity.json"
    if location.exists():
        if read_object(location) != value:
            raise ReliabilityError("Statistical target Delta identity changed")
    else:
        location.write_bytes(canonical_json(value))
    return value


def inspect_state_compatibility(integration: Path) -> dict[str, object]:
    identity = inspect_identity(integration)
    path = integration / "statistical-state-compatibility.json"
    if not path.is_file():
        raise ReliabilityError("Missing statistical state compatibility record")
    record = read_object(path)
    expected = _semantic_record(identity)
    expected["semantic_fingerprint"] = digest(expected)
    if record != expected:
        raise ReliabilityError("Statistical state compatibility record differs")
    target = integration / "statistical-target-identity.json"
    if target.exists():
        target_identity(integration)
    return record


def validate_stage_paths(integration: Path) -> None:
    identity = inspect_identity(integration)
    paths = cast(dict[str, str], identity["paths"])
    checkpoints = cast(dict[str, str], identity["checkpoints"])
    if set(paths) != set(STAGES) or set(checkpoints) != set(STAGES):
        raise ReliabilityError("Four-stage lineage is incomplete")
