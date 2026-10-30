"""Dependency closure for the existing Phase 28/29 synthetic research lineage."""

from __future__ import annotations

import platform
import sqlite3
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import cast

from deltalake import DeltaTable

from sentinelaegisforge_control_plane.candidates.run import inspect_candidate
from sentinelaegisforge_control_plane.incremental_shadow.operations import inspect as inspect_stream
from sentinelaegisforge_control_plane.incremental_shadow.progress import progress
from sentinelaegisforge_control_plane.reliability.contract import inspect_state_compatibility
from sentinelaegisforge_control_plane.reliability.operations import _commit_rows
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import (
    CANDIDATE_ID,
    STAGES,
    inspect_identity,
    read_plan,
)
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig

from .contract import RecoveryError, files, inventory, read, sha, sqlite_identity

MODEL = "sentinelaegisforge-offline-fraud-logistic"


@dataclass(frozen=True)
class Selection:
    repository: Path
    integration: Path
    stream: Path
    candidate: Path
    assessment: Path

    @property
    def tracking(self) -> TrackingConfig:
        return TrackingConfig(
            self.repository / ".local/mlflow/tracking.db",
            self.repository / ".local/mlflow/artifacts",
        )

    def paths(self) -> dict[str, str]:
        return {key: str(value) for key, value in vars(self).items()}


def registry_identity(database: Path) -> dict[str, object]:
    logical = sqlite_identity(database)
    with sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True) as db:
        records = db.execute(
            "SELECT name,version,source,run_id,status FROM model_versions WHERE name=?",
            (MODEL,),
        ).fetchall()
        if len(records) != 1 or records[0][1] != 1 or records[0][4] != "READY":
            raise RecoveryError("Expected the single original READY Registry version 1")
        tags = dict(
            db.execute(
                "SELECT key,value FROM model_version_tags WHERE name=? AND version=1", (MODEL,)
            ).fetchall()
        )
        if (
            tags.get("candidate_id") != CANDIDATE_ID
            or tags.get("governance_state") != "REGISTERED_RESEARCH_ONLY"
            or tags.get("production_eligible") != "false"
        ):
            raise RecoveryError("Registry identity or research-only governance differs")
        return {"database": logical, "model_version": list(records[0]), "tags": tags}


def runtime() -> dict[str, object]:
    return {
        "python": platform.python_version(),
        "packages": {
            name: version(name)
            for name in ("mlflow", "scikit-learn", "pandas", "skops", "deltalake", "pyarrow")
        },
        "spark": "4.2.0",
        "scala": "2.13.18",
        "delta": "4.4.0",
        "jdk_major": 21,
        "kafka_image": "apache/kafka:4.3.1",
        "registry_image": "confluentinc/cp-schema-registry:8.3.2",
    }


def delta_history(path: Path) -> list[dict[str, object]]:
    latest = DeltaTable(path).version()
    if latest > 63:
        raise RecoveryError("Selected history exceeds bounded 64-version recovery scope")
    result = []
    for ver in range(latest + 1):
        table = DeltaTable(path, version=ver)
        # Read every retained version, not only logs or the current active Parquet set.
        row = _commit_rows(path, ver)
        result.append({**row, "schema": str(table.schema()), "protocol": str(table.protocol())})
    return result


def dependencies(selection: Selection, *, deep: bool = True) -> dict[str, object]:
    """No inference truth/label data is passed into scoring; historical provenance is retained."""
    identity = inspect_identity(selection.integration)
    compatibility = inspect_state_compatibility(selection.integration)
    stream = inspect_stream(selection.stream)
    if any(selection.stream.rglob(".pending-*")):
        raise RecoveryError("Incomplete local inference claim; inspect before capture")
    if (
        stream["source_delta_path"] != cast(dict[str, str], identity["paths"])["statistical"]
        or stream["group_id"] != identity["group_id"]
        or stream["candidate_id"] != CANDIDATE_ID
    ):
        raise RecoveryError("Selected integration/stream/candidate do not share a lineage")
    if not (selection.integration / "waves/0.json").is_file():
        raise RecoveryError("At least one real Scala wave must have completed")
    candidate = (
        inspect_candidate(selection.candidate, tracking=selection.tracking)
        if deep
        else read(selection.candidate / "candidate_manifest.json")
    )
    if candidate["candidate_id"] != CANDIDATE_ID:
        raise RecoveryError("Unexpected research candidate")
    registered = registry_identity(selection.tracking.database)
    if (
        cast(list[object], registered["model_version"])[2]
        != (selection.candidate / "model").as_uri()
    ):
        raise RecoveryError("Registry source URI differs from selected candidate")
    assessment = read(selection.assessment / "assessment.json")
    evidence = read(selection.assessment / "evidence_manifest.json")
    if (
        assessment.get("production_eligible") is not False
        or assessment.get("candidate_id") != CANDIDATE_ID
        or "NOT_ELIGIBLE_FOR_PROMOTION" not in assessment.values()
        or evidence["assessment_sha256"] != sha(selection.assessment / "assessment.json")
        or evidence["report_sha256"] != sha(selection.assessment / "validation_report.md")
    ):
        raise RecoveryError("Phase 18 evidence or governance differs")
    training = Path(cast(str, candidate["source_training_path"]))
    training_manifest = read(training / "manifest.json")
    snapshot = Path(cast(str, training_manifest["dataset_snapshot_path"]))
    snapshot_manifest = read(snapshot / "manifest.json")
    backend = hashlib_backend(selection.tracking.tracking_uri)
    receipt = (
        selection.tracking.database.parent
        / "registration-receipts"
        / backend
        / MODEL
        / f"{CANDIDATE_ID}.json"
    )
    corpus_id = cast(dict[str, object], evidence["identity"])["scenario_corpus_id"]
    source_corpus = selection.repository / ".local/ml/scenarios" / str(corpus_id)
    run_id = str(candidate["source_mlflow_run_id"])
    nodes: list[dict[str, object]] = []

    def add(key: str, path: Path, reason: str, requires: tuple[str, ...] = ()) -> None:
        if not path.is_relative_to(selection.repository / ".local"):
            raise RecoveryError(f"Dependency escapes selected local authority: {path}")
        files(path)
        nodes.append(
            {
                "id": key,
                "path": str(path),
                "role": key,
                "reason": reason,
                "requires": list(requires),
            }
        )

    add(
        "integration",
        selection.integration,
        "Selected real Scala identity, logs and progress",
        (
            "corpus",
            "transaction_fence",
            *(f"delta_{s}" for s in STAGES),
            *(f"checkpoint_{s}" for s in STAGES),
        ),
    )
    add("corpus", Path(str(identity["corpus_path"])), "Original deterministic plan and wave acks")
    add("transaction_fence", Path(str(identity["transaction_fence_path"])), "Original input fence")
    add(
        "stream",
        selection.stream,
        "Outbox, journals, receipts, results and exact progress",
        ("stream_fence", "delta_statistical", "candidate"),
    )
    add("stream_fence", Path(str(stream["fence_path"])), "Research topic/group offset identity")
    histories: dict[str, object] = {}
    checkpoints: dict[str, object] = {}
    for stage in STAGES:
        path = Path(cast(dict[str, str], identity["paths"])[stage])
        checkpoint = Path(cast(dict[str, str], identity["checkpoints"])[stage])
        add(f"delta_{stage}", path, "Complete retained log and data history for source/replay")
        add(f"checkpoint_{stage}", checkpoint, "Original offsets, commits, state and query ID")
        for required in ("metadata", "offsets", "commits"):
            files(checkpoint / required)
        if stage != "validated":
            files(checkpoint / "state")
        if stage in {"rolling", "statistical"} and not any(
            p.suffix in {".zip", ".sst"} for p in files(checkpoint / "state")
        ):
            raise RecoveryError("Missing required RocksDB state")
        histories[stage] = delta_history(path)
        checkpoints[stage] = {
            "path": str(checkpoint),
            "inventory": inventory([checkpoint]),
            "txn_app_id": cast(dict[str, str], identity["transaction_app_ids"])[stage],
        }
    add(
        "candidate",
        selection.candidate,
        "Exact serialized research model and equivalence",
        ("training", "tracking_artifacts", "sqlite"),
    )
    add("training", training, "Original metrics, manifest and prediction reference", ("snapshot",))
    add(
        "snapshot",
        snapshot,
        "Immutable labeled dataset, not model inference input",
        ("training_delta", "labels"),
    )
    add(
        "training_delta",
        Path(str(snapshot_manifest["source_delta_path"])),
        "All retained training feature versions referenced by provenance",
    )
    histories["training"] = delta_history(Path(str(snapshot_manifest["source_delta_path"])))
    add(
        "labels", Path(str(snapshot_manifest["label_source_file"])), "Dataset label-byte provenance"
    )
    add("training_corpus", source_corpus, "Phase 18 tracked synthetic truth provenance only")
    add(
        "tracking_artifacts",
        selection.tracking.artifacts / run_id,
        "Existing training-run tracking artifact hashes",
    )
    add("sqlite", selection.tracking.database, "Consistent original tracking/Registry authority")
    add(
        "registry_receipt",
        receipt,
        "Original registration receipt identity",
        ("candidate", "sqlite"),
    )
    add(
        "assessment",
        selection.assessment,
        "Original production-ineligible Phase 18 dossier",
        ("registry_receipt", "training_corpus"),
    )
    from .contract import validate_graph

    validate_graph(nodes)
    plan = read_plan(Path(str(identity["corpus_path"])))
    current = progress(selection.stream)
    return {
        "selection": selection.paths(),
        "lineage_id": identity["integration_id"],
        "corpus_id": plan.corpus_id,
        "graph": nodes,
        "runtime": runtime(),
        "state_compatibility": compatibility,
        "delta_histories": histories,
        "checkpoints": checkpoints,
        "research_progress": current,
        "candidate_id": CANDIDATE_ID,
        "model_tree_sha256": candidate["model_tree_sha256"],
        "registry": registered,
        "assessment_id": assessment["assessment_id"],
        "plan_event_ids": sorted(row["event_id"] for row in plan.rows),
        "excluded_dependencies": {
            "phase19": (
                "New isolated stream uses direct restored-candidate reference scoring, "
                "not its ledger"
            ),
            "phase22": (
                "Feedback is not an inference input or a selected model provenance dependency"
            ),
            "other_experiments": "No other Phase 28/29 checkpoint or result is referenced",
        },
    }


def hashlib_backend(uri: str) -> str:
    import hashlib

    return hashlib.sha256(uri.encode()).hexdigest()


def compare_identity(expected: object, observed: object, name: str) -> None:
    if expected != observed:
        raise RecoveryError(f"Restored {name} identity/content differs")
