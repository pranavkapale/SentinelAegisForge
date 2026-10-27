"""Bounded Phase 29 operations that reuse the Phase 28 real pipeline."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pyarrow.fs as pa_fs
from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import read_object
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.stateful_shadow_integration.cli import _make
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import (
    IntegrationError,
    inspect_identity,
)
from sentinelaegisforge_control_plane.stateful_shadow_integration.reconciliation import verify_wave

from .contract import (
    INACTIVITY,
    ReliabilityError,
    inspect_state_compatibility,
    state_compatibility,
    target_identity,
)


def _write_exclusive(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(canonical_json(value))


def _commit_rows(path: Path, version: int | None = None) -> dict[str, object]:
    table = DeltaTable(path, version=version)
    filesystem = pa_fs.SubTreeFileSystem(str(path.resolve()), pa_fs.LocalFileSystem())
    rows = cast(list[dict[str, object]], table.to_pyarrow_table(filesystem=filesystem).to_pylist())
    ids = [cast(str, row["event_id"]) for row in rows]
    if len(ids) != len(set(ids)):
        raise ReliabilityError("Duplicate statistical event IDs after post-commit failure")

    def normalize(value: object) -> object:
        if isinstance(value, (datetime, Decimal)):
            return str(value)
        if isinstance(value, bytes):
            return value.hex()
        if isinstance(value, list):
            return [normalize(item) for item in value]
        if isinstance(value, dict):
            return {str(key): normalize(item) for key, item in value.items()}
        return value

    ordered = sorted(rows, key=lambda row: cast(str, row["event_id"]))
    return {
        "table_id": str(table.metadata().id),
        "delta_version": table.version(),
        "row_count": len(ids),
        "event_id_sha256": hashlib.sha256(canonical_json(sorted(ids))).hexdigest(),
        "row_sha256": hashlib.sha256(canonical_json(normalize(ordered))).hexdigest(),
    }


def run_wave(integration: Path, wave: int, *, fault_batch: int | None = None) -> dict[str, object]:
    """Publish and process one real wave, optionally failing after statistical commit."""
    identity = inspect_identity(integration)
    if wave not in range(3) or (fault_batch is not None and (wave != 0 or fault_batch < 0)):
        raise ReliabilityError("Fault injection is limited to a nonnegative batch in wave zero")
    if not (integration / "source-prime.json").is_file():
        raise ReliabilityError("Fresh latest-offset source must be primed before publication")
    if wave and not (integration / "waves" / f"{wave - 1}.json").is_file():
        raise ReliabilityError("Prior wave must reconcile before continuing")
    if (integration / "waves" / f"{wave}.json").exists():
        raise ReliabilityError("Wave already completed; use read-only inspection")
    timings: dict[str, float] = {}
    timings["transaction_publication"] = _make(
        "publish-synthetic-wave", identity, {"SCENARIO_WAVE": str(wave)}
    )
    for stage, target in (
        ("validated", "ingest-delta"),
        ("deduplicated", "deduplicate-transactions"),
        ("rolling", "process-rolling-features"),
    ):
        timings[stage] = _make(target, identity, {"SCENARIO_WAVE": str(wave)})
    state_compatibility(integration)
    statistical_variables = {
        "SCENARIO_WAVE": str(wave),
        "STATISTICAL_FEATURE_INACTIVITY": INACTIVITY,
        "STATISTICAL_FAIL_AFTER_DELTA_BATCH_ID": "",
    }
    if fault_batch is None:
        timings["statistical"] = _make(
            "process-statistical-features", identity, statistical_variables
        )
    else:
        started = time.perf_counter()
        try:
            _make(
                "process-statistical-features",
                identity,
                {
                    **statistical_variables,
                    "STATISTICAL_FAIL_AFTER_DELTA_BATCH_ID": str(fault_batch),
                },
                label=f"wave-{wave}-injected",
            )
        except IntegrationError:
            pass
        else:
            raise ReliabilityError("Injected statistical post-commit failure did not occur")
        timings["statistical_failed_attempt"] = time.perf_counter() - started
        fault_log = integration / "logs" / f"process-statistical-features-wave-{wave}-injected.log"
        raw = fault_log.read_bytes()
        if b"IntentionalDeltaPostCommitFailure" not in raw:
            raise ReliabilityError("Statistical failure was not the intended post-commit hook")
        statistical = Path(cast(dict[str, str], identity["paths"])["statistical"])
        before = _commit_rows(statistical)
        if before["row_count"] == 0:
            raise ReliabilityError("Fault did not follow a nonempty statistical Delta commit")
        target_identity(integration)
        inspect_state_compatibility(integration)
        timings["statistical_recovery"] = _make(
            "process-statistical-features",
            identity,
            statistical_variables,
            label=f"wave-{wave}-recovery",
        )
        after = _commit_rows(statistical)
        if any(
            before[key] != after[key]
            for key in ("table_id", "row_count", "event_id_sha256", "row_sha256")
        ):
            raise ReliabilityError("Recovered statistical output differs from committed batch")
        _write_exclusive(
            integration / "reliability" / "statistical-postcommit-recovery.json",
            {
                "contract_version": "statistical-postcommit-recovery-v1",
                "integration_id": identity["integration_id"],
                "fault_batch_id": fault_batch,
                "fault_log_sha256": hashlib.sha256(raw).hexdigest(),
                "before_restart": before,
                "after_restart": after,
                "checkpoint": cast(dict[str, str], identity["checkpoints"])["statistical"],
                "txn_app_id": cast(dict[str, str], identity["transaction_app_ids"])["statistical"],
                "status": "VERIFIED",
            },
        )
    target_identity(integration)
    report = verify_wave(integration, wave)
    _write_exclusive(integration / "waves" / f"{wave}.json", report)
    _write_exclusive(
        integration / "timings" / f"wave-{wave}.json",
        {
            "clock": "Python time.perf_counter; per-invocation elapsed seconds",
            "wave": wave,
            "durations": timings,
        },
    )
    return report


def inspect_recovery(integration: Path) -> dict[str, object]:
    inspect_state_compatibility(integration)
    identity = inspect_identity(integration)
    saved = read_object(integration / "reliability" / "statistical-postcommit-recovery.json")
    if (
        saved.get("status") != "VERIFIED"
        or saved.get("integration_id") != identity["integration_id"]
    ):
        raise ReliabilityError("Post-commit recovery evidence is incomplete")
    if (
        saved.get("checkpoint") != cast(dict[str, str], identity["checkpoints"])["statistical"]
        or saved.get("txn_app_id")
        != cast(dict[str, str], identity["transaction_app_ids"])["statistical"]
    ):
        raise ReliabilityError("Post-commit recovery lineage differs")
    fault_log = integration / "logs" / "process-statistical-features-wave-0-injected.log"
    raw = fault_log.read_bytes()
    if (
        hashlib.sha256(raw).hexdigest() != saved.get("fault_log_sha256")
        or b"IntentionalDeltaPostCommitFailure" not in raw
    ):
        raise ReliabilityError("Post-commit failure log changed")
    path = Path(cast(dict[str, str], identity["paths"])["statistical"])
    for key in ("before_restart", "after_restart"):
        original_version = cast(dict[str, object], saved[key])
        if _commit_rows(path, cast(int, original_version["delta_version"])) != original_version:
            raise ReliabilityError("Post-commit historical output evidence differs")
    current = _commit_rows(path)
    original = cast(dict[str, object], saved["after_restart"])
    if current["table_id"] != original["table_id"] or cast(int, current["row_count"]) < cast(
        int, original["row_count"]
    ):
        raise ReliabilityError("Recovered Delta lineage was replaced or truncated")
    committed = cast(dict[str, object], saved["before_restart"])
    log = path / "_delta_log" / f"{cast(int, committed['delta_version']):020d}.json"
    transactions = [
        action["txn"]
        for line in log.read_text(encoding="utf-8").splitlines()
        if "txn" in (action := json.loads(line))
    ]
    expected = [
        item
        for item in transactions
        if item.get("appId") == saved["txn_app_id"]
        and item.get("version") == saved["fault_batch_id"]
    ]
    if len(expected) != 1:
        raise ReliabilityError("Committed statistical Delta transaction action differs")
    checkpoint = Path(cast(str, saved["checkpoint"]))
    commits = sorted(
        item.name for item in (checkpoint / "commits").iterdir() if item.name.isdigit()
    )
    if not commits:
        raise ReliabilityError("Recovered checkpoint has no completed Spark batch")
    return {
        **saved,
        "committed_delta_transaction": expected[0],
        "completed_checkpoint_batches": commits,
    }


def print_result(value: dict[str, object]) -> None:
    print(json.dumps(value, sort_keys=True, indent=2))
