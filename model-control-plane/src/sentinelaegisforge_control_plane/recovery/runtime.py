"""Restore-only execution of existing applications from their pinned compiled classpath.

The Phase 28 harness is reused with an explicitly scoped command transport adapter:
JVM entry points replace host sbt/Make invocation; processing contracts are unchanged.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import time
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.incremental_shadow.cli import main as worker_cli
from sentinelaegisforge_control_plane.incremental_shadow.worker import verify_reference
from sentinelaegisforge_control_plane.registry.run import RegistryConfig
from sentinelaegisforge_control_plane.reliability.observation import _progress
from sentinelaegisforge_control_plane.reliability.state_examples import examples
from sentinelaegisforge_control_plane.stateful_shadow_integration import cli as harness
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import (
    inspect_identity,
    read_plan,
)
from sentinelaegisforge_control_plane.stateful_shadow_integration.reconciliation import (
    verify_final,
    verify_wave,
)

from .bundle import verify
from .contract import RecoveryError, read, sha, write
from .lineage import Selection, compare_identity, dependencies

PREFIX = "io.sentinelaegisforge.streaming."


def selection(cutoff: dict[str, object]) -> Selection:
    return Selection(**{k: Path(v) for k, v in cast(dict[str, str], cutoff["selection"]).items()})


def inspect_restored(bundle: Path) -> dict[str, object]:
    manifest = verify(bundle)
    cutoff = read(bundle / "source_cutoff.json")
    if Path(str(cutoff["source_only_sentinel"])).exists():
        raise RecoveryError("Original source sentinel is accessible; refuse fallback")
    # This check deliberately uses logical absolute paths, only inside isolated mounts.
    import json

    for item in json.loads((bundle / "source_inventory.json").read_bytes()):
        if sha(Path(item["path"])) != item["sha256"]:
            raise RecoveryError(f"Missing/corrupt restored dependency: {item['path']}")
    observed = dependencies(selection(cutoff))
    for key in observed:
        compare_identity(cutoff[key], observed[key], key)
    return {
        "bundle_id": manifest["bundle_id"],
        "status": "PARTIALLY_VERIFIED",
        "local_authorities": "VERIFIED",
        "pending_work": observed["research_progress"],
        "broker_identity": "REQUIRES_HOST_DOCKER_PROBE",
        "stateful_resume": "NOT_EXECUTED",
    }


def run_stage(
    target: str,
    identity: dict[str, object],
    extra: dict[str, str] | None = None,
    label: str | None = None,
) -> float:
    values = extra or {}
    paths = cast(dict[str, str], identity["paths"])
    cps = cast(dict[str, str], identity["checkpoints"])
    apps = cast(dict[str, str], identity["transaction_app_ids"])
    wave = values.get("SCENARIO_WAVE", "resume")
    root = Path(str(identity["integration_path"])) / "recovery-logs"
    root.mkdir(exist_ok=True)
    log = root / f"{target}-{label or wave}.log"
    started = time.perf_counter()
    if target == "process-incremental-shadow-work":
        result = worker_cli(
            [
                "process",
                "--stream",
                values["INCREMENTAL_STREAM_PATH"],
                "--model-version",
                "1",
                "--score-start",
                "2030-01-04T00:00:00Z",
                "--score-end",
                "2030-01-10T00:00:00Z",
                "--maximum",
                values["INCREMENTAL_TICK_LIMIT"],
            ]
        )
        if result:
            raise RecoveryError("Restored inference failed")
        return time.perf_counter() - started
    master = ["--spark-master=local[*]"]
    broker = ["--bootstrap-servers=localhost:9092", "--schema-registry-url=http://localhost:8081"]
    stage_args = {
        "ingest-delta": (
            "persistence.delta.DeltaTransactionIngestionApp",
            broker
            + master
            + [
                "--topic=transactions.raw",
                "--starting-offsets=latest",
                f"--delta-path={paths['validated']}",
                f"--checkpoint-location={cps['validated']}",
                f"--delta-txn-app-id={apps['validated']}",
            ],
        ),
        "deduplicate-transactions": (
            "processing.deduplication.TransactionDeduplicationApp",
            master
            + [
                f"--validated-delta-path={paths['validated']}",
                f"--deduplicated-delta-path={paths['deduplicated']}",
                f"--checkpoint-location={cps['deduplicated']}",
                f"--delta-txn-app-id={apps['deduplicated']}",
            ],
        ),
        "process-rolling-features": (
            "processing.rollingfeatures.TransactionRollingFeatureApp",
            master
            + [
                f"--deduplicated-delta-path={paths['deduplicated']}",
                f"--feature-delta-path={paths['rolling']}",
                f"--checkpoint-location={cps['rolling']}",
                f"--delta-txn-app-id={apps['rolling']}",
            ],
        ),
        "process-statistical-features": (
            "processing.statisticalfeatures.TransactionStatisticalFeatureApp",
            master
            + [
                f"--source-delta-path={paths['rolling']}",
                f"--target-delta-path={paths['statistical']}",
                f"--checkpoint-location={cps['statistical']}",
                f"--delta-txn-app-id={apps['statistical']}",
            ],
        ),
        "publish-synthetic-wave": (
            "simulation.SyntheticScenarioApp",
            ["publish", *broker, f"--corpus={identity['corpus_path']}", f"--wave={wave}"],
        ),
    }
    if target in {"publish-incremental-shadow-batch", "accept-incremental-shadow-batch"}:
        stream = values["INCREMENTAL_STREAM_PATH"]
        batch = values["INCREMENTAL_BATCH_ID"]
        args = [
            "--bootstrap-servers=localhost:9092",
            f"--batch={values['INCREMENTAL_BATCH_PATH']}",
            f"--journal-root={stream}/journals",
            f"--observation-root={stream}/observations",
        ]
        if target.startswith("publish"):
            args += ["--mode=publish", "--maximum-deliveries=5000"]
        else:
            args += [
                "--mode=accept-operational-batch",
                f"--group={identity['group_id']}",
                f"--receipt-root={stream}/receipts/{batch}",
                f"--acceptance-path={stream}/acceptance/{batch}.json",
                f"--preceding-boundary={values['INCREMENTAL_PRECEDING_BOUNDARY']}",
                "--maximum=5000",
            ]
        main = "kafka.featureavailability.FeatureReadyKafkaApp"
    elif target in stage_args:
        main, args = stage_args[target]
    else:
        raise RecoveryError(f"Unsupported restore command: {target}")
    env = {
        **os.environ,
        "WATERMARK_DELAY": "10 minutes",
        "STATISTICAL_FEATURE_INACTIVITY": "24 hours",
    }
    with log.open("xb") as output:
        completed = subprocess.run(
            ["java", "-cp", "/runtime/jvm/*", PREFIX + main, *args],
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if completed.returncode:
        raise RecoveryError(f"Restored {target} failed; inspect {log}")
    return time.perf_counter() - started


def resume(bundle: Path, evidence: Path) -> dict[str, object]:
    before = inspect_restored(bundle)
    cutoff = read(bundle / "source_cutoff.json")
    selected = selection(cutoff)
    identity = inspect_identity(selected.integration)
    timings: dict[str, float] = {}
    started = time.perf_counter()
    original_runner = harness._make
    try:
        harness._make = run_stage
        timings["pending_stateful_wave"] = run_stage(
            "process-statistical-features", identity, {"SCENARIO_WAVE": "1"}
        )
        report = verify_wave(selected.integration, 1)
        write(selected.integration / "waves/1.json", report)
        args = argparse.Namespace(
            integration=selected.integration,
            stream=selected.stream,
            wave=1,
            pause_inference=False,
            demonstrate_backpressure=False,
        )
        harness._handoff(args)
        args.wave = 2
        harness._run_wave(args)
        harness._handoff(args)
    finally:
        harness._make = original_runner
    final = verify_final(selected.integration, selected.stream)
    parity = verify_reference(selected.stream, selected.candidate, RegistryConfig())
    continuity = examples(
        Path(cast(dict[str, str], identity["paths"])["statistical"]),
        read_plan(Path(str(identity["corpus_path"]))),
        _progress(selected.integration / "logs/process-statistical-features-0.log", "statistical"),
    )
    if continuity["retained"] is None:
        raise RecoveryError("No verified customer/currency state continuity across restoration")
    state_progress = _progress(
        selected.integration / "recovery-logs/process-statistical-features-1.log", "statistical"
    )
    old_metadata = cast(dict[str, dict[str, object]], cutoff["checkpoints"])["statistical"]
    metadata_file = Path(str(old_metadata["path"])) / "metadata"
    import json

    query_id = json.loads(metadata_file.read_bytes())["id"]
    if any(
        batch["id"] != query_id
        for batch in cast(list[dict[str, object]], state_progress["batches"])
    ):
        raise RecoveryError("Statistical query identity changed on restore")
    after = dependencies(selected)
    for key in (
        "candidate_id",
        "model_tree_sha256",
        "registry",
        "assessment_id",
        "runtime",
        "state_compatibility",
        "plan_event_ids",
    ):
        compare_identity(cutoff[key], after[key], key)
    # Every pre-cutoff Delta version remains readable with exact rows/IDs after resumed appends.
    for name, history in cast(dict[str, list[object]], cutoff["delta_histories"]).items():
        compare_identity(
            history,
            cast(dict[str, list[object]], after["delta_histories"])[name][: len(history)],
            name,
        )
    # Original durable manifests/results/journals are immutable. Checkpoints legitimately advance.
    for item in json.loads((bundle / "source_inventory.json").read_bytes()):
        path = Path(item["path"])
        if path.is_relative_to(selected.stream) or path.is_relative_to(selected.candidate):
            if sha(path) != item["sha256"]:
                raise RecoveryError("Historical research/model evidence changed after resume")
    result = {
        "bundle_id": before["bundle_id"],
        "simulation_status": "VERIFIED_RESTORED",
        "storage_independence": "SIMULATED_SAME_HOST_DISK",
        "before": before,
        "final_reconciliation": final,
        "probability_parity": parity,
        "statistical_continuity": continuity,
        "statistical_checkpoint_progress": state_progress,
        "after": after["research_progress"],
        "historical_content": "UNCHANGED",
        "timings": {**timings, "resume_seconds": time.perf_counter() - started},
    }
    write(evidence / "resumed.json", result)
    return result
