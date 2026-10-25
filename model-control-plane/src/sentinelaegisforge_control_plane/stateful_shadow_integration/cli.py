"""Operator-triggered Phase 28 harness over existing Make, Scala and research stages."""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.availability.contract import (
    AvailabilityError,
    publish,
    read_object,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.incremental_shadow.operations import (
    discover_next,
    records,
)
from sentinelaegisforge_control_plane.incremental_shadow.operations import (
    inspect as inspect_stream,
)
from sentinelaegisforge_control_plane.incremental_shadow.progress import (
    authorize_publication,
    progress,
)

from .contract import IntegrationError, create_identity, inspect_identity
from .evidence import build as build_evidence
from .evidence import inspect as inspect_evidence
from .reconciliation import trace_event, verify_final, verify_wave

REPO = Path(__file__).resolve().parents[4]


def _make(
    target: str,
    identity: dict[str, object],
    extra: dict[str, str] | None = None,
    label: str | None = None,
) -> float:
    paths = cast(dict[str, str], identity["paths"])
    checkpoints = cast(dict[str, str], identity["checkpoints"])
    apps = cast(dict[str, str], identity["transaction_app_ids"])
    variables = {
        "SCENARIO_CORPUS_PATH": cast(str, identity["corpus_path"]),
        "DELTA_PATH": paths["validated"],
        "DELTA_CHECKPOINT_DIR": checkpoints["validated"],
        "DELTA_TXN_APP_ID": apps["validated"],
        "DEDUPLICATED_DELTA_PATH": paths["deduplicated"],
        "DEDUP_CHECKPOINT_DIR": checkpoints["deduplicated"],
        "DEDUP_DELTA_TXN_APP_ID": apps["deduplicated"],
        "FEATURE_DELTA_PATH": paths["rolling"],
        "FEATURE_CHECKPOINT_DIR": checkpoints["rolling"],
        "FEATURE_DELTA_TXN_APP_ID": apps["rolling"],
        "STATISTICAL_FEATURE_DELTA_PATH": paths["statistical"],
        "STATISTICAL_FEATURE_CHECKPOINT_DIR": checkpoints["statistical"],
        "STATISTICAL_FEATURE_DELTA_TXN_APP_ID": apps["statistical"],
        "WATERMARK_DELAY": "10 minutes",
        "STARTING_OFFSETS": "latest",
    }
    variables.update(extra or {})
    log_dir = Path(cast(str, identity["integration_path"])) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{target}-{label or variables.get('SCENARIO_WAVE', 'init')}.log"
    if log_path.exists():
        raise IntegrationError(f"Stage log already exists; inspect before retry: {log_path}")
    command = ["make", "--no-print-directory", target, *[f"{k}={v}" for k, v in variables.items()]]
    started = time.perf_counter()
    with log_path.open("xb") as output:
        result = subprocess.run(
            command,
            cwd=REPO,
            env=os.environ.copy(),
            stdout=output,
            stderr=subprocess.STDOUT,
            check=False,
        )
    elapsed = time.perf_counter() - started
    if result.returncode:
        raise IntegrationError(f"{target} failed ({result.returncode}); inspect {log_path}")
    return elapsed


def _identity(args: argparse.Namespace) -> dict[str, object]:
    return inspect_identity(args.integration)


def _initialize(args: argparse.Namespace) -> dict[str, object]:
    identity = create_identity(args.corpus, args.root, args.transaction_fence)
    path = Path(cast(str, identity["integration_path"]))
    if path.exists():
        return inspect_identity(path)
    if any(Path(value).exists() for value in cast(dict[str, str], identity["paths"]).values()):
        raise IntegrationError("Isolated Delta path already exists")
    publish(path / "identity", {"integration_manifest.json": canonical_json(identity)})
    return inspect_identity(path)


def _prime(args: argparse.Namespace) -> dict[str, object]:
    identity = _identity(args)
    path = args.integration / "source-prime.json"
    if path.exists():
        return read_object(path)
    if any((args.integration / "waves").glob("*.json")):
        raise IntegrationError("Cannot initialize source offsets after publication")
    elapsed = _make("ingest-delta", identity)
    checkpoint = Path(cast(dict[str, str], identity["checkpoints"])["validated"])
    if not checkpoint.is_dir():
        raise IntegrationError("Validated ingestion checkpoint was not initialized")
    value: dict[str, object] = {
        "contract_version": "phase28-source-prime-v1",
        "integration_id": identity["integration_id"],
        "checkpoint": str(checkpoint),
        "starting_offsets": identity["starting_offsets"],
        "elapsed_seconds": elapsed,
    }
    path.write_bytes(canonical_json(value))
    return value


def _run_wave(args: argparse.Namespace) -> dict[str, object]:
    identity = _identity(args)
    if args.wave not in range(3):
        raise IntegrationError("Wave must be 0, 1 or 2")
    if not (args.integration / "source-prime.json").is_file():
        raise IntegrationError("Prime fresh latest-offset ingestion before publication")
    if args.wave and not (args.integration / "waves" / f"{args.wave - 1}.json").is_file():
        raise IntegrationError("Complete the previous wave before publishing another")
    if (args.integration / "waves" / f"{args.wave}.json").exists():
        raise IntegrationError("Wave already completed; use read-only verification")
    timings: dict[str, float] = {}
    timings["transaction_publication"] = _make(
        "publish-synthetic-wave", identity, {"SCENARIO_WAVE": str(args.wave)}
    )
    for stage, target in (
        ("validated", "ingest-delta"),
        ("deduplicated", "deduplicate-transactions"),
        ("rolling", "process-rolling-features"),
        ("statistical", "process-statistical-features"),
    ):
        timings[stage] = _make(target, identity, {"SCENARIO_WAVE": str(args.wave)})
    report = verify_wave(args.integration, args.wave)
    wave_path = args.integration / "waves"
    wave_path.mkdir(parents=True, exist_ok=True)
    (wave_path / f"{args.wave}.json").write_bytes(canonical_json(report))
    timing_dir = args.integration / "timings"
    timing_dir.mkdir(parents=True, exist_ok=True)
    (timing_dir / f"wave-{args.wave}.json").write_bytes(
        canonical_json(
            {
                "clock": "Python time.perf_counter; per-invocation elapsed seconds",
                "wave": args.wave,
                "durations": timings,
            }
        )
    )
    return report


def _inspect_wave(args: argparse.Namespace) -> dict[str, object]:
    report = verify_wave(args.integration, args.wave)
    saved = read_object(args.integration / "waves" / f"{args.wave}.json")
    if report != saved:
        raise IntegrationError("Saved wave evidence differs from current committed Delta state")
    return report


def _final(args: argparse.Namespace) -> dict[str, object]:
    identity = _identity(args)
    report = verify_final(args.integration, args.stream)
    if report["integration_id"] != identity["integration_id"]:
        raise IntegrationError("Final report identity differs")
    return report


def _trace(args: argparse.Namespace) -> dict[str, object]:
    return trace_event(args.integration, args.stream, args.event_id)


def _build_evidence(args: argparse.Namespace) -> dict[str, object]:
    return build_evidence(args.integration, args.stream, args.candidate, args.recovery)


def _inspect_evidence(args: argparse.Namespace) -> dict[str, object]:
    return inspect_evidence(args.integration / "evidence")


def _inspect_transport(args: argparse.Namespace) -> dict[str, object]:
    identity = _identity(args)
    versions = records(args.stream)
    summary = progress(args.stream)
    return {
        "integration_id": identity["integration_id"],
        "stream_id": summary["stream_id"],
        "discovered_delta_versions": [record["delta_version"] for record in versions],
        "ready_events": sum(cast(int, record["ready_event_count"]) for record in versions),
        "progress": summary,
    }


def _drain(
    stream: Path, identity: dict[str, object], wave: int, timings: list[dict[str, object]]
) -> None:
    log_dir = Path(cast(str, identity["integration_path"])) / "logs"
    tick = len(list(log_dir.glob(f"process-incremental-shadow-work-wave-{wave}-tick-*.log")))
    initial = tick
    while cast(int, progress(stream)["inference_backlog_ready_events"]) > 0:
        if tick - initial >= 10:
            raise IntegrationError("Research inference did not drain within ten bounded ticks")
        elapsed = _make(
            "process-incremental-shadow-work",
            identity,
            {
                "INCREMENTAL_STREAM_PATH": str(stream),
                "INCREMENTAL_MODEL_VERSION": "1",
                "INCREMENTAL_TICK_LIMIT": "500",
                "INCREMENTAL_SCORE_START": "2030-01-04T00:00:00Z",
                "INCREMENTAL_SCORE_END": "2030-01-10T00:00:00Z",
            },
            label=f"wave-{wave}-tick-{tick}",
        )
        timings.append({"stage": "research_inference", "elapsed_seconds": elapsed, "tick": tick})
        tick += 1


def _handoff(args: argparse.Namespace) -> dict[str, object]:
    identity = _identity(args)
    stream = args.stream
    manifest = inspect_stream(stream)
    if (
        manifest["source_delta_path"] != cast(dict[str, str], identity["paths"])["statistical"]
        or manifest["group_id"] != identity["group_id"]
        or manifest["candidate_id"] != identity["candidate_id"]
    ):
        raise IntegrationError("Research stream is not bound to this Scala feature lineage")
    if not (args.integration / "waves" / f"{args.wave}.json").is_file():
        raise IntegrationError("Committed Scala feature wave must reconcile before handoff")
    evidence_path = args.integration / "handoffs" / f"{args.wave}.json"
    if evidence_path.exists():
        return read_object(evidence_path)
    timings: list[dict[str, object]] = []
    new_versions: list[int] = []
    paused = False
    while True:
        started = time.perf_counter()
        record = discover_next(stream)
        if record is None:
            break
        timings.append(
            {
                "stage": "commit_discovery_and_outbox",
                "elapsed_seconds": time.perf_counter() - started,
                "delta_version": record["delta_version"],
            }
        )
        new_versions.append(cast(int, record["delta_version"]))
    for record in records(stream):
        batch_id = record["publication_batch_id"]
        if batch_id is None or (stream / "acceptance" / f"{batch_id}.json").is_file():
            continue
        prior = [
            item
            for item in records(stream)
            if cast(int, item["delta_version"]) < cast(int, record["delta_version"])
            and item["publication_batch_id"] is not None
        ]
        boundary = (
            stream / "acceptance" / f"{prior[-1]['publication_batch_id']}.json"
            if prior
            else Path(cast(str, manifest["fence_path"]))
        )
        try:
            authorize_publication(stream, cast(str, batch_id))
        except AvailabilityError as error:
            if not args.demonstrate_backpressure or "PAUSED_BACKPRESSURE" not in str(error):
                raise
            paused = True
            _drain(stream, identity, args.wave, timings)
            authorize_publication(stream, cast(str, batch_id))
        variables = {
            "INCREMENTAL_STREAM_PATH": str(stream),
            "INCREMENTAL_BATCH_ID": cast(str, batch_id),
            "INCREMENTAL_BATCH_PATH": cast(str, record["batch_path"]),
            "INCREMENTAL_PRECEDING_BOUNDARY": str(boundary),
            "INCREMENTAL_GROUP": cast(str, identity["group_id"]),
            "FEATURE_KAFKA_MAXIMUM": "5000",
        }
        for stage, target in (
            ("ready_kafka_publication", "publish-incremental-shadow-batch"),
            ("ready_kafka_acceptance", "accept-incremental-shadow-batch"),
        ):
            elapsed = _make(
                target, identity, variables, label=f"wave-{args.wave}-{str(batch_id)[:12]}"
            )
            timings.append({"stage": stage, "elapsed_seconds": elapsed, "batch_id": batch_id})
    if not args.pause_inference:
        _drain(stream, identity, args.wave, timings)
    summary = progress(stream)
    result: dict[str, object] = {
        "contract_version": "phase28-wave-handoff-v1",
        "integration_id": identity["integration_id"],
        "stream_id": manifest["stream_id"],
        "wave": args.wave,
        "discovered_versions": new_versions,
        "backpressure_pause_observed": paused,
        "inference_intentionally_paused": args.pause_inference,
        "progress": summary,
    }
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_bytes(canonical_json(result))
    timing_dir = args.integration / "timings"
    timing_dir.mkdir(parents=True, exist_ok=True)
    (timing_dir / f"handoff-{args.wave}.json").write_bytes(
        canonical_json(
            {"clock": "Python time.perf_counter", "wave": args.wave, "observations": timings}
        )
    )
    return result


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    init = commands.add_parser("initialize")
    init.add_argument("--corpus", type=Path, required=True)
    init.add_argument("--root", type=Path, required=True)
    init.add_argument("--transaction-fence", type=Path, required=True)
    for name in (
        "prime",
        "run-wave",
        "inspect-wave",
        "handoff",
        "inspect-transport",
        "verify-final",
        "trace",
        "build-evidence",
        "inspect-evidence",
    ):
        item = commands.add_parser(name)
        item.add_argument("--integration", type=Path, required=True)
        if name in {"run-wave", "inspect-wave", "handoff"}:
            item.add_argument("--wave", type=int, required=True)
        if name in {"handoff", "inspect-transport", "verify-final", "trace", "build-evidence"}:
            item.add_argument("--stream", type=Path, required=True)
        if name == "handoff":
            item.add_argument("--pause-inference", action="store_true")
            item.add_argument("--demonstrate-backpressure", action="store_true")
        if name == "trace":
            item.add_argument("--event-id", required=True)
        if name == "build-evidence":
            item.add_argument("--candidate", type=Path, required=True)
            item.add_argument("--recovery", type=Path, required=True)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = {
            "initialize": _initialize,
            "prime": _prime,
            "run-wave": _run_wave,
            "inspect-wave": _inspect_wave,
            "handoff": _handoff,
            "inspect-transport": _inspect_transport,
            "verify-final": _final,
            "trace": _trace,
            "build-evidence": _build_evidence,
            "inspect-evidence": _inspect_evidence,
        }[args.command](args)
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Stateful shadow integration failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
