"""Explicit local Phase 29 operations; ordinary unit verification remains offline."""

import argparse
import json
import sys
from pathlib import Path

from sentinelaegisforge_control_plane.availability.contract import read_object
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

from .contract import inspect_state_compatibility, state_compatibility, target_identity
from .evidence import build
from .evidence import inspect as inspect_dossier
from .observation import compare
from .observation import inspect as inspect_observation
from .operations import inspect_recovery, run_wave
from .outage import record_backlog, record_backpressure, record_failed_publication
from .outage import verify as verify_outage
from .retention import inspect as inspect_retention


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("run-wave", "fault-wave", "state", "target", "inspect-state", "inspect-recovery"):
        item = commands.add_parser(name)
        item.add_argument("--integration", type=Path, required=True)
        if name in {"run-wave", "fault-wave"}:
            item.add_argument("--wave", type=int, required=True)
        if name == "fault-wave":
            item.add_argument("--batch-id", type=int, required=True)
    observation = commands.add_parser("observe")
    observation.add_argument("--integration", type=Path, required=True)
    observation.add_argument("--stream", type=Path, required=True)
    observation.add_argument("--output", type=Path)
    retention = commands.add_parser("inspect-retention")
    retention.add_argument("--integration", type=Path, required=True)
    retention.add_argument("--current-fence", type=Path, required=True)
    failure = commands.add_parser("record-broker-failure")
    failure.add_argument("--integration", type=Path, required=True)
    failure.add_argument("--stream", type=Path, required=True)
    failure.add_argument("--batch-id", required=True)
    failure.add_argument("--log", type=Path, required=True)
    failure.add_argument("--exit-code", type=int, required=True)
    for name in ("record-backlog", "record-backpressure"):
        item = commands.add_parser(name)
        item.add_argument("--integration", type=Path, required=True)
        item.add_argument("--stream", type=Path, required=True)
        if name == "record-backlog":
            item.add_argument("--label", required=True)
        else:
            item.add_argument("--batch-id", required=True)
    outage = commands.add_parser("verify-outage")
    outage.add_argument("--integration", type=Path, required=True)
    outage.add_argument("--stream", type=Path, required=True)
    outage.add_argument("--output", type=Path, required=True)
    dossier = commands.add_parser("build-dossier")
    for label in "abcd":
        dossier.add_argument(f"--{label}-integration", type=Path, required=True)
        dossier.add_argument(f"--{label}-stream", type=Path, required=True)
    dossier.add_argument("--outage-evidence", type=Path, required=True)
    dossier.add_argument("--candidate", type=Path, required=True)
    dossier.add_argument("--current-transaction-fence", type=Path, required=True)
    dossier.add_argument("--current-ready-offset-log", type=Path, required=True)
    dossier.add_argument("--root", type=Path, required=True)
    inspect_cmd = commands.add_parser("inspect-dossier")
    inspect_cmd.add_argument("--path", type=Path, required=True)
    comparison = commands.add_parser("compare")
    comparison.add_argument("--baseline", type=Path, required=True)
    comparison.add_argument("--changed", type=Path, required=True)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        match args.command:
            case "run-wave":
                result = run_wave(args.integration, args.wave)
            case "fault-wave":
                result = run_wave(args.integration, args.wave, fault_batch=args.batch_id)
            case "state":
                result = state_compatibility(args.integration)
            case "target":
                result = target_identity(args.integration)
            case "inspect-state":
                result = inspect_state_compatibility(args.integration)
            case "inspect-recovery":
                result = inspect_recovery(args.integration)
            case "observe":
                result = inspect_observation(args.integration, args.stream)
                if args.output is not None:
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    raw = canonical_json(result)
                    if args.output.exists():
                        if args.output.read_bytes() != raw:
                            raise ValueError("Existing observation export differs")
                    else:
                        with args.output.open("xb") as output:
                            output.write(raw)
            case "inspect-retention":
                result = inspect_retention(args.integration, args.current_fence)
            case "record-broker-failure":
                result = record_failed_publication(
                    args.integration, args.stream, args.batch_id, args.log, args.exit_code
                )
            case "record-backlog":
                result = record_backlog(args.integration, args.stream, args.label)
            case "record-backpressure":
                result = record_backpressure(args.integration, args.stream, args.batch_id)
            case "verify-outage":
                result = verify_outage(args.integration, args.stream)
                args.output.parent.mkdir(parents=True, exist_ok=True)
                with args.output.open("xb") as output:
                    output.write(canonical_json(result))
            case "build-dossier":
                result = build(
                    {label.upper(): getattr(args, f"{label}_integration") for label in "abcd"},
                    {label.upper(): getattr(args, f"{label}_stream") for label in "abcd"},
                    args.outage_evidence,
                    args.candidate,
                    args.current_transaction_fence,
                    args.current_ready_offset_log,
                    args.root,
                )
            case "inspect-dossier":
                result = inspect_dossier(args.path)
            case "compare":
                result = compare(read_object(args.baseline), read_object(args.changed))
            case _:
                raise ValueError("Unsupported reliability operation")
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Reliability operation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
