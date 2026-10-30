"""Explicit Phase 31 capture, inspect, copy, restore and isolated-rehearsal operations."""

import argparse
import json
import sys
from pathlib import Path
from typing import cast

from . import bundle, docker_restore, environment, runtime
from .contract import REHEARSAL_VERSION, RecoveryError, digest, read, sha, write
from .lineage import Selection, dependencies


def finish(output: Path) -> dict[str, object]:
    env = read(output / "restore-environment.json")
    source = Path(str(env["bundle"]))
    manifest = bundle.verify(source)
    cutoff = read(source / "source_cutoff.json")
    environment.assert_source_stopped(
        str(cutoff["source_compose_project"]), str(cutoff["source_volume"])
    )
    resumed = read(output / "resumed.json")
    before = read(output / "broker-pre-resume.json")
    docker_restore.check_broker_identity(
        cast(dict[str, object], cutoff["broker"]),
        cast(dict[str, object], before["broker"]),
        cast(list[dict[str, object]], before["internal_startup_noops"]),
    )
    after = environment.kafka_snapshot(
        str(env["project"]), str(cast(dict[str, object], cutoff["broker"])["group_id"])
    )
    pending = cast(dict[str, object], resumed["after"])
    if (
        resumed["bundle_id"] != manifest["bundle_id"]
        or resumed["simulation_status"] != "VERIFIED_RESTORED"
        or any(
            pending[key] != 0
            for key in (
                "discovery_backlog_versions",
                "transport_backlog_ready_events",
                "inference_backlog_ready_events",
            )
        )
    ):
        raise RecoveryError("Incomplete resumed logical work")
    names = [
        "restore-environment.json",
        "inspect-restored-execution.json",
        "inspect-restored-container.json",
        "broker-pre-resume.json",
        "resume-execution.json",
        "resume-container.json",
        "resumed.json",
    ]
    evidence = {name: sha(output / name) for name in names}
    value = {
        "contract_version": REHEARSAL_VERSION,
        "bundle_id": manifest["bundle_id"],
        "source_lineage_id": cutoff["lineage_id"],
        "artifact_sha256": evidence,
        "docker_simulation_status": "VERIFIED_RESTORED",
        "status": "PARTIALLY_VERIFIED",
        "independent_storage": "NOT_EXECUTED_SIMULATED_SAME_HOST_DISK",
        "source_environment": "PRESERVED_STOPPED",
        "broker_after": after,
        "limitations": [
            "Local same-disk backup is not protection against physical disk loss",
            "No production RPO/RTO, HA or globally exactly-once claim",
        ],
    }
    value["rehearsal_id"] = digest(value)
    write(output / "rehearsal_manifest.json", value)
    return value


def inspect_rehearsal(output: Path) -> dict[str, object]:
    result = read(output / "rehearsal_manifest.json")
    if result.get("contract_version") != REHEARSAL_VERSION:
        raise RecoveryError("Unsupported recovery rehearsal")
    basis = {k: v for k, v in result.items() if k != "rehearsal_id"}
    if digest(basis) != result["rehearsal_id"]:
        raise RecoveryError("Rehearsal identity differs")
    for name, value in cast(dict[str, str], result["artifact_sha256"]).items():
        if Path(name).name != name or sha(output / name) != value:
            raise RecoveryError("Rehearsal evidence differs")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=[
            "dependencies",
            "capture",
            "build",
            "verify",
            "copy",
            "restore-files",
            "stage-docker",
            "preflight-docker",
            "start-docker",
            "resume-docker",
            "finish",
            "inspect-restored",
            "resume",
            "inspect-rehearsal",
        ],
    )
    parser.add_argument("--selection", type=Path)
    parser.add_argument("--cutoff", type=Path)
    parser.add_argument("--kafka-archive", type=Path)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--restored", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--project", default="sentinel-recovery-phase31")
    args = parser.parse_args(argv)

    def path(name: str) -> Path:
        value = getattr(args, name)
        if value is None or not value.is_absolute():
            raise RecoveryError(f"Supply explicit absolute --{name.replace('_', '-')}")
        return cast(Path, value)

    try:
        result: object
        if args.command in {"dependencies", "capture"}:
            selected = Selection(**{k: Path(str(v)) for k, v in read(path("selection")).items()})
            result = (
                dependencies(selected)
                if args.command == "dependencies"
                else environment.capture(selected, path("output"))
            )
        elif args.command == "build":
            result = bundle.build(path("cutoff"), path("kafka_archive"), path("output"))
        elif args.command == "verify":
            result = bundle.verify(path("bundle"))
        elif args.command == "copy":
            result = bundle.copy_bundle(path("bundle"), path("output"))
        elif args.command == "restore-files":
            result = bundle.restore_files(path("bundle"), path("output"))
        elif args.command == "stage-docker":
            result = docker_restore.stage(
                path("bundle"), path("restored"), path("output"), args.project
            )
        elif args.command == "preflight-docker":
            result = docker_restore.run_runtime(path("output"), "inspect-restored", offline=True)
        elif args.command == "start-docker":
            result = docker_restore.start(path("output"))
        elif args.command == "resume-docker":
            if not (path("output") / "broker-pre-resume.json").is_file():
                raise RecoveryError("Broker identity preflight required")
            result = docker_restore.run_runtime(path("output"), "resume")
        elif args.command == "finish":
            result = finish(path("output"))
        elif args.command == "inspect-restored":
            result = runtime.inspect_restored(path("bundle"))
        elif args.command == "resume":
            result = runtime.resume(path("bundle"), path("output"))
        else:
            result = inspect_rehearsal(path("output"))
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Recovery refused: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
