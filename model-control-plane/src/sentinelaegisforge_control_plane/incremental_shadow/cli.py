"""Explicit bounded Phase 27 operations; no always-on daemon or model promotion."""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import cast
from uuid import uuid4

from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.registry.run import (
    RegistryConfig,
    inspect_registered_candidate,
)

from .fixture import append_wave, initialize_source
from .operations import GROUP, Limits, discover_next, initialize
from .progress import authorize_publication, progress, reconcile
from .worker import process, verify_reference


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    empty = commands.add_parser("fixture-init")
    empty.add_argument("--source", type=Path, required=True)
    wave = commands.add_parser("fixture-append")
    wave.add_argument("--source", type=Path, required=True)
    wave.add_argument("--seed", type=int, required=True)
    wave.add_argument("--first-index", type=int, required=True)
    wave.add_argument("--count", type=int, required=True)
    wave.add_argument("--base-time", required=True)
    wave.add_argument("--observation-root", type=Path)
    init = commands.add_parser("initialize")
    init.add_argument("--source", type=Path, required=True)
    init.add_argument("--root", type=Path, required=True)
    init.add_argument("--fence", type=Path, required=True)
    init.add_argument("--first-version", type=int, required=True)
    init.add_argument("--model-version", default="1")
    init.add_argument("--group", default=GROUP)
    init.add_argument("--maximum-undiscovered-versions", type=int, default=8)
    init.add_argument("--maximum-unpublished-ready-events", type=int, default=500)
    init.add_argument("--maximum-accepted-unprocessed-events", type=int, default=200)
    init.add_argument("--maximum-inference-events-per-tick", type=int, default=50)
    init.add_argument("--maximum-cached-source-rows", type=int, default=5000)
    for name in (
        "discover",
        "inspect",
        "authorize-publish",
        "process",
        "verify-reference",
        "reconcile",
        "report",
    ):
        command = commands.add_parser(name)
        command.add_argument("--stream", type=Path, required=True)
        if name == "authorize-publish":
            command.add_argument("--batch-id", required=True)
        if name == "process":
            command.add_argument("--model-version", default="1")
            command.add_argument("--score-start", required=True)
            command.add_argument("--score-end", required=True)
            command.add_argument("--maximum", type=int, required=True)
            command.add_argument("--fail-after-result", type=int, default=-1)
        if name == "verify-reference":
            command.add_argument("--candidate", type=Path, required=True)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result: object
        if args.command == "fixture-init":
            result = initialize_source(args.source)
        elif args.command == "fixture-append":
            result = append_wave(
                args.source,
                args.seed,
                args.first_index,
                args.count,
                parse_utc_timestamp(args.base_time),
            )
            if args.observation_root is not None:
                _observe(
                    args.observation_root,
                    "delta_write",
                    cast(float, result["delta_write_seconds"]),
                    cast(int, result["committed_row_count"]),
                )
        elif args.command == "initialize":
            candidate = inspect_registered_candidate(RegistryConfig(), args.model_version)
            result = initialize(
                args.source,
                args.root,
                args.fence,
                args.first_version,
                str(candidate["candidate_id"]),
                str(candidate["model_tree_sha256"]),
                Limits(
                    maximum_undiscovered_versions=args.maximum_undiscovered_versions,
                    maximum_unpublished_ready_events=args.maximum_unpublished_ready_events,
                    maximum_accepted_unprocessed_events=args.maximum_accepted_unprocessed_events,
                    maximum_inference_events_per_tick=args.maximum_inference_events_per_tick,
                    maximum_cached_source_rows=args.maximum_cached_source_rows,
                ),
                args.group,
            )
        elif args.command == "discover":
            started = time.perf_counter()
            result = discover_next(args.stream)
            if result is not None:
                _observe(
                    args.stream / "observations",
                    "commit_discovery_and_outbox",
                    time.perf_counter() - started,
                    cast(int, result["ready_event_count"]),
                )
        elif args.command == "inspect":
            result = progress(args.stream)
        elif args.command == "reconcile":
            result = reconcile(args.stream)
        elif args.command == "report":
            from .telemetry import build_report

            result = build_report(args.stream)
        elif args.command == "authorize-publish":
            result = authorize_publication(args.stream, args.batch_id)
        elif args.command == "process":
            result = process(
                args.stream,
                RegistryConfig(),
                args.model_version,
                parse_utc_timestamp(args.score_start),
                parse_utc_timestamp(args.score_end),
                maximum=args.maximum,
                fail_after_result=args.fail_after_result,
            )
        else:
            result = verify_reference(args.stream, args.candidate, RegistryConfig())
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(f"Incremental shadow operation failed: {error}", file=sys.stderr)
        return 1


def _observe(root: Path, stage: str, elapsed_seconds: float, count: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{uuid4()}.json").write_bytes(
        canonical_json(
            {
                "observation_contract_version": "incremental-shadow-timing-v1",
                "stage": stage,
                "elapsed_seconds": elapsed_seconds,
                "sample_count": count,
            }
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
