"""Explicit local research-only inbox inspection, bounded inference and result inspection."""

import argparse
import json
import sys
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp
from sentinelaegisforge_control_plane.registry.run import RegistryConfig
from sentinelaegisforge_control_plane.tracking.run import (
    DEFAULT_EXPERIMENT,
    REPOSITORY_ROOT,
    TrackingConfig,
)

from .inbox import inspect_inbox
from .results import counts, durable_results
from .worker import WorkerConfig, execute, inspect_run_artifact


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    inbox = commands.add_parser("inspect-inbox")
    inbox.add_argument("--inbox", type=Path, required=True)
    run = commands.add_parser("execute")
    run.add_argument("--inbox", type=Path, required=True)
    run.add_argument("--source-delta", type=Path, required=True)
    run.add_argument("--model-version", required=True)
    run.add_argument("--score-start", required=True)
    run.add_argument("--score-end", required=True)
    run.add_argument("--reference-ledger", type=Path, required=True)
    run.add_argument("--result-root", type=Path, required=True)
    run.add_argument("--run-root", type=Path, required=True)
    run.add_argument("--maximum-events", type=int, default=5000)
    run.add_argument("--maximum-cached-rows", type=int, default=10000)
    run.add_argument("--maximum-attempts", type=int, default=5000)
    run.add_argument("--inference-batch-size", type=int, default=100)
    run.add_argument("--fail-after-result", type=int, default=-1)
    run.add_argument("--database", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/tracking.db")
    run.add_argument("--artifacts", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/artifacts")
    run.add_argument("--experiment", default=DEFAULT_EXPERIMENT)
    run.add_argument("--model-name", default="sentinelaegisforge-offline-fraud-logistic")
    run.add_argument(
        "--receipts",
        type=Path,
        default=REPOSITORY_ROOT / ".local/mlflow/registration-receipts",
    )
    results = commands.add_parser("inspect-results")
    results.add_argument("--inbox", type=Path, required=True)
    results.add_argument("--candidate-id", required=True)
    results.add_argument("--model-tree-sha256", required=True)
    results.add_argument("--result-root", type=Path, required=True)
    evidence = commands.add_parser("inspect-run")
    evidence.add_argument("--run", type=Path, required=True)
    return root


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        result: object
        if arguments.command == "inspect-inbox":
            result = inspect_inbox(arguments.inbox)[0]
        elif arguments.command == "inspect-run":
            result = inspect_run_artifact(arguments.run)
        elif arguments.command == "inspect-results":
            from .contract import request_id

            inbox, events = inspect_inbox(arguments.inbox)
            identities = [
                request_id(
                    cast(str, event["ready_event_id"]),
                    arguments.candidate_id,
                    arguments.model_tree_sha256,
                )
                for event in events
            ]
            complete = durable_results(
                arguments.result_root / cast(str, inbox["inbox_id"]),
                arguments.candidate_id,
                identities,
            )
            result = counts(complete, len(events))
        else:
            registry = RegistryConfig(
                TrackingConfig(arguments.database, arguments.artifacts, arguments.experiment),
                arguments.model_name,
                arguments.receipts,
            )
            config = WorkerConfig(
                arguments.inbox,
                arguments.source_delta,
                registry,
                arguments.model_version,
                parse_utc_timestamp(arguments.score_start),
                parse_utc_timestamp(arguments.score_end),
                arguments.result_root,
                arguments.run_root,
                arguments.reference_ledger,
                arguments.maximum_events,
                arguments.maximum_cached_rows,
                arguments.maximum_attempts,
                arguments.inference_batch_size,
            )
            result = execute(config, fail_after_result=arguments.fail_after_result)
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(f"Kafka shadow research failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
