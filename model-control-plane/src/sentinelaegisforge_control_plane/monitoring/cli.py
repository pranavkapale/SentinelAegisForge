"""Small CLI for offline replay and point-in-time outcome inspection."""

import argparse
import json
from pathlib import Path

from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp
from sentinelaegisforge_control_plane.registry.run import RegistryConfig
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig

from .ledger import DEFAULT_LEDGER_ROOT, build_ledger, inspect_ledger
from .outcomes import (
    DEFAULT_MONITORING_ROOT,
    compare_snapshots,
    evaluate_outcomes,
    inspect_monitoring_snapshot,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline synthetic prediction monitoring")
    commands = parser.add_subparsers(dest="command", required=True)
    score = commands.add_parser("build-prediction-ledger")
    score.add_argument("--database", type=Path, required=True)
    score.add_argument("--artifacts", type=Path, required=True)
    score.add_argument("--experiment", required=True)
    score.add_argument("--model-name", required=True)
    score.add_argument("--receipts", type=Path, required=True)
    score.add_argument("--model-version", required=True)
    score.add_argument("--features-delta", type=Path, required=True)
    score.add_argument("--source-version", type=int, required=True)
    score.add_argument("--score-start", required=True)
    score.add_argument("--score-end", required=True)
    score.add_argument("--output-root", type=Path, default=DEFAULT_LEDGER_ROOT)
    ledger = commands.add_parser("inspect-prediction-ledger")
    ledger.add_argument("--ledger", type=Path, required=True)
    evaluate = commands.add_parser("evaluate-delayed-outcomes")
    evaluate.add_argument("--ledger", type=Path, required=True)
    evaluate.add_argument("--corpus", type=Path, required=True)
    evaluate.add_argument("--evaluation-as-of", required=True)
    evaluate.add_argument("--minimum-rows", type=int, default=20)
    evaluate.add_argument("--minimum-per-class", type=int, default=2)
    evaluate.add_argument("--output-root", type=Path, default=DEFAULT_MONITORING_ROOT)
    inspect = commands.add_parser("inspect-monitoring-snapshot")
    inspect.add_argument("--snapshot", type=Path, required=True)
    compare = commands.add_parser("compare-monitoring-snapshots")
    compare.add_argument("--first", type=Path, required=True)
    compare.add_argument("--second", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.command == "build-prediction-ledger":
        tracking = TrackingConfig(
            arguments.database.resolve(), arguments.artifacts.resolve(), arguments.experiment
        )
        registry = RegistryConfig(tracking, arguments.model_name, arguments.receipts.resolve())
        result = build_ledger(
            registry,
            arguments.model_version,
            arguments.features_delta,
            arguments.source_version,
            parse_utc_timestamp(arguments.score_start),
            parse_utc_timestamp(arguments.score_end),
            arguments.output_root,
        )
        print(json.dumps({"path": str(result), "manifest": inspect_ledger(result)}, indent=2))
    elif arguments.command == "inspect-prediction-ledger":
        print(json.dumps(inspect_ledger(arguments.ledger), indent=2))
    elif arguments.command == "evaluate-delayed-outcomes":
        result = evaluate_outcomes(
            arguments.ledger,
            arguments.corpus,
            parse_utc_timestamp(arguments.evaluation_as_of),
            arguments.output_root,
            min_rows=arguments.minimum_rows,
            min_per_class=arguments.minimum_per_class,
        )
        print(
            json.dumps(
                {"path": str(result), "manifest": inspect_monitoring_snapshot(result)}, indent=2
            )
        )
    elif arguments.command == "inspect-monitoring-snapshot":
        print((arguments.snapshot / "report.txt").read_text())
        print(json.dumps(inspect_monitoring_snapshot(arguments.snapshot), indent=2))
    else:
        print(json.dumps(compare_snapshots(arguments.first, arguments.second), indent=2))


if __name__ == "__main__":
    main()
