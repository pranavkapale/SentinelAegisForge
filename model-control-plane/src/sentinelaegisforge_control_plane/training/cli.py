"""Local baseline training over one verified immutable Phase 12 snapshot."""

import argparse
import json
import sys
from pathlib import Path

from sentinelaegisforge_control_plane.datasets.errors import DatasetContractError
from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp

from .errors import TrainingContractError
from .run import inspect_run, train_baseline
from .temporal import TemporalConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    train = commands.add_parser("train", help="Run the fixed temporal logistic baseline")
    train.add_argument("--dataset-snapshot", type=Path, required=True)
    train.add_argument("--train-end", required=True)
    train.add_argument("--validation-end", required=True)
    train.add_argument("--test-end", required=True)
    train.add_argument("--min-train-rows", type=int, default=20)
    train.add_argument("--min-validation-rows", type=int, default=10)
    train.add_argument("--min-test-rows", type=int, default=10)
    train.add_argument("--output-root", type=Path, required=True)
    inspect = commands.add_parser("inspect", help="Verify immutable run artifacts")
    inspect.add_argument("--run", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "train":
            config = TemporalConfig(
                parse_utc_timestamp(args.train_end),
                parse_utc_timestamp(args.validation_end),
                parse_utc_timestamp(args.test_end),
                args.min_train_rows,
                args.min_validation_rows,
                args.min_test_rows,
            )
            run = train_baseline(args.dataset_snapshot, config, args.output_root)
            print(f"Training-run directory: {run.path}")
            manifest = run.manifest
        else:
            manifest = inspect_run(args.run)
        print(json.dumps(manifest, sort_keys=True, indent=2))
        return 0
    except (DatasetContractError, TrainingContractError, OSError) as error:
        print(f"Training contract failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
