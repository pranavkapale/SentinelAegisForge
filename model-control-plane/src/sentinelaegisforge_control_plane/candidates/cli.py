"""Build, inspect and score trusted local-only Phase 16 candidates."""

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import cast

from mlflow.exceptions import MlflowException

from sentinelaegisforge_control_plane.datasets.errors import DatasetContractError
from sentinelaegisforge_control_plane.tracking.run import (
    DEFAULT_EXPERIMENT,
    REPOSITORY_ROOT,
    TrackingConfig,
    TrackingError,
)
from sentinelaegisforge_control_plane.training.errors import TrainingContractError

from .contract import CandidateError
from .run import DEFAULT_OUTPUT_ROOT, build_candidate, inspect_candidate, score_candidate


def _input(path: Path) -> dict[str, object]:
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise CandidateError("Offline scoring input must be one JSON object")
    row = cast(dict[str, object], raw)
    for name in ("amount", "prior_amount_sum_10m"):
        value = row.get(name)
        if not isinstance(value, str):
            raise CandidateError(f"{name} must be a decimal string in JSON")
        try:
            row[name] = Decimal(value)
        except (ValueError, ArithmeticError) as error:
            raise CandidateError(f"{name} is not a decimal string") from error
    return row


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build", help="Reconstruct and publish a verified candidate")
    build.add_argument("--run", type=Path, required=True)
    build.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    build.add_argument("--mlflow-run-id")
    build.add_argument(
        "--database", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/tracking.db"
    )
    build.add_argument(
        "--artifacts", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/artifacts"
    )
    build.add_argument("--experiment", default=DEFAULT_EXPERIMENT)
    inspect = commands.add_parser("inspect", help="Verify candidate source and model bytes")
    inspect.add_argument("--candidate", type=Path, required=True)
    inspect.add_argument(
        "--database", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/tracking.db"
    )
    inspect.add_argument(
        "--artifacts", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/artifacts"
    )
    inspect.add_argument("--experiment", default=DEFAULT_EXPERIMENT)
    score = commands.add_parser("score", help="Score one strict JSON input offline")
    score.add_argument("--candidate", type=Path, required=True)
    score.add_argument("--input-json", type=Path, required=True)
    score.add_argument(
        "--database", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/tracking.db"
    )
    score.add_argument(
        "--artifacts", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/artifacts"
    )
    score.add_argument("--experiment", default=DEFAULT_EXPERIMENT)
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            tracking = TrackingConfig(args.database, args.artifacts, args.experiment)
            path = build_candidate(
                args.run,
                args.output_root,
                tracking_run_id=args.mlflow_run_id,
                tracking=tracking,
            )
            result: dict[str, object] = {"candidate_path": str(path), **inspect_candidate(path)}
        elif args.command == "inspect":
            result = inspect_candidate(
                args.candidate,
                tracking=TrackingConfig(args.database, args.artifacts, args.experiment),
            )
        else:
            probability = score_candidate(
                args.candidate,
                [_input(args.input_json)],
                tracking=TrackingConfig(args.database, args.artifacts, args.experiment),
            )[0]
            result = {
                "fraud_probability_estimate": probability,
                "positive_class": "FRAUD",
                "warning": (
                    "Synthetic-only, uncalibrated offline estimate; not an enforcement decision"
                ),
            }
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (
        CandidateError,
        DatasetContractError,
        TrainingContractError,
        TrackingError,
        MlflowException,
        OSError,
        ValueError,
    ) as error:
        print(f"Candidate failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
