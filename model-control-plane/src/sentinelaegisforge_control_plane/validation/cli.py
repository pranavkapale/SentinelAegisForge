"""Create or read immutable model-validation dossiers for exact Registry versions."""

import argparse
import json
import sys
from pathlib import Path

from mlflow.exceptions import MlflowException

from sentinelaegisforge_control_plane.candidates.contract import CandidateError
from sentinelaegisforge_control_plane.datasets.errors import DatasetContractError
from sentinelaegisforge_control_plane.registry.run import (
    DEFAULT_MODEL_NAME,
    DEFAULT_RECEIPT_ROOT,
    RegistryConfig,
    RegistryError,
)
from sentinelaegisforge_control_plane.tracking.run import (
    DEFAULT_EXPERIMENT,
    REPOSITORY_ROOT,
    TrackingConfig,
    TrackingError,
)
from sentinelaegisforge_control_plane.training.errors import TrainingContractError

from .run import (
    DEFAULT_OUTPUT_ROOT,
    AssessmentError,
    assess_model,
    compare_assessments,
    inspect_assessment,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    assess = commands.add_parser("assess", help="Publish a new immutable, research-only dossier")
    assess.add_argument("--version", required=True, help="Exact positive numeric Registry version")
    assess.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    inspect = commands.add_parser("inspect", help="Read-only dossier and upstream inspection")
    inspect.add_argument("--assessment", type=Path, required=True)
    compare = commands.add_parser("compare", help="Read-only comparison without selection")
    compare.add_argument("--assessments", type=Path, nargs="+", required=True)
    for command in (assess, inspect, compare):
        command.add_argument(
            "--database", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/tracking.db"
        )
        command.add_argument(
            "--artifacts", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/artifacts"
        )
        command.add_argument("--experiment", default=DEFAULT_EXPERIMENT)
        command.add_argument("--model-name", default=DEFAULT_MODEL_NAME)
        command.add_argument("--receipts", type=Path, default=DEFAULT_RECEIPT_ROOT)
    args = parser.parse_args(argv)
    try:
        config = RegistryConfig(
            TrackingConfig(args.database, args.artifacts, args.experiment),
            args.model_name,
            args.receipts,
        )
        if args.command == "assess":
            path = assess_model(config, args.version, args.output_root)
            result = inspect_assessment(path, config)
            result = {"path": str(path), **result}
        elif args.command == "inspect":
            result = inspect_assessment(args.assessment, config)
        else:
            result = compare_assessments(args.assessments, config)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        AssessmentError,
        RegistryError,
        CandidateError,
        DatasetContractError,
        TrainingContractError,
        TrackingError,
        MlflowException,
        OSError,
        ValueError,
    ) as error:
        print(f"Model assessment failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
