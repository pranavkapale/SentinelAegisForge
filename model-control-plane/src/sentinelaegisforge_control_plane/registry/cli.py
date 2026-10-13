"""Register or explicitly inspect research-only MLflow model versions."""

import argparse
import json
import sys
from pathlib import Path

from mlflow.exceptions import MlflowException

from sentinelaegisforge_control_plane.candidates.contract import CandidateError
from sentinelaegisforge_control_plane.datasets.errors import DatasetContractError
from sentinelaegisforge_control_plane.tracking.run import (
    DEFAULT_EXPERIMENT,
    REPOSITORY_ROOT,
    TrackingConfig,
    TrackingError,
)
from sentinelaegisforge_control_plane.training.errors import TrainingContractError

from .run import (
    DEFAULT_MODEL_NAME,
    DEFAULT_RECEIPT_ROOT,
    RegistryConfig,
    RegistryError,
    compare_registered_candidates,
    inspect_registered_candidate,
    register_candidate,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser(
        "register", help="Mutate Registry after full candidate validation"
    )
    register.add_argument("--candidate", type=Path, required=True)
    inspect = commands.add_parser("inspect", help="Read-only metadata/source-hash inspection")
    inspect.add_argument("--version", required=True)
    compare = commands.add_parser("compare", help="Read-only comparison; no selection")
    compare.add_argument("--versions", nargs="+", required=True)
    verify = commands.add_parser("verify-artifact", help="Trusted loading and offline equivalence")
    verify.add_argument("--version", required=True)
    for command in (register, inspect, compare, verify):
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
        if args.command == "register":
            result = register_candidate(args.candidate, config)
        elif args.command == "inspect":
            result = inspect_registered_candidate(config, args.version)
        elif args.command == "compare":
            result = compare_registered_candidates(config, args.versions)
        else:
            result = inspect_registered_candidate(config, args.version, deep=True)
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (
        RegistryError,
        CandidateError,
        DatasetContractError,
        TrainingContractError,
        TrackingError,
        MlflowException,
        OSError,
        ValueError,
    ) as error:
        print(f"Registry failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
