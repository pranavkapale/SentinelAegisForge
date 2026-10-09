"""Local SQLite-backed MLflow tracking over already verified training runs."""

import argparse
import json
import sys
from pathlib import Path

from mlflow.exceptions import MlflowException

from sentinelaegisforge_control_plane.datasets.errors import DatasetContractError
from sentinelaegisforge_control_plane.synthetic.corpus import SyntheticCorpusError
from sentinelaegisforge_control_plane.training.errors import TrainingContractError

from .run import (
    DEFAULT_EXPERIMENT,
    REPOSITORY_ROOT,
    CorpusLineage,
    TrackingConfig,
    TrackingError,
    compare_tracked_runs,
    inspect_tracked_run,
    track_run,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/tracking.db"
    )
    parser.add_argument(
        "--artifacts", type=Path, default=REPOSITORY_ROOT / ".local/mlflow/artifacts"
    )
    parser.add_argument("--experiment", default=DEFAULT_EXPERIMENT)
    commands = parser.add_subparsers(dest="command", required=True)
    track = commands.add_parser("track", help="Verify and index an existing immutable Phase 13 run")
    track.add_argument("--run", type=Path, required=True)
    track.add_argument("--corpus", type=Path)
    track.add_argument("--validated", type=Path)
    track.add_argument("--deduplicated", type=Path)
    track.add_argument("--rolling", type=Path)
    track.add_argument("--statistical", type=Path)
    inspect = commands.add_parser("inspect", help="Read an MLflow run without mutation")
    inspect.add_argument("--run-id", required=True)
    commands.add_parser("compare", help="Compare experiment records without selecting a winner")
    args = parser.parse_args(argv)
    try:
        config = TrackingConfig(args.database, args.artifacts, args.experiment)
        if args.command == "track":
            paths = [args.corpus, args.validated, args.deduplicated, args.rolling, args.statistical]
            if any(path is not None for path in paths) and not all(
                path is not None for path in paths
            ):
                raise TrackingError("Provide all five explicit corpus lineage paths or none")
            lineage = CorpusLineage(*paths) if all(path is not None for path in paths) else None
            result: dict[str, object] = dict(track_run(args.run, config, lineage))
        elif args.command == "inspect":
            result = inspect_tracked_run(config, args.run_id)
        else:
            result = compare_tracked_runs(config)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        DatasetContractError,
        TrainingContractError,
        SyntheticCorpusError,
        TrackingError,
        MlflowException,
        KeyError,
        OSError,
        ValueError,
    ) as error:
        print(f"Tracking failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
