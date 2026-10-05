"""Local CLI over already materialized v2 features. Does not start infrastructure."""

import argparse
import json
import sys
from pathlib import Path

from .dataset_builder import build_dataset, inspect_snapshot
from .errors import DatasetContractError
from .labels import parse_utc_timestamp


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    build = subcommands.add_parser("build", help="Create or verify an immutable labeled snapshot")
    build.add_argument("--features-delta", type=Path, required=True)
    build.add_argument("--source-version", type=int)
    build.add_argument("--labels", type=Path, required=True)
    build.add_argument("--as-of", required=True)
    build.add_argument("--output-root", type=Path, required=True)
    inspect = subcommands.add_parser(
        "inspect", help="Verify artifact integrity and report metadata"
    )
    inspect.add_argument("--snapshot", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            snapshot = build_dataset(
                args.features_delta,
                args.labels,
                parse_utc_timestamp(args.as_of),
                args.output_root,
                args.source_version,
            )
            print(f"Snapshot directory: {snapshot.path}")
            manifest = snapshot.manifest
        else:
            manifest = inspect_snapshot(args.snapshot)
        print(json.dumps(manifest, sort_keys=True, indent=2))
        return 0
    except (DatasetContractError, OSError) as error:
        print(f"Dataset contract failure: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
