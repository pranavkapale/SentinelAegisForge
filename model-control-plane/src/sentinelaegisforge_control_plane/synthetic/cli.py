"""Inspect independent synthetic truth and reconcile the real Delta feature path."""

import argparse
import json
import sys
from pathlib import Path

from sentinelaegisforge_control_plane.datasets.errors import DatasetContractError
from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp

from .corpus import SyntheticCorpusError, inspect_corpus
from .reconcile import reconcile, temporal_quality


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--corpus", type=Path, required=True)
    quality = commands.add_parser("quality")
    quality.add_argument("--corpus", type=Path, required=True)
    for name in ("train-end", "validation-end", "test-end", "as-of"):
        quality.add_argument(f"--{name}", required=True)
    verify = commands.add_parser("reconcile")
    verify.add_argument("--corpus", type=Path, required=True)
    for name in ("validated", "deduplicated", "rolling", "statistical"):
        verify.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        corpus = inspect_corpus(args.corpus)
        if args.command == "inspect":
            result: dict[str, object] = dict(corpus.manifest)
        elif args.command == "quality":
            result = temporal_quality(
                corpus,
                parse_utc_timestamp(args.train_end),
                parse_utc_timestamp(args.validation_end),
                parse_utc_timestamp(args.test_end),
                parse_utc_timestamp(args.as_of),
            )
        else:
            result = reconcile(
                corpus, args.validated, args.deduplicated, args.rolling, args.statistical
            )
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (DatasetContractError, SyntheticCorpusError, OSError) as error:
        print(f"Synthetic corpus verification failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
