"""Explicit local commands for committed-feature discovery and research handoff."""

import argparse
from collections.abc import Sequence
from pathlib import Path

from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

from .contract import MAX_READY_EVENTS
from .handoff import LocalResearchReceiver
from .outbox import build_outbox, build_segments, inspect_outbox, inspect_segment


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    discover = sub.add_parser("discover-feature-commits")
    discover.add_argument("--source-delta", required=True, type=Path)
    discover.add_argument("--start-version", required=True, type=int)
    discover.add_argument("--end-version", required=True, type=int)
    discover.add_argument("--segment-root", required=True, type=Path)
    inspect_segment_cmd = sub.add_parser("inspect-feature-commit-segment")
    inspect_segment_cmd.add_argument("--segment", required=True, type=Path)
    build = sub.add_parser("build-feature-ready-outbox")
    build.add_argument("--source-delta", required=True, type=Path)
    build.add_argument("--start-version", required=True, type=int)
    build.add_argument("--end-version", required=True, type=int)
    build.add_argument("--segment-root", required=True, type=Path)
    build.add_argument("--outbox-root", required=True, type=Path)
    inspect = sub.add_parser("inspect-feature-ready-outbox")
    inspect.add_argument("--outbox", required=True, type=Path)
    for name in ("handoff-ready-events", "inspect-feature-handoff", "verify-feature-handoff"):
        command = sub.add_parser(name)
        command.add_argument("--outbox", required=True, type=Path)
        command.add_argument("--receiver-root", required=True, type=Path)
        if name == "handoff-ready-events":
            command.add_argument("--maximum", required=True, type=int)
            command.add_argument("--replay", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "discover-feature-commits":
        paths = build_segments(
            args.source_delta, args.start_version, args.end_version, args.segment_root
        )
        result: object = [inspect_segment(path)[0] for path in paths]
    elif args.command == "inspect-feature-commit-segment":
        result = inspect_segment(args.segment)[0]
    elif args.command == "build-feature-ready-outbox":
        paths = build_segments(
            args.source_delta, args.start_version, args.end_version, args.segment_root
        )
        output = build_outbox(paths, args.outbox_root)
        result = inspect_outbox(output)[0]
    elif args.command == "inspect-feature-ready-outbox":
        result = inspect_outbox(args.outbox)[0]
    else:
        if args.command != "handoff-ready-events" and not args.receiver_root.exists():
            raise ValueError("Receiver root does not exist; inspection never creates receipts")
        receiver = LocalResearchReceiver(args.outbox, args.receiver_root)
        if args.command == "handoff-ready-events":
            if not 1 <= args.maximum <= MAX_READY_EVENTS:
                raise ValueError(f"maximum must be in [1,{MAX_READY_EVENTS}]")
            result = receiver.handoff(args.maximum, replay=args.replay)
        elif args.command == "verify-feature-handoff":
            result = receiver.verify_complete()
        else:
            result = receiver.inspect()
    print(canonical_json(result).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
