"""Local, explicit research replay commands; no network serving or enforcement API."""

import argparse
import json
from pathlib import Path

from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp
from sentinelaegisforge_control_plane.registry.run import RegistryConfig
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig

from .execution import DEFAULT_EXECUTION_ROOT, execute_plan, inspect_execution
from .parity import DEFAULT_PARITY_ROOT, verify_parity
from .plan import DEFAULT_PLAN_ROOT, build_plan, inspect_plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded non-enforcing research shadow replay")
    parser.add_argument("--database", type=Path, default=TrackingConfig().database)
    parser.add_argument("--artifacts", type=Path, default=TrackingConfig().artifacts)
    parser.add_argument("--experiment", default=TrackingConfig().experiment_name)
    parser.add_argument("--model-name", default=RegistryConfig().model_name)
    parser.add_argument("--receipts", type=Path, default=RegistryConfig().receipts)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build-shadow-replay-plan")
    build.add_argument("--model-version", required=True)
    build.add_argument("--source-delta", type=Path, required=True)
    build.add_argument("--source-version", type=int, required=True)
    build.add_argument("--start", required=True)
    build.add_argument("--end", required=True)
    build.add_argument("--maximum", type=int, default=5000)
    build.add_argument("--output-root", type=Path, default=DEFAULT_PLAN_ROOT)
    inspect_p = sub.add_parser("inspect-shadow-replay-plan")
    inspect_p.add_argument("--plan", type=Path, required=True)
    execute = sub.add_parser("execute-shadow-replay")
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--maximum", type=int, default=5000)
    execute.add_argument("--output-root", type=Path, default=DEFAULT_EXECUTION_ROOT)
    inspect_e = sub.add_parser("inspect-shadow-execution")
    inspect_e.add_argument("--plan", type=Path, required=True)
    inspect_e.add_argument("--execution", type=Path, required=True)
    parity = sub.add_parser("verify-shadow-parity")
    parity.add_argument("--plan", type=Path, required=True)
    parity.add_argument("--execution", type=Path, required=True)
    parity.add_argument("--reference-ledger", type=Path, required=True)
    parity.add_argument("--output-root", type=Path, default=DEFAULT_PARITY_ROOT)
    args = parser.parse_args()
    registry = RegistryConfig(
        TrackingConfig(args.database.resolve(), args.artifacts.resolve(), args.experiment),
        args.model_name,
        args.receipts.resolve(),
    )
    if args.command == "build-shadow-replay-plan":
        output = build_plan(
            registry,
            args.model_version,
            args.source_delta,
            args.source_version,
            parse_utc_timestamp(args.start),
            parse_utc_timestamp(args.end),
            args.output_root,
            maximum=args.maximum,
        )
        print(output)
    elif args.command == "inspect-shadow-replay-plan":
        print(json.dumps(inspect_plan(args.plan, registry)[0], indent=2, sort_keys=True))
    elif args.command == "execute-shadow-replay":
        print(execute_plan(args.plan, registry, args.output_root, maximum=args.maximum))
    elif args.command == "inspect-shadow-execution":
        print(
            json.dumps(
                inspect_execution(args.execution, args.plan, registry), indent=2, sort_keys=True
            )
        )
    elif args.command == "verify-shadow-parity":
        print(
            verify_parity(
                args.execution, args.plan, args.reference_ledger, registry, args.output_root
            )
        )


if __name__ == "__main__":
    main()
