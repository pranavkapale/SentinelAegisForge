"""Explicit local feedback ingestion, AS_OF resolution and offline evaluation."""

import argparse
import json
from pathlib import Path

from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp, utc_text

from .contract import parse_jsonl
from .fixture import DEFAULT_FIXTURE_ROOT, build_local_fixture, inspect_local_fixture
from .ingestion import DEFAULT_FEEDBACK_ROOT, build_feedback_ledger, inspect_feedback_ledger
from .monitoring import (
    DEFAULT_ROOT,
    evaluate_feedback_outcomes,
    inspect_feedback_monitoring,
)
from .resolution import resolve_feedback


def main() -> None:
    parser = argparse.ArgumentParser(description="Local synthetic provider-like outcome feedback")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build-feedback-ledger")
    build.add_argument("--batch", type=Path, required=True)
    build.add_argument("--source-delta", type=Path, required=True)
    build.add_argument("--source-version", type=int, required=True)
    build.add_argument("--base-ledger", type=Path)
    build.add_argument("--output-root", type=Path, default=DEFAULT_FEEDBACK_ROOT)
    inspect = commands.add_parser("inspect-feedback-ledger")
    inspect.add_argument("--ledger", type=Path, required=True)
    resolve = commands.add_parser("resolve-feedback-as-of")
    resolve.add_argument("--ledger", type=Path, required=True)
    resolve.add_argument("--as-of", required=True)
    evaluate = commands.add_parser("evaluate-feedback-outcomes")
    evaluate.add_argument("--prediction-ledger", type=Path, required=True)
    evaluate.add_argument("--feedback-ledger", type=Path, required=True)
    evaluate.add_argument("--evaluation-as-of", required=True)
    evaluate.add_argument("--minimum-rows", type=int, default=20)
    evaluate.add_argument("--minimum-per-class", type=int, default=2)
    evaluate.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    inspect_monitoring = commands.add_parser("inspect-feedback-monitoring")
    inspect_monitoring.add_argument("--snapshot", type=Path, required=True)
    fixture = commands.add_parser("generate-local-feedback-fixture")
    fixture.add_argument("--prediction-ledger", type=Path, required=True)
    fixture.add_argument("--output-root", type=Path, default=DEFAULT_FIXTURE_ROOT)
    inspect_fixture = commands.add_parser("inspect-local-feedback-fixture")
    inspect_fixture.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "build-feedback-ledger":
        result = build_feedback_ledger(
            args.batch,
            args.source_delta,
            args.source_version,
            args.output_root,
            base_ledger=args.base_ledger,
        )
        value: object = {"path": str(result), "manifest": inspect_feedback_ledger(result)}
    elif args.command == "inspect-feedback-ledger":
        value = inspect_feedback_ledger(args.ledger)
    elif args.command == "resolve-feedback-as-of":
        manifest = inspect_feedback_ledger(args.ledger)
        records = parse_jsonl((args.ledger / "feedback.jsonl").read_bytes())
        as_of = parse_utc_timestamp(args.as_of)
        resolved = resolve_feedback(
            records, as_of, tuple(sorted({str(record.event_id) for record in records}))
        )
        value = {
            "feedback_ledger_id": manifest["feedback_ledger_id"],
            "as_of": utc_text(as_of),
            "cases": {
                event_id: {
                    "state": item.state.value,
                    "selected": [
                        {
                            "provider_id": entry.record.provider_id,
                            "provider_case_id": entry.record.provider_case_id,
                            "provider_record_id": entry.record.provider_record_id,
                            "source_revision": entry.record.source_revision,
                            "effective_available_at": utc_text(entry.effective_available_at),
                        }
                        for entry in item.selected
                    ],
                }
                for event_id, item in resolved.items()
            },
        }
    elif args.command == "evaluate-feedback-outcomes":
        result = evaluate_feedback_outcomes(
            args.prediction_ledger,
            args.feedback_ledger,
            parse_utc_timestamp(args.evaluation_as_of),
            args.output_root,
            min_rows=args.minimum_rows,
            min_per_class=args.minimum_per_class,
        )
        value = {"path": str(result), "manifest": inspect_feedback_monitoring(result)}
    elif args.command == "generate-local-feedback-fixture":
        result = build_local_fixture(args.prediction_ledger, args.output_root)
        value = {"path": str(result), "manifest": inspect_local_fixture(result)}
    elif args.command == "inspect-local-feedback-fixture":
        value = inspect_local_fixture(args.fixture)
    else:
        value = inspect_feedback_monitoring(args.snapshot)
    print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
