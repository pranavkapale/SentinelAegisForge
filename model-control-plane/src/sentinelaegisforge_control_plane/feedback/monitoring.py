"""Source-neutral feedback evaluation of a frozen, verified Phase 19 prediction ledger."""

import json
import statistics
from datetime import datetime
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.datasets.labels import (
    parse_utc_timestamp,
    require_utc,
    utc_text,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.monitoring.contract import (
    SEGMENTS,
    MonitoringError,
    artifact_directory,
    identity,
    sha256,
    support_config,
)
from sentinelaegisforge_control_plane.monitoring.ledger import ROOT, inspect_ledger
from sentinelaegisforge_control_plane.monitoring.outcomes import _metrics

from .contract import FeedbackError, parse_jsonl
from .ingestion import inspect_feedback_ledger
from .resolution import EffectiveState, ResolvedFeedback, confirmed_projection, resolve_feedback

VERSION = "feedback-outcome-monitoring-v2"
DEFAULT_ROOT = ROOT / ".local/ml/feedback-monitoring-v2"
FILES = {
    "manifest.json",
    "resolved_outcomes.json",
    "quality_metrics.json",
    "segment_metrics.json",
    "coverage_report.json",
    "report.txt",
}
IDENTITY_FIELDS = (
    "monitoring_contract_version",
    "prediction_ledger_id",
    "feedback_ledger_id",
    "evaluation_as_of",
    "support_configuration",
)


def _source_evidence(
    item: ResolvedFeedback, as_of: datetime, feedback_ledger_id: str
) -> dict[str, object]:
    return {
        "event_id": item.event_id,
        "feedback_ledger_id": feedback_ledger_id,
        "state": item.state.value,
        "case_count": item.case_count,
        "selected_cases": [
            {
                "provider_id": entry.record.provider_id,
                "provider_case_id": entry.record.provider_case_id,
                "provider_record_id": entry.record.provider_record_id,
                "source_revision": entry.record.source_revision,
                "source_updated_at": utc_text(entry.record.source_updated_at),
                "received_at": utc_text(entry.record.received_at),
                "effective_available_at": utc_text(entry.effective_available_at),
                "status": entry.record.status.value,
            }
            for entry in item.selected
        ],
        "evaluation_as_of": utc_text(as_of),
    }


def _derive(
    predictions: list[dict[str, object]],
    resolved: dict[str, ResolvedFeedback],
    as_of: datetime,
    minimum: dict[str, int],
    feedback_ledger_id: str,
) -> tuple[list[dict[str, object]], dict[str, object], dict[str, object], dict[str, object]]:
    states = [
        _source_evidence(resolved[cast(str, row["event_id"])], as_of, feedback_ledger_id)
        for row in predictions
    ]
    buckets = {state.value: 0 for state in EffectiveState}
    for item in resolved.values():
        buckets[item.state.value] += 1
    if buckets[EffectiveState.AMBIGUOUS.value]:
        conflicts = [
            f"{item.event_id}: "
            + ", ".join(
                f"{entry.record.provider_id}/{entry.record.provider_case_id}"
                for entry in item.selected
            )
            for item in resolved.values()
            if item.state is EffectiveState.AMBIGUOUS
        ]
        raise FeedbackError(f"Conflicting current confirmed providers: {conflicts}")
    labeled = [
        {**row, "outcome": outcome.value}
        for row in predictions
        if (outcome := confirmed_projection(resolved[cast(str, row["event_id"])])) is not None
    ]
    total = len(predictions)
    selected_count = len(labeled)
    if sum(buckets.values()) != total:
        raise FeedbackError("Feedback state buckets do not reconcile with scored population")
    unresolved_age_hours = [
        (as_of - cast(datetime, row["ingestion_time"])).total_seconds() / 3600
        for row in predictions
        if resolved[cast(str, row["event_id"])].state
        in (EffectiveState.PENDING, EffectiveState.RETRACTED)
    ]
    coverage: dict[str, object] = {
        "scored_events": total,
        "eligible_labeled_events": selected_count,
        "label_coverage": selected_count / total,
        "confirmed_fraud": buckets[EffectiveState.FRAUD.value],
        "confirmed_legit": buckets[EffectiveState.LEGIT.value],
        "pending_cases": buckets[EffectiveState.PENDING.value],
        "retracted_cases": buckets[EffectiveState.RETRACTED.value],
        "not_yet_observed_events": buckets[EffectiveState.NOT_YET_OBSERVED.value],
        "events_with_no_feedback_case": buckets[EffectiveState.NO_CASE.value],
        "ambiguous_events": 0,
        "pending_or_retracted_age_hours_since_ingestion": {
            "minimum": min(unresolved_age_hours) if unresolved_age_hours else None,
            "median": statistics.median(unresolved_age_hours) if unresolved_age_hours else None,
            "maximum": max(unresolved_age_hours) if unresolved_age_hours else None,
        },
        "denominator_warning": (
            "Quality is conditional on confirmed outcomes; pending/retracted/unknown are not LEGIT"
        ),
    }
    quality = _metrics(labeled, minimum, data_origin="unverified_feedback")
    segments: dict[str, object] = {}
    for dimension in SEGMENTS:
        groups: dict[str, object] = {}
        for value in sorted({cast(str, row[dimension]) for row in predictions}):
            group = [row for row in predictions if row[dimension] == value]
            group_labeled = [row for row in labeled if row[dimension] == value]
            groups[value] = {
                "scored_events": len(group),
                "labeled_events": len(group_labeled),
                "unlabeled_events": len(group) - len(group_labeled),
                "label_coverage": len(group_labeled) / len(group),
                "quality": _metrics(group_labeled, minimum, data_origin="unverified_feedback"),
            }
        segments[dimension] = groups
    return states, coverage, quality, segments


def _inputs(
    prediction_path: Path, feedback_path: Path, as_of: datetime, minimum: dict[str, int]
) -> tuple[dict[str, object], dict[str, bytes], dict[str, object]]:
    ledger = inspect_ledger(prediction_path)
    feedback = inspect_feedback_ledger(feedback_path)
    if as_of < parse_utc_timestamp(cast(str, ledger["score_end"])):
        raise FeedbackError("evaluation_as_of must be at or after score_end")
    for field in (
        "source_delta_path",
        "source_delta_table_id",
        "source_delta_version",
        "source_schema_fingerprint",
    ):
        if ledger[field] != feedback[field]:
            raise FeedbackError(f"Feedback and prediction source mismatch: {field}")
    predictions = cast(
        list[dict[str, object]],
        pq.read_table(prediction_path / "predictions.parquet").to_pylist(),
    )
    records = parse_jsonl((feedback_path / "feedback.jsonl").read_bytes())
    selected = resolve_feedback(
        records, as_of, tuple(cast(str, row["event_id"]) for row in predictions)
    )
    states, coverage, quality, segments = _derive(
        predictions, selected, as_of, minimum, cast(str, feedback["feedback_ledger_id"])
    )
    basis: dict[str, object] = {
        "monitoring_contract_version": VERSION,
        "prediction_ledger_id": ledger["ledger_id"],
        "feedback_ledger_id": feedback["feedback_ledger_id"],
        "evaluation_as_of": utc_text(as_of),
        "support_configuration": minimum,
    }
    contents = {
        "resolved_outcomes.json": canonical_json(states),
        "coverage_report.json": canonical_json(coverage),
        "quality_metrics.json": canonical_json(quality),
        "segment_metrics.json": canonical_json(segments),
        "report.txt": (
            f"Synthetic provider-like feedback evaluation, not real-world fraud quality\n"
            f"Prediction ledger {ledger['ledger_id']} | "
            f"feedback ledger {feedback['feedback_ledger_id']}\n"
            f"AS_OF {utc_text(as_of)} | scored {coverage['scored_events']} | "
            f"confirmed {coverage['eligible_labeled_events']} | "
            f"pending {coverage['pending_cases']} | "
            f"retracted {coverage['retracted_cases']}\n"
            f"Quality {quality['status']} | AP {quality['average_precision']} | "
            f"ROC AUC {quality['roc_auc']}\n"
        ).encode(),
    }
    manifest: dict[str, object] = {
        **basis,
        "feedback_monitoring_snapshot_id": identity(basis),
        "prediction_ledger_path": str(prediction_path.resolve()),
        "feedback_ledger_path": str(feedback_path.resolve()),
        "candidate_id": ledger["candidate_id"],
        "provider_ids": sorted({record.provider_id for record in records}),
        "registered_model_name": ledger["registered_model_name"],
        "registered_model_version": ledger["registered_model_version"],
        "source_delta_table_id": ledger["source_delta_table_id"],
        "source_delta_version": ledger["source_delta_version"],
        "resolved_rows_fingerprint": identity(states),
        "coverage": coverage,
        "quality": quality,
        "segments": segments,
        "artifact_sha256": {name: sha256(content) for name, content in contents.items()},
        "scope": "Local synthetic provider-like fixture only; not production ground truth",
    }
    return manifest, contents, ledger


def evaluate_feedback_outcomes(
    prediction_path: Path,
    feedback_path: Path,
    evaluation_as_of: datetime,
    output_root: Path = DEFAULT_ROOT,
    *,
    min_rows: int = 20,
    min_per_class: int = 2,
) -> Path:
    try:
        as_of = require_utc(evaluation_as_of)
    except ValueError as error:
        raise FeedbackError(str(error)) from error
    minimum = support_config(min_rows, min_per_class)
    manifest, contents, _ = _inputs(prediction_path, feedback_path, as_of, minimum)
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / cast(str, manifest["feedback_monitoring_snapshot_id"])
    if target.exists():
        if inspect_feedback_monitoring(target) != manifest:
            raise FeedbackError("Existing feedback monitoring snapshot conflicts")
        return target
    target.mkdir()  # Exclusive publication; interrupted writes fail inspection.
    for name, content in contents.items():
        with (target / name).open("xb") as stream:
            stream.write(content)
    with (target / "manifest.json").open("xb") as stream:
        stream.write(canonical_json(manifest))
    inspect_feedback_monitoring(target)
    return target


def inspect_feedback_monitoring(path: Path) -> dict[str, object]:
    try:
        artifact_directory(path, FILES)
        raw = (path / "manifest.json").read_bytes()
        value: object = json.loads(raw)
        if not isinstance(value, dict) or raw != canonical_json(value):
            raise FeedbackError("Feedback monitoring manifest is not canonical")
        manifest = cast(dict[str, object], value)
        if (
            manifest["monitoring_contract_version"] != VERSION
            or path.name != manifest["feedback_monitoring_snapshot_id"]
        ):
            raise FeedbackError("Feedback monitoring version/identity differs")
        if identity({field: manifest[field] for field in IDENTITY_FIELDS}) != path.name:
            raise FeedbackError("Feedback monitoring canonical identity differs")
        hashes = cast(dict[str, str], manifest["artifact_sha256"])
        if set(hashes) != FILES - {"manifest.json"}:
            raise FeedbackError("Feedback monitoring artifact inventory differs")
        for name, expected in hashes.items():
            if sha256((path / name).read_bytes()) != expected:
                raise FeedbackError(f"Feedback monitoring artifact hash differs: {name}")
        as_of = parse_utc_timestamp(cast(str, manifest["evaluation_as_of"]))
        minimum = cast(dict[str, int], manifest["support_configuration"])
        expected_manifest, contents, _ = _inputs(
            Path(cast(str, manifest["prediction_ledger_path"])),
            Path(cast(str, manifest["feedback_ledger_path"])),
            as_of,
            support_config(minimum["minimum_rows"], minimum["minimum_per_class"]),
        )
        if manifest != expected_manifest or any(
            (path / name).read_bytes() != content for name, content in contents.items()
        ):
            raise FeedbackError("Feedback monitoring content differs from verified sources")
        return manifest
    except FeedbackError:
        raise
    except (MonitoringError, OSError, ValueError, KeyError, TypeError) as error:
        raise FeedbackError(f"Cannot inspect feedback monitoring snapshot: {error}") from error
