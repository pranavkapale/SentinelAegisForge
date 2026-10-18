"""Point-in-time independent outcomes over frozen prediction ledgers."""

import json
import statistics
from datetime import datetime
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.labels import (
    LABEL_RESOLUTION_VERSION,
    LabelInput,
    TransactionOutcomeLabel,
    parse_utc_timestamp,
    require_utc,
    resolve_labels,
    utc_text,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.synthetic.corpus import inspect_corpus
from sentinelaegisforge_control_plane.training.baseline import evaluate

from .contract import (
    METRICS_VERSION,
    MONITORING_VERSION,
    OUTCOME_SCHEMA,
    SEGMENT_VERSION,
    SEGMENTS,
    MonitoringError,
    artifact_directory,
    identity,
    object_value,
    rows_fingerprint,
    sha256,
    support_config,
)
from .ledger import ROOT, inspect_ledger

DEFAULT_MONITORING_ROOT = ROOT / ".local/ml/monitoring-snapshots"
SNAPSHOT_FILES = {
    "monitoring_manifest.json",
    "quality_metrics.json",
    "segment_metrics.json",
    "coverage_report.json",
    "selected_outcomes.parquet",
    "report.txt",
}
OUTCOME_COLUMNS = tuple(OUTCOME_SCHEMA.names)
IDENTITY_FIELDS = (
    "monitoring_contract_version",
    "ledger_id",
    "candidate_id",
    "label_source_sha256",
    "evaluation_as_of",
    "label_resolution_contract_version",
    "metrics_contract_version",
    "segment_contract_version",
    "support_configuration",
)


def _metrics(
    rows: list[dict[str, object]],
    minimum: dict[str, int],
    *,
    data_origin: str = "synthetic",
) -> dict[str, object]:
    positives = sum(row["outcome"] == "FRAUD" for row in rows)
    negatives = len(rows) - positives
    base: dict[str, object] = {
        "rows": len(rows),
        "positives": positives,
        "negatives": negatives,
        "positive_prevalence": positives / len(rows) if rows else None,
        "classification_threshold": 0.5,
        "data_origin": data_origin,
    }
    reason = None
    if len(rows) < minimum["minimum_rows"]:
        reason = "INSUFFICIENT_TOTAL_ROWS"
    elif min(positives, negatives) < minimum["minimum_per_class"]:
        reason = "INSUFFICIENT_CLASS_SUPPORT"
    if reason is not None:
        return {
            **base,
            "status": "UNAVAILABLE",
            "reason": reason,
            "average_precision": None,
            "roc_auc": None,
            "precision_at_0_5": None,
            "recall_at_0_5": None,
            "f1_at_0_5": None,
            "confusion_matrix": None,
        }
    evaluated = evaluate(
        tuple({"fraud_label": int(row["outcome"] == "FRAUD")} for row in rows),
        [cast(float, row["fraud_probability_estimate"]) for row in rows],
    )
    return {**base, **evaluated, "status": "AVAILABLE", "reason": None}


def _coverage(
    predictions: list[dict[str, object]],
    selected: dict[str, TransactionOutcomeLabel],
    labels: LabelInput,
    as_of: datetime,
) -> dict[str, object]:
    scored_ids = {cast(str, row["event_id"]) for row in predictions}
    selected_scored = {key: value for key, value in selected.items() if key in scored_ids}
    delays = [
        (label.label_observed_at - cast(datetime, row["ingestion_time"])).total_seconds() / 3600
        for row in predictions
        if (label := selected_scored.get(cast(str, row["event_id"]))) is not None
    ]
    count = len(predictions)
    fraud = sum(label.outcome.value == "FRAUD" for label in selected_scored.values())
    revision_one = sum(label.label_revision == 1 for label in selected_scored.values())
    times = [label.label_observed_at for label in selected_scored.values()]
    return {
        "scored_events": count,
        "labeled_events": len(selected_scored),
        "unlabeled_events": count - len(selected_scored),
        "label_coverage": len(selected_scored) / count,
        "fraud_labeled": fraud,
        "legit_labeled": len(selected_scored) - fraud,
        "first_revision_selections": revision_one,
        "later_revision_selections": len(selected_scored) - revision_one,
        "label_source": sorted({label.label_source for label in labels.records}),
        "first_label_observed_at": utc_text(min(times)) if times else None,
        "last_label_observed_at": utc_text(max(times)) if times else None,
        "selected_label_delay_hours": {
            "minimum": min(delays) if delays else None,
            "median": statistics.median(delays) if delays else None,
            "maximum": max(delays) if delays else None,
            "under_24h": sum(delay < 24 for delay in delays),
            "24h_to_7d": sum(24 <= delay < 168 for delay in delays),
            "at_least_7d": sum(delay >= 168 for delay in delays),
        },
        "label_records_total": len(labels.records),
        "label_records_observed_by_cutoff": sum(
            label.label_observed_at <= as_of for label in labels.records
        ),
        "selected_labels_outside_scoring_window": len(selected) - len(selected_scored),
        "future_label_records": sum(label.label_observed_at > as_of for label in labels.records),
        "denominator_warning": (
            "Quality and labeled prevalence describe only matured labeled events, "
            "not all scored transactions"
        ),
    }


def _joined(
    predictions: list[dict[str, object]], selected: dict[str, TransactionOutcomeLabel]
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    matched: list[dict[str, object]] = []
    outcome_rows: list[dict[str, object]] = []
    for prediction in predictions:
        event_id = cast(str, prediction["event_id"])
        label = selected.get(event_id)
        if label is not None:
            matched.append({**prediction, "outcome": label.outcome.value})
            outcome_rows.append(
                {
                    "event_id": event_id,
                    "outcome": label.outcome.value,
                    "label_observed_at": label.label_observed_at,
                    "label_source": label.label_source,
                    "label_revision": label.label_revision,
                }
            )
    return matched, outcome_rows


def _segments(
    predictions: list[dict[str, object]],
    selected: dict[str, TransactionOutcomeLabel],
    minimum: dict[str, int],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for dimension in SEGMENTS:
        values: dict[str, dict[str, object]] = {}
        for value in sorted({cast(str, row[dimension]) for row in predictions}):
            group = [row for row in predictions if row[dimension] == value]
            labeled, _ = _joined(group, selected)
            values[value] = {
                "scored_events": len(group),
                "labeled_events": len(labeled),
                "unlabeled_events": len(group) - len(labeled),
                "label_coverage": len(labeled) / len(group),
                "quality": _metrics(labeled, minimum),
            }
        result[dimension] = values
    return result


def evaluate_outcomes(
    ledger_path: Path,
    corpus_path: Path,
    evaluation_as_of: datetime,
    output_root: Path = DEFAULT_MONITORING_ROOT,
    *,
    min_rows: int = 20,
    min_per_class: int = 2,
) -> Path:
    """Use a previously published ledger; never deserialize or rescore the model."""
    as_of = require_utc(evaluation_as_of)
    minimum = support_config(min_rows, min_per_class)
    ledger = inspect_ledger(ledger_path)
    if as_of < parse_utc_timestamp(cast(str, ledger["score_end"])):
        raise MonitoringError("evaluation_as_of must be at or after score_end")
    corpus = inspect_corpus(corpus_path)
    labels = corpus.labels
    source = StatisticalFeatureSource(
        Path(cast(str, ledger["source_delta_path"])), cast(int, ledger["source_delta_version"])
    ).read()
    if (
        source.delta_table_id != ledger["source_delta_table_id"]
        or source.source_schema_fingerprint != ledger["source_schema_fingerprint"]
        or source.delta_version != ledger["source_delta_version"]
    ):
        raise MonitoringError("Ledger feature source no longer matches pinned Delta lineage")
    source_rows = cast(list[dict[str, object]], source.table.to_pylist())
    source_by_id = {cast(str, row["event_id"]): row for row in source_rows}
    if set(source_by_id) != {event.event_id for event in corpus.plan}:
        raise MonitoringError("Verified corpus and feature-source event IDs differ")
    for label in labels.records:
        row = source_by_id.get(str(label.event_id))
        if row is None:
            raise MonitoringError(f"Outcome references unmaterialized event: {label.event_id}")
        if label.label_observed_at < cast(datetime, row["ingestion_time"]):
            raise MonitoringError(f"Outcome precedes transaction ingestion: {label.event_id}")
    predictions = cast(
        list[dict[str, object]], pq.read_table(ledger_path / "predictions.parquet").to_pylist()
    )
    selected = resolve_labels(labels, as_of)
    labeled, outcomes = _joined(predictions, selected)
    if len(labeled) != len(outcomes) or len(outcomes) > len(predictions):
        raise MonitoringError("Outcome join cardinality differs")
    coverage = _coverage(predictions, selected, labels, as_of)
    quality = _metrics(labeled, minimum)
    segments = _segments(predictions, selected, minimum)
    basis: dict[str, object] = {
        "monitoring_contract_version": MONITORING_VERSION,
        "ledger_id": ledger["ledger_id"],
        "candidate_id": ledger["candidate_id"],
        "label_source_sha256": labels.sha256,
        "evaluation_as_of": utc_text(as_of),
        "label_resolution_contract_version": LABEL_RESOLUTION_VERSION,
        "metrics_contract_version": METRICS_VERSION,
        "segment_contract_version": SEGMENT_VERSION,
        "support_configuration": minimum,
    }
    snapshot_id = identity(basis)
    contents: dict[str, bytes] = {
        "quality_metrics.json": canonical_json(quality),
        "segment_metrics.json": canonical_json(segments),
        "coverage_report.json": canonical_json(coverage),
    }
    report = "\n".join(
        [
            "Offline Synthetic Historical Replay & Delayed-Outcome Evaluation",
            f"Candidate {ledger['candidate_id']} | Registry {ledger['registered_model_name']} "
            f"v{ledger['registered_model_version']}",
            f"Ledger {ledger['ledger_id']} | Delta version {ledger['source_delta_version']}",
            f"Scoring [{ledger['score_start']}, {ledger['score_end']}) | AS_OF {utc_text(as_of)}",
            f"Scored {coverage['scored_events']} | labeled {coverage['labeled_events']} | "
            f"unlabeled {coverage['unlabeled_events']} | coverage {coverage['label_coverage']:.6f}",
            f"Labeled FRAUD {coverage['fraud_labeled']} | LEGIT {coverage['legit_labeled']} | "
            f"later revisions {coverage['later_revision_selections']}",
            f"Quality {quality['status']} | AP {quality['average_precision']} | "
            f"ROC AUC {quality['roc_auc']} | threshold 0.5",
            f"Segments: {json.dumps(segments, sort_keys=True, allow_nan=False)}",
            "Synthetic-only; observed quality is conditional on label maturity. "
            "Offline replay does not prove historical online model/feature availability, "
            "production quality, or deployment.",
            "",
        ]
    )
    contents["report.txt"] = report.encode("utf-8")
    manifest: dict[str, object] = {
        **basis,
        "monitoring_snapshot_id": snapshot_id,
        "ledger_path": str(ledger_path.resolve()),
        "registered_model_name": ledger["registered_model_name"],
        "registered_model_version": ledger["registered_model_version"],
        "source_delta_table_id": ledger["source_delta_table_id"],
        "source_delta_version": ledger["source_delta_version"],
        "label_source_file": str(labels.path),
        "corpus_id": corpus.manifest["corpus_id"],
        "outcome_rows_fingerprint": rows_fingerprint(outcomes, OUTCOME_COLUMNS),
        "coverage": coverage,
        "quality": quality,
        "segments": segments,
        "artifact_sha256": {name: sha256(content) for name, content in contents.items()},
        "scope": "Synthetic-only delayed-outcome evidence; no promotion or production monitoring",
    }
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / snapshot_id
    if target.exists():
        existing = inspect_monitoring_snapshot(target)
        expected = {
            **manifest,
            "artifact_sha256": {
                **cast(dict[str, str], manifest["artifact_sha256"]),
                "selected_outcomes.parquet": cast(dict[str, str], existing["artifact_sha256"])[
                    "selected_outcomes.parquet"
                ],
            },
        }
        if existing != expected:
            raise MonitoringError("Existing monitoring snapshot conflicts with evaluated evidence")
        return target
    target.mkdir()  # Incomplete exclusive claim fails visibly on retry.
    table = pa.Table.from_pylist(outcomes, schema=OUTCOME_SCHEMA)
    with (target / "selected_outcomes.parquet").open("xb") as stream:
        pq.write_table(table, stream, version="2.6", compression="zstd", use_dictionary=False)
    cast(dict[str, str], manifest["artifact_sha256"])["selected_outcomes.parquet"] = sha256(
        (target / "selected_outcomes.parquet").read_bytes()
    )
    for name, content in contents.items():
        with (target / name).open("xb") as stream:
            stream.write(content)
    with (target / "monitoring_manifest.json").open("xb") as stream:
        stream.write(canonical_json(manifest))
    inspect_monitoring_snapshot(target)
    return target


def inspect_monitoring_snapshot(path: Path) -> dict[str, object]:
    artifact_directory(path, SNAPSHOT_FILES)
    try:
        raw = (path / "monitoring_manifest.json").read_bytes()
        manifest = object_value(json.loads(raw), "monitoring manifest")
        if raw != canonical_json(manifest):
            raise MonitoringError("Monitoring manifest is not canonical")
        if manifest["monitoring_contract_version"] != MONITORING_VERSION:
            raise MonitoringError("Unsupported monitoring contract")
        if (
            manifest["monitoring_snapshot_id"] != path.name
            or identity({key: manifest[key] for key in IDENTITY_FIELDS}) != path.name
        ):
            raise MonitoringError("Monitoring snapshot identity differs")
        hashes = object_value(manifest["artifact_sha256"], "artifact hashes")
        if set(hashes) != SNAPSHOT_FILES - {"monitoring_manifest.json"}:
            raise MonitoringError("Monitoring artifact inventory differs")
        for name, expected in hashes.items():
            if sha256((path / name).read_bytes()) != expected:
                raise MonitoringError(f"Monitoring artifact hash differs: {name}")
        for filename, field in (
            ("quality_metrics.json", "quality"),
            ("segment_metrics.json", "segments"),
            ("coverage_report.json", "coverage"),
        ):
            if json.loads((path / filename).read_bytes()) != manifest[field]:
                raise MonitoringError(f"Monitoring {field} differs")
        table = pq.read_table(path / "selected_outcomes.parquet")
        if [(f.name, f.type) for f in table.schema] != [(f.name, f.type) for f in OUTCOME_SCHEMA]:
            raise MonitoringError("Selected-outcome schema differs")
        outcomes = cast(list[dict[str, object]], table.to_pylist())
        if len({row["event_id"] for row in outcomes}) != len(outcomes):
            raise MonitoringError("Selected-outcome identity duplicated")
        if manifest["outcome_rows_fingerprint"] != rows_fingerprint(outcomes, OUTCOME_COLUMNS):
            raise MonitoringError("Selected-outcome logical rows differ")
        coverage = object_value(manifest["coverage"], "coverage")
        if (
            coverage["labeled_events"] != len(outcomes)
            or cast(int, coverage["labeled_events"]) + cast(int, coverage["unlabeled_events"])
            != coverage["scored_events"]
        ):
            raise MonitoringError("Monitoring coverage cardinality differs")
        ledger = inspect_ledger(Path(cast(str, manifest["ledger_path"])))
        if (
            ledger["ledger_id"] != manifest["ledger_id"]
            or ledger["scored_rows"] != coverage["scored_events"]
        ):
            raise MonitoringError("Monitoring ledger identity/count differs")
        return manifest
    except MonitoringError:
        raise
    except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as error:
        raise MonitoringError(f"Cannot inspect monitoring snapshot: {error}") from error


def compare_snapshots(first: Path, second: Path) -> dict[str, object]:
    left, right = inspect_monitoring_snapshot(first), inspect_monitoring_snapshot(second)
    if left["ledger_id"] != right["ledger_id"]:
        raise MonitoringError("Monitoring snapshots use different prediction ledgers")
    if parse_utc_timestamp(cast(str, left["evaluation_as_of"])) > parse_utc_timestamp(
        cast(str, right["evaluation_as_of"])
    ):
        raise MonitoringError("Snapshot comparison requires chronological AS_OF order")
    a = {
        row["event_id"]: row
        for row in pq.read_table(first / "selected_outcomes.parquet").to_pylist()
    }
    b = {
        row["event_id"]: row
        for row in pq.read_table(second / "selected_outcomes.parquet").to_pylist()
    }
    if not set(a) <= set(b):
        raise MonitoringError("Later snapshot lost previously known labels")
    changed = [key for key in a if a[key]["label_revision"] != b[key]["label_revision"]]
    flips = sum(a[key]["outcome"] != b[key]["outcome"] for key in changed)
    left_quality = object_value(left["quality"], "first quality")
    right_quality = object_value(right["quality"], "second quality")
    return {
        "ledger_id": left["ledger_id"],
        "first_snapshot_id": left["monitoring_snapshot_id"],
        "second_snapshot_id": right["monitoring_snapshot_id"],
        "newly_labeled_events": len(set(b) - set(a)),
        "changed_label_revisions": len(changed),
        "outcome_flips": flips,
        "unchanged_labels": len(a) - len(changed),
        "first_support": {key: left_quality[key] for key in ("rows", "positives", "negatives")},
        "second_support": {key: right_quality[key] for key in ("rows", "positives", "negatives")},
        "metric_changes": {
            key: (cast(float, right_quality[key]) - cast(float, left_quality[key]))
            if isinstance(left_quality[key], (int, float))
            and isinstance(right_quality[key], (int, float))
            else None
            for key in (
                "average_precision",
                "roc_auc",
                "precision_at_0_5",
                "recall_at_0_5",
                "f1_at_0_5",
            )
        },
        "prediction_ledger_unchanged": True,
    }
