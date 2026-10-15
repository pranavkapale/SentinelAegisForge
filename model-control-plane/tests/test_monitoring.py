"""Phase 19 verifies offline replay and evolving synthetic outcomes without Docker."""

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake
from test_training import config, make_snapshot, moment

from sentinelaegisforge_control_plane.candidates.run import build_candidate
from sentinelaegisforge_control_plane.datasets.labels import load_labels
from sentinelaegisforge_control_plane.monitoring import outcomes
from sentinelaegisforge_control_plane.monitoring.contract import (
    MonitoringError,
    identity,
    support_config,
    utc_window,
)
from sentinelaegisforge_control_plane.monitoring.ledger import build_ledger, inspect_ledger
from sentinelaegisforge_control_plane.monitoring.outcomes import (
    _metrics,
    compare_snapshots,
    evaluate_outcomes,
    inspect_monitoring_snapshot,
)
from sentinelaegisforge_control_plane.registry.run import RegistryConfig, register_candidate
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig, track_run
from sentinelaegisforge_control_plane.training.run import train_baseline


@pytest.fixture(scope="module")
def replay(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, RegistryConfig, Path, Path]:
    root = tmp_path_factory.mktemp("offline-monitoring")
    snapshot = make_snapshot(root)
    training = train_baseline(snapshot, config(), root / "runs")
    tracking = TrackingConfig(root / "tracking.db", root / "artifacts", "monitoring-test")
    tracked = track_run(training.path, tracking)
    candidate = build_candidate(
        training.path,
        root / "candidates",
        tracking_run_id=tracked["mlflow_run_id"],
        tracking=tracking,
    )
    registry = RegistryConfig(tracking, "monitoring-test-model", root / "receipts")
    register_candidate(candidate, registry)
    source = root / "transaction_statistical_features_v2"
    ledger = build_ledger(
        registry, "1", source, DeltaTable(source).version(), moment(3), moment(7), root / "ledgers"
    )
    return root, registry, source, ledger


def _labels(root: Path) -> Path:
    original = (root / "labels.jsonl").read_text().splitlines()
    changed: list[dict[str, object]] = []
    for line in original:
        record = cast(dict[str, object], json.loads(line))
        if int(cast(str, record["event_id"]).split("-")[-1], 16) in (26, 27):
            record["label_observed_at"] = moment(8, 30).isoformat().replace("+00:00", "Z")
        changed.append(record)
    revised = dict(changed[27])
    revised["label_revision"] = 2
    revised["outcome"] = "LEGIT" if revised["outcome"] == "FRAUD" else "FRAUD"
    revised["label_observed_at"] = moment(9).isoformat().replace("+00:00", "Z")
    changed.append(revised)
    path = root / "monitoring-labels.jsonl"
    path.write_text("".join(json.dumps(item) + "\n" for item in changed))
    return path


def _corpus_stub(root: Path, labels_path: Path, source: Path) -> SimpleNamespace:
    ids = [row["event_id"] for row in DeltaTable(source).to_pyarrow_table().to_pylist()]
    return SimpleNamespace(
        labels=load_labels(labels_path),
        plan=tuple(SimpleNamespace(event_id=event_id) for event_id in ids),
        manifest={"corpus_id": "isolated-synthetic-fixture"},
    )


def test_ledger_exact_registry_frozen_source_and_archive_equivalence(
    replay: tuple[Path, RegistryConfig, Path, Path],
) -> None:
    root, registry, source, ledger = replay
    manifest = inspect_ledger(ledger)
    comparison = cast(dict[str, object], manifest["archived_prediction_comparison"])
    assert manifest["scored_rows"] == 24
    assert manifest["excluded_before_window"] == 25
    assert manifest["excluded_after_window"] == 1
    assert comparison["overlap_count"] == 24
    assert comparison["mismatches"] == 0
    assert comparison["max_absolute_difference"] == 0
    rows = pq.read_table(ledger / "predictions.parquet").to_pylist()
    assert len({row["event_id"] for row in rows}) == len(rows)
    assert all(0 <= row["fraud_probability_estimate"] <= 1 for row in rows)
    assert all(row["candidate_id"] == manifest["candidate_id"] for row in rows)
    assert not {"fraud_label", "risk_disposition", "customer_id", "label_revision"} & set(rows[0])
    assert build_ledger(registry, "1", source, 0, moment(3), moment(7), root / "ledgers") == ledger


def test_scoring_rejects_unpinned_or_training_period_window(
    replay: tuple[Path, RegistryConfig, Path, Path],
) -> None:
    root, registry, source, _ = replay
    with pytest.raises(ValueError, match="numeric"):
        build_ledger(registry, "latest", source, 0, moment(3), moment(7), root / "other")
    with pytest.raises(MonitoringError, match="training cutoff"):
        build_ledger(registry, "1", source, 0, moment(1), moment(7), root / "other")
    other = root / "other-source"
    write_deltalake(other, DeltaTable(source).to_pyarrow_table())
    with pytest.raises(MonitoringError, match="lineage"):
        build_ledger(registry, "1", other, 0, moment(3), moment(7), root / "other")


def test_scoring_contract_has_no_label_or_risk_input() -> None:
    import inspect

    parameters = set(inspect.signature(build_ledger).parameters)
    assert not {"labels", "label_path", "risk_decisions", "fraud_label"} & parameters
    with pytest.raises(MonitoringError, match="precede"):
        utc_window(moment(7), moment(3))
    with pytest.raises(ValueError, match="UTC"):
        utc_window(datetime(2030, 1, 1), moment(3))


def test_corrupt_or_incomplete_ledger_fails_closed(
    replay: tuple[Path, RegistryConfig, Path, Path], tmp_path: Path
) -> None:
    _, _, _, ledger = replay
    target = tmp_path / ledger.name
    target.mkdir()
    with pytest.raises(MonitoringError, match="Incomplete"):
        inspect_ledger(target)
    shutil.copy2(ledger / "manifest.json", target / "manifest.json")
    shutil.copy2(ledger / "predictions.parquet", target / "predictions.parquet")
    with (target / "predictions.parquet").open("ab") as handle:
        handle.write(b"corrupt")
    with pytest.raises(MonitoringError, match="Cannot inspect prediction ledger|hash differs"):
        inspect_ledger(target)


def test_metric_support_is_explicit_and_one_class_is_unavailable() -> None:
    minimum = support_config(2, 1)
    assert _metrics([], minimum)["reason"] == "INSUFFICIENT_TOTAL_ROWS"
    one = [{"outcome": "LEGIT", "fraud_probability_estimate": 0.2}] * 2
    assert _metrics(one, minimum)["reason"] == "INSUFFICIENT_CLASS_SUPPORT"
    assert _metrics(one, minimum)["roc_auc"] is None
    both = [one[0], {"outcome": "FRAUD", "fraud_probability_estimate": 0.8}]
    assert _metrics(both, minimum)["status"] == "AVAILABLE"
    assert _metrics(both, minimum)["roc_auc"] == 1.0
    with pytest.raises(MonitoringError):
        support_config(0, 1)


def test_delayed_label_cutoffs_revisions_coverage_and_frozen_probabilities(
    replay: tuple[Path, RegistryConfig, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _, source, ledger = replay
    label_path = _labels(root)
    monkeypatch.setattr(
        outcomes, "inspect_corpus", lambda _path: _corpus_stub(root, label_path, source)
    )
    first = evaluate_outcomes(
        ledger, root, moment(8), root / "monitoring", min_rows=2, min_per_class=1
    )
    second = evaluate_outcomes(
        ledger, root, moment(9), root / "monitoring", min_rows=2, min_per_class=1
    )
    a, b = inspect_monitoring_snapshot(first), inspect_monitoring_snapshot(second)
    a_coverage = cast(dict[str, object], a["coverage"])
    b_coverage = cast(dict[str, object], b["coverage"])
    assert a_coverage["scored_events"] == b_coverage["scored_events"] == 24
    assert cast(int, a_coverage["labeled_events"]) < cast(int, b_coverage["labeled_events"])
    assert cast(int, a_coverage["unlabeled_events"]) > cast(int, b_coverage["unlabeled_events"])
    assert a_coverage["later_revision_selections"] == 0
    assert b_coverage["later_revision_selections"] == 1
    changes = compare_snapshots(first, second)
    assert cast(int, changes["newly_labeled_events"]) > 0
    assert changes["changed_label_revisions"] == changes["outcome_flips"] == 1
    assert changes["prediction_ledger_unchanged"] is True
    assert (
        evaluate_outcomes(ledger, root, moment(8), root / "monitoring", min_rows=2, min_per_class=1)
        == first
    )
    assert inspect_monitoring_snapshot(first) == a
    assert identity({"cutoff": "A"}) != identity({"cutoff": "B"})


def test_outcome_cutoff_and_unknown_corpus_event_fail(
    replay: tuple[Path, RegistryConfig, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _, source, ledger = replay
    label_path = _labels(root)
    monkeypatch.setattr(
        outcomes, "inspect_corpus", lambda _path: _corpus_stub(root, label_path, source)
    )
    with pytest.raises(MonitoringError, match="at or after score_end"):
        evaluate_outcomes(ledger, root, moment(6), root / "invalid")
    corpus = _corpus_stub(root, label_path, source)
    corpus.plan = corpus.plan[1:]
    monkeypatch.setattr(outcomes, "inspect_corpus", lambda _path: corpus)
    with pytest.raises(MonitoringError, match="event IDs differ"):
        evaluate_outcomes(ledger, root, moment(9), root / "invalid")


def test_unknown_outcome_identity_and_no_rescoring_during_evaluation(
    replay: tuple[Path, RegistryConfig, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _, source, ledger = replay
    label_path = _labels(root)
    monkeypatch.setattr(
        outcomes, "inspect_corpus", lambda _path: _corpus_stub(root, label_path, source)
    )
    import sentinelaegisforge_control_plane.monitoring.ledger as scoring_module

    monkeypatch.setattr(
        scoring_module,
        "score_candidate",
        lambda *_args, **_kwargs: pytest.fail("evaluation rescored"),
    )
    assert evaluate_outcomes(ledger, root, moment(9), root / "no-rescore").is_dir()
    new_label = json.loads(label_path.read_text().splitlines()[0])
    new_label["event_id"] = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    label_path.write_text(label_path.read_text() + json.dumps(new_label) + "\n")
    with pytest.raises(MonitoringError, match="unmaterialized event"):
        evaluate_outcomes(ledger, root, moment(9), root / "no-rescore")


def test_outcome_causality_and_invalid_revisions_fail(
    replay: tuple[Path, RegistryConfig, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _, source, ledger = replay
    label_path = _labels(root)
    original = label_path.read_text()
    records = [json.loads(line) for line in original.splitlines()]
    records[25]["label_observed_at"] = (
        (moment(3) - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    )
    label_path.write_text("".join(json.dumps(row) + "\n" for row in records))
    monkeypatch.setattr(
        outcomes, "inspect_corpus", lambda _path: _corpus_stub(root, label_path, source)
    )
    with pytest.raises(MonitoringError, match="precedes transaction ingestion"):
        evaluate_outcomes(ledger, root, moment(9), root / "invalid")
    label_path.write_text(original + original.splitlines()[25] + "\n")
    with pytest.raises(ValueError, match="revisions must be unique"):
        evaluate_outcomes(ledger, root, moment(9), root / "invalid")


def test_snapshot_corruption_and_label_hash_identity(
    replay: tuple[Path, RegistryConfig, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _, source, ledger = replay
    label_path = _labels(root)
    monkeypatch.setattr(
        outcomes, "inspect_corpus", lambda _path: _corpus_stub(root, label_path, source)
    )
    first = evaluate_outcomes(
        ledger, root, moment(9), root / "another", min_rows=2, min_per_class=1
    )
    old_id = first.name
    label_path.write_bytes(label_path.read_bytes() + b"\n")
    # The Phase 12 loader rejects an empty JSONL record rather than silently changing a hash.
    with pytest.raises(ValueError):
        evaluate_outcomes(ledger, root, moment(9), root / "another", min_rows=2, min_per_class=1)
    label_path.write_bytes(label_path.read_bytes().rstrip() + b"\n")
    changed = [json.loads(line) for line in label_path.read_text().splitlines()]
    changed[25]["label_source"] = "SYNTHETIC_FIXTURE_V2"
    label_path.write_text("".join(json.dumps(row) + "\n" for row in changed))
    second = evaluate_outcomes(
        ledger, root, moment(9), root / "another", min_rows=2, min_per_class=1
    )
    assert second.name != old_id
    assert inspect_monitoring_snapshot(first)["monitoring_snapshot_id"] == old_id
    with (second / "quality_metrics.json").open("ab") as stream:
        stream.write(b"tamper")
    with pytest.raises(MonitoringError, match="hash differs"):
        inspect_monitoring_snapshot(second)


def test_incomplete_snapshot_and_ledger_conflict_remain_visible(
    replay: tuple[Path, RegistryConfig, Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, registry, source, ledger = replay
    incomplete = tmp_path / "ledgers" / ledger.name
    incomplete.mkdir(parents=True)
    with pytest.raises(MonitoringError, match="Incomplete"):
        build_ledger(registry, "1", source, 0, moment(3), moment(7), incomplete.parent)
    label_path = _labels(root)
    monkeypatch.setattr(
        outcomes, "inspect_corpus", lambda _path: _corpus_stub(root, label_path, source)
    )
    published = evaluate_outcomes(ledger, root, moment(9), tmp_path / "snapshots")
    corrupt = tmp_path / "copy" / published.name
    shutil.copytree(published, corrupt)
    (corrupt / "report.txt").unlink()
    with pytest.raises(MonitoringError, match="Incomplete"):
        inspect_monitoring_snapshot(corrupt)


def test_segment_denominators_and_snapshot_revisions(
    replay: tuple[Path, RegistryConfig, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _, source, ledger = replay
    label_path = _labels(root)
    monkeypatch.setattr(
        outcomes, "inspect_corpus", lambda _path: _corpus_stub(root, label_path, source)
    )
    path = evaluate_outcomes(
        ledger, root, moment(9), root / "segments", min_rows=20, min_per_class=2
    )
    result = inspect_monitoring_snapshot(path)
    segments = cast(dict[str, object], result["segments"])
    coverage = cast(dict[str, object], result["coverage"])
    for dimension in ("currency", "country", "transaction_type"):
        groups = cast(dict[str, dict[str, object]], segments[dimension])
        assert sum(cast(int, group["scored_events"]) for group in groups.values()) == 24
        assert (
            sum(cast(int, group["labeled_events"]) for group in groups.values())
            == coverage["labeled_events"]
        )
    currency = cast(dict[str, dict[str, object]], segments["currency"])
    eur_quality = cast(dict[str, object], currency["EUR"]["quality"])
    assert eur_quality["status"] == "UNAVAILABLE"
