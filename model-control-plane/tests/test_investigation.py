"""Phase 21 verifies evidence-only triage, compatibility and immutable publication."""

import inspect
import json
import shutil
from pathlib import Path
from typing import Any, cast

import pytest
from deltalake import DeltaTable
from test_monitoring import _corpus_stub, _labels
from test_training import config, make_snapshot, moment

from sentinelaegisforge_control_plane.candidates.run import build_candidate
from sentinelaegisforge_control_plane.monitoring import investigation, outcomes
from sentinelaegisforge_control_plane.monitoring.contract import MonitoringError
from sentinelaegisforge_control_plane.monitoring.drift import build_drift_snapshot
from sentinelaegisforge_control_plane.monitoring.investigation import (
    build_investigation,
    inspect_investigation,
)
from sentinelaegisforge_control_plane.monitoring.ledger import build_ledger, inspect_ledger
from sentinelaegisforge_control_plane.monitoring.outcomes import evaluate_outcomes
from sentinelaegisforge_control_plane.registry.run import RegistryConfig, register_candidate
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig, track_run
from sentinelaegisforge_control_plane.training.run import train_baseline
from sentinelaegisforge_control_plane.validation.run import assess_model


@pytest.fixture(scope="module")
def replay(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, RegistryConfig, Path, Path]:
    root = tmp_path_factory.mktemp("investigation-source")
    snapshot = make_snapshot(root)
    training = train_baseline(snapshot, config(), root / "runs")
    tracking = TrackingConfig(root / "tracking.db", root / "artifacts", "investigation-test")
    tracked = track_run(training.path, tracking)
    candidate = build_candidate(
        training.path,
        root / "candidates",
        tracking_run_id=tracked["mlflow_run_id"],
        tracking=tracking,
    )
    registry = RegistryConfig(tracking, "investigation-test-model", root / "receipts")
    register_candidate(candidate, registry)
    source = root / "transaction_statistical_features_v2"
    ledger = build_ledger(
        registry, "1", source, DeltaTable(source).version(), moment(3), moment(7), root / "ledgers"
    )
    return root, registry, source, ledger


@pytest.fixture(scope="module")
def drift_source(
    replay: tuple[Path, RegistryConfig, Path, Path],
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    return build_drift_snapshot(
        replay[3],
        moment(3),
        moment(5),
        moment(5),
        moment(7),
        tmp_path_factory.mktemp("investigation-drift"),
        minimum_rows=2,
        minimum_non_null=2,
    )


def _findings(packet: Path) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        json.loads((packet / "findings.json").read_bytes())["findings"],
    )


def test_distribution_only_integrity_findings_and_repeat(
    drift_source: Path, tmp_path: Path
) -> None:
    packet = build_investigation(drift_source, tmp_path / "packets")
    manifest = cast(dict[str, Any], inspect_investigation(packet))
    before = {path.name: path.read_bytes() for path in packet.iterdir()}
    assert build_investigation(drift_source, tmp_path / "packets") == packet
    assert before == {path.name: path.read_bytes() for path in packet.iterdir()}
    assert manifest["report_configuration"]["mode"] == "distribution-only"
    findings = _findings(packet)
    ids = [row["finding_id"] for row in findings]
    assert ids[:3] == [
        "MISSINGNESS_PRIOR_AMOUNT_MEAN",
        "MISSINGNESS_PRIOR_AMOUNT_STDDEV",
        "MISSINGNESS_AMOUNT_ZSCORE",
    ]
    assert "FROZEN_SCORE_DISTRIBUTION" in ids
    assert "LABEL_EVIDENCE_ABSENT" in ids
    assert "GOVERNANCE_DOSSIER_ABSENT" in ids
    assert not any("SEVERITY" in item or "DRIFT_ALERT" in item for item in ids)
    metrics = json.loads((drift_source / "drift_metrics.json").read_bytes())
    missing = findings[0]
    assert (
        missing["measurement_context"]["reference"]
        == metrics["numeric"]["prior_amount_mean"]["reference"]["missing_fraction"]
    )
    assert (
        missing["measurement_context"]["comparison"]
        == metrics["numeric"]["prior_amount_mean"]["comparison"]["missing_fraction"]
    )
    assert {row["category"] for row in findings} >= {
        "MISSINGNESS",
        "FEATURE_DISTRIBUTION",
        "PREDICTION_DISTRIBUTION",
        "POPULATION_COMPOSITION",
        "CURRENCY_STRATIFICATION",
        "EVIDENCE_GAP",
    }
    assert "not fraud prevalence" in (packet / "runbook.md").read_text()
    for finding in findings:
        assert finding["evidence_references"]
        assert all(ref["source_id"] for ref in finding["evidence_references"])


def test_currency_and_unsupported_metric_evidence(drift_source: Path, tmp_path: Path) -> None:
    packet = build_investigation(drift_source, tmp_path / "packets")
    findings = _findings(packet)
    metrics = json.loads((drift_source / "drift_metrics.json").read_bytes())
    for currency in metrics["currency_strata"]:
        item = next(row for row in findings if row["finding_id"] == f"CURRENCY_{currency}_AMOUNT")
        assert (
            item["measurement_context"]["reference_mean"]
            == metrics["currency_strata"][currency]["features"]["amount"]["reference"]["mean"]
        )
    assert all(
        row["measurement_context"]["psi_reason"] is not None
        for row in findings
        if row["finding_id"].startswith("NUMERIC_")
        and row["measurement_context"]["psi_status"] == "UNAVAILABLE"
    )


def test_corrupt_and_incomplete_sources_fail_before_publication(
    drift_source: Path, tmp_path: Path
) -> None:
    copied = tmp_path / drift_source.name
    shutil.copytree(drift_source, copied)
    (copied / "drift_metrics.json").write_text("{}")
    output = tmp_path / "packets"
    with pytest.raises(MonitoringError):
        build_investigation(copied, output)
    assert not output.exists()
    (copied / "drift_metrics.json").unlink()
    with pytest.raises(MonitoringError):
        build_investigation(copied, output)
    assert not output.exists()


def test_explicit_ledger_mismatch_fails(drift_source: Path, tmp_path: Path) -> None:
    with pytest.raises(MonitoringError, match="ledger"):
        build_investigation(drift_source, tmp_path / "packets", ledger_path=tmp_path)
    assert not (tmp_path / "packets").exists()


def test_verified_ledger_identity_mismatch_fails(
    drift_source: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorded = json.loads((drift_source / "drift_manifest.json").read_bytes())
    path = Path(recorded["ledger_path"])
    original = inspect_ledger

    def wrong_ledger(target: Path) -> dict[str, object]:
        result = dict(original(target))
        result["ledger_id"] = "different-ledger"
        return result

    monkeypatch.setattr(investigation, "inspect_ledger", wrong_ledger)
    with pytest.raises(MonitoringError, match="differs"):
        build_investigation(drift_source, tmp_path / "packets", ledger_path=path)
    assert not (tmp_path / "packets").exists()


def test_partial_pair_rejected_and_no_optional_evidence_invented(
    drift_source: Path, tmp_path: Path
) -> None:
    with pytest.raises(MonitoringError, match="both"):
        build_investigation(drift_source, tmp_path / "packets", earlier_path=tmp_path)
    packet = build_investigation(drift_source, tmp_path / "packets")
    assert {row["finding_id"] for row in _findings(packet)} >= {
        "LABEL_EVIDENCE_ABSENT",
        "GOVERNANCE_DOSSIER_ABSENT",
    }


def test_packet_corruption_and_incomplete_directory_fail_closed(
    drift_source: Path, tmp_path: Path
) -> None:
    packet = build_investigation(drift_source, tmp_path / "packets")
    (packet / "investigation_report.md").write_text("corrupt")
    with pytest.raises(MonitoringError, match="artifact differs"):
        inspect_investigation(packet)
    with pytest.raises(MonitoringError):
        build_investigation(drift_source, tmp_path / "packets")
    (packet / "findings.json").unlink()
    with pytest.raises(MonitoringError, match="Incomplete"):
        inspect_investigation(packet)


def test_corrupt_findings_fail_without_repair(drift_source: Path, tmp_path: Path) -> None:
    packet = build_investigation(drift_source, tmp_path / "packets")
    (packet / "findings.json").write_text("{}")
    with pytest.raises(MonitoringError, match="artifact differs"):
        inspect_investigation(packet)
    with pytest.raises(MonitoringError):
        build_investigation(drift_source, tmp_path / "packets")


def test_runbook_contract_change_changes_identity(
    drift_source: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = build_investigation(drift_source, tmp_path / "packets")
    monkeypatch.setattr(investigation, "RUNBOOK_VERSION", "offline-monitoring-runbook-v2-test")
    changed = build_investigation(drift_source, tmp_path / "packets")
    assert changed != original
    assert changed.name != original.name


def test_changed_drift_source_changes_identity(
    replay: tuple[Path, RegistryConfig, Path, Path], drift_source: Path, tmp_path: Path
) -> None:
    changed = build_drift_snapshot(
        replay[3],
        moment(3),
        moment(4),
        moment(4),
        moment(7),
        tmp_path / "drift",
        minimum_rows=2,
        minimum_non_null=2,
    )
    first = build_investigation(drift_source, tmp_path / "packets")
    second = build_investigation(changed, tmp_path / "packets")
    assert first != second
    assert _findings(first) != _findings(second)


def test_unavailable_metric_reason_is_preserved(drift_source: Path) -> None:
    sources, context, metrics = investigation._load_evidence(
        drift_source, None, None, None, None, None
    )
    context["sources"] = sources
    numeric = cast(dict[str, Any], metrics["numeric"])
    amount = cast(dict[str, Any], numeric["amount"])
    amount["psi"] = {
        **amount["psi"],
        "status": "UNAVAILABLE",
        "reason": "DEGENERATE_REFERENCE",
        "value": None,
    }
    finding = next(
        row
        for row in investigation._findings(context, metrics)
        if row["finding_id"] == "NUMERIC_AMOUNT"
    )
    values = cast(dict[str, object], finding["measurement_context"])
    assert values["psi"] is None
    assert values["psi_reason"] == "DEGENERATE_REFERENCE"


def test_combined_outcomes_and_dossier_context(
    replay: tuple[Path, RegistryConfig, Path, Path],
    drift_source: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, registry, source, ledger = replay
    label_path = _labels(root)
    monkeypatch.setattr(
        outcomes, "inspect_corpus", lambda _path: _corpus_stub(root, label_path, source)
    )
    first = evaluate_outcomes(
        ledger, root, moment(8), tmp_path / "outcomes", min_rows=2, min_per_class=1
    )
    second = evaluate_outcomes(
        ledger, root, moment(9), tmp_path / "outcomes", min_rows=2, min_per_class=1
    )
    dossier = assess_model(registry, "1", tmp_path / "dossiers")
    packet = build_investigation(
        drift_source,
        tmp_path / "packets",
        earlier_path=first,
        later_path=second,
        dossier_path=dossier,
        registry=registry,
    )
    manifest = cast(dict[str, Any], inspect_investigation(packet, registry))
    assert manifest["report_configuration"]["mode"] == "combined"
    findings = _findings(packet)
    assert {row["category"] for row in findings} >= {
        "LABEL_COVERAGE",
        "LABEL_REVISION",
        "MODEL_QUALITY_CONTEXT",
    }
    comparison = next(row for row in findings if row["finding_id"] == "LABEL_REVISION_COMPARISON")
    assert comparison["measurement_context"]["newly_labeled_events"] > 0
    assert comparison["measurement_context"]["changed_label_revisions"] > 0
    assert {ref["source_id"] for ref in comparison["evidence_references"]} == {
        first.name,
        second.name,
    }
    assert "NOT_ELIGIBLE_FOR_PROMOTION" in (packet / "investigation_report.md").read_text()
    assert "nonrandom" in (packet / "runbook.md").read_text()
    assert (
        build_investigation(
            drift_source,
            tmp_path / "packets",
            earlier_path=first,
            later_path=second,
            dossier_path=dossier,
            registry=registry,
        )
        == packet
    )


def test_incompatible_outcomes_and_dossier_fail(
    drift_source: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def wrong_snapshot(_path: Path) -> dict[str, object]:
        result = cast(
            dict[str, object], json.loads((drift_source / "drift_manifest.json").read_bytes())
        )
        result["candidate_id"] = "another-candidate"
        return result

    monkeypatch.setattr(investigation, "inspect_monitoring_snapshot", wrong_snapshot)
    with pytest.raises(MonitoringError, match="incompatible"):
        build_investigation(
            drift_source,
            tmp_path / "packets",
            earlier_path=drift_source,
            later_path=drift_source,
        )
    assert not (tmp_path / "packets").exists()


def test_incompatible_dossier_fails_before_packet(
    replay: tuple[Path, RegistryConfig, Path, Path],
    drift_source: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def wrong_dossier(_path: Path, _registry: RegistryConfig) -> dict[str, object]:
        return {
            "identity": {"source_delta_version": 0, "source_schema_fingerprint": "wrong"},
            "candidate_id": "wrong",
            "registered_model_name": "wrong",
            "model_version": "1",
            "promotion_readiness": "NOT_ELIGIBLE_FOR_PROMOTION",
            "production_eligible": False,
        }

    monkeypatch.setattr(investigation, "inspect_assessment", wrong_dossier)
    with pytest.raises(MonitoringError, match="incompatible"):
        build_investigation(
            drift_source,
            tmp_path / "packets",
            dossier_path=tmp_path,
            registry=replay[1],
        )
    assert not (tmp_path / "packets").exists()


def test_no_scoring_training_registry_action_or_wall_clock(
    drift_source: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from mlflow.tracking import MlflowClient
    from sklearn.pipeline import Pipeline

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Investigation invoked a model or Registry action")

    monkeypatch.setattr(Pipeline, "fit", forbidden)
    monkeypatch.setattr(Pipeline, "predict_proba", forbidden)
    monkeypatch.setattr(MlflowClient, "create_model_version", forbidden)
    monkeypatch.setattr(MlflowClient, "set_registered_model_alias", forbidden)
    monkeypatch.setattr(MlflowClient, "set_model_version_tag", forbidden)
    packet = build_investigation(drift_source, tmp_path / "packets")
    assert inspect_investigation(packet)["investigation_id"] == packet.name
    source = inspect.getsource(investigation)
    assert "predict_proba(" not in source
    assert ".fit(" not in source
    assert "register_model(" not in source
    assert "set_registered_model_alias(" not in source
    assert "Instant.now" not in source
    assert "datetime.now(" not in source
    assert "Kafka" not in source
