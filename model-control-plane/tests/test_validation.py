"""Phase 18 policy and immutable dossier checks use isolated SQLite backends."""

import json
import shutil
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from mlflow.tracking import MlflowClient
from test_registry import registry
from test_training import config, make_snapshot

from sentinelaegisforge_control_plane.candidates.run import build_candidate
from sentinelaegisforge_control_plane.registry.run import register_candidate
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig, track_run
from sentinelaegisforge_control_plane.training.run import train_baseline
from sentinelaegisforge_control_plane.validation.policy import (
    REQUIREMENTS,
    EvidenceFact,
    EvidenceState,
    ValidationPolicy,
    VerifiedFacts,
    evaluate,
)
from sentinelaegisforge_control_plane.validation.run import (
    AssessmentError,
    assess_model,
    compare_assessments,
    inspect_assessment,
)


@pytest.fixture(scope="module")
def registry_evidence(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, TrackingConfig]:
    root = tmp_path_factory.mktemp("validation-evidence")
    snapshot = make_snapshot(root)
    source = train_baseline(snapshot, config(), root / "runs")
    tracking = TrackingConfig(root / "tracking.db", root / "artifacts", "validation-test")
    tracked = track_run(source.path, tracking)
    candidate = build_candidate(
        source.path,
        root / "candidates",
        tracking_run_id=tracked["mlflow_run_id"],
        tracking=tracking,
    )
    return candidate, tracking


def facts() -> VerifiedFacts:
    verified = EvidenceFact(EvidenceState.VERIFIED, "Authoritative technical source", ("source",))
    missing = EvidenceFact(EvidenceState.NOT_ESTABLISHED, "Evidence not available")
    return VerifiedFacts(verified, verified, verified, *(missing for _ in range(7)))


def test_policy_order_blocking_and_fingerprint() -> None:
    policy = ValidationPolicy()
    result = evaluate(facts(), policy)
    rows = cast(list[dict[str, object]], result["requirements"])
    assert [row["requirement_id"] for row in rows] == [item.identifier for item in REQUIREMENTS]
    assert [row["status"] for row in rows] == ["VERIFIED"] * 3 + ["NOT_ESTABLISHED"] * 7
    assert result["technical_integrity_verified"] is True
    assert result["production_eligible"] is False
    assert len(cast(list[str], result["blocking_reasons"])) == 7
    assert policy.fingerprint == ValidationPolicy().fingerprint
    changed = replace(
        policy,
        requirements=(replace(REQUIREMENTS[0], meaning="Different integrity meaning"),)
        + REQUIREMENTS[1:],
    )
    assert changed.fingerprint != policy.fingerprint


def test_failed_technical_requirement_and_unknown_fact_fail_closed() -> None:
    failed = replace(
        facts(),
        artifact_integrity=EvidenceFact(EvidenceState.FAILED, "Source hash mismatch"),
    )
    result = evaluate(failed)
    assert result["technical_integrity_verified"] is False
    assert cast(list[dict[str, object]], result["requirements"])[0]["status"] == "FAILED"
    assert result["production_eligible"] is False
    with pytest.raises(TypeError):
        VerifiedFacts(**{**facts().__dict__, "untrusted_approval": "VERIFIED"})
    with pytest.raises(TypeError):
        evaluate(cast(VerifiedFacts, {"artifact_integrity": "VERIFIED"}))


def test_even_all_in_memory_facts_verified_cannot_authorize_promotion() -> None:
    verified = EvidenceFact(EvidenceState.VERIFIED, "Controlled evaluator fixture")
    complete = VerifiedFacts(*(verified for _ in range(10)))
    result = evaluate(complete)
    assert result["production_eligible"] is False
    assert result["promotion_readiness"] == "NOT_ELIGIBLE_FOR_PROMOTION"
    assert "no production approval authority" in cast(list[str], result["blocking_reasons"])[0]


def test_assessment_repeat_readback_report_and_compare(
    registry_evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, _ = registry_evidence
    settings = registry(registry_evidence, "assessment")
    registered = register_candidate(candidate, settings)
    version = cast(str, registered["model_version"])
    output = tmp_path / "assessments"
    first = assess_model(settings, version, output)
    prior = {item.name: item.read_bytes() for item in first.iterdir()}
    second = assess_model(settings, version, output)
    assert second == first
    assert {item.name: item.read_bytes() for item in first.iterdir()} == prior
    assessment = inspect_assessment(first, settings)
    assert assessment["production_eligible"] is False
    assert assessment["promotion_readiness"] == "NOT_ELIGIBLE_FOR_PROMOTION"
    evidence_details = cast(dict[str, object], assessment["evidence"])
    assert evidence_details["temporal_split_mechanics"] == "VERIFIED"
    assert evidence_details["representative_out_of_time_performance"] == "NOT_ESTABLISHED"
    assert evidence_details["rule_decisions_used_as_labels"] is False
    assert cast(int, evidence_details["excluded_from_fit_due_to_label_delay"]) >= 0
    assert (
        cast(dict[str, object], evidence_details["validation_metrics"])["average_precision"]
        is not None
    )
    source = Path(
        cast(
            str,
            json.loads((candidate / "candidate_manifest.json").read_bytes())[
                "source_training_path"
            ],
        )
    )
    authoritative = json.loads((source / "metrics.json").read_bytes())
    assert evidence_details["validation_metrics"] == authoritative["validation"]
    assert evidence_details["test_metrics"] == authoritative["test"]
    report = (first / "validation_report.md").read_text()
    assert "synthetic" in report.lower() and "NOT ELIGIBLE FOR PROMOTION" in report
    assert "VAL010_BUSINESS_GOVERNANCE" in report
    comparison = compare_assessments([first], settings)
    assert comparison["selection"] == "none; read-only research comparison"
    assert len(list(output.iterdir())) == 1


def test_corrupt_and_incomplete_dossiers_fail_without_repair(
    registry_evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, _ = registry_evidence
    settings = registry(registry_evidence, "dossier-corruption")
    version = cast(str, register_candidate(candidate, settings)["model_version"])
    path = assess_model(settings, version, tmp_path / "assessments")
    report = path / "validation_report.md"
    report.write_bytes(report.read_bytes() + b"tampered")
    with pytest.raises(AssessmentError, match="corrupt/conflicting"):
        assess_model(settings, version, path.parent)
    with pytest.raises(AssessmentError, match="corrupt/conflicting"):
        inspect_assessment(path, settings)
    assert report.read_bytes().endswith(b"tampered")
    (path / "assessment.json").unlink()
    with pytest.raises(AssessmentError, match="incomplete"):
        assess_model(settings, version, path.parent)


def test_registry_or_source_tampering_blocks_assessment(
    registry_evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, _ = registry_evidence
    copied = tmp_path / candidate.name
    shutil.copytree(candidate, copied)
    settings = registry(registry_evidence, "upstream-tampering")
    version = cast(str, register_candidate(copied, settings)["model_version"])
    source = copied / "model" / "model.skops"
    original = source.read_bytes()
    source.write_bytes(original + b"tampered")
    with pytest.raises(ValueError, match="hash differs"):
        assess_model(settings, version, tmp_path / "assessments")
    assert not (tmp_path / "assessments").exists()
    source.write_bytes(original)
    path = assess_model(settings, version, tmp_path / "assessments")
    assert inspect_assessment(path, settings)["assessment_id"] == path.name


def test_assessment_identity_includes_exact_registry_version_and_policy(
    registry_evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, _ = registry_evidence
    left = registry(registry_evidence, "identity-left")
    right = registry(registry_evidence, "identity-right")
    left_version = cast(str, register_candidate(candidate, left)["model_version"])
    right_version = cast(str, register_candidate(candidate, right)["model_version"])
    left_path = assess_model(left, left_version, tmp_path / "assessments")
    right_path = assess_model(right, right_version, tmp_path / "assessments")
    assert left_path != right_path
    left_manifest = json.loads((left_path / "evidence_manifest.json").read_bytes())
    right_manifest = json.loads((right_path / "evidence_manifest.json").read_bytes())
    assert (
        left_manifest["identity"]["registry_backend_identity"]
        == right_manifest["identity"]["registry_backend_identity"]
    )
    assert (
        left_manifest["identity"]["registered_model_name"]
        != right_manifest["identity"]["registered_model_name"]
    )


def test_changed_policy_creates_new_identity_without_approving(
    registry_evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, _ = registry_evidence
    settings = registry(registry_evidence, "changed-policy")
    version = cast(str, register_candidate(candidate, settings)["model_version"])
    original = assess_model(settings, version, tmp_path / "assessments")
    changed = replace(
        ValidationPolicy(),
        version="model-validation-policy-v2-test",
        requirements=REQUIREMENTS[:9]
        + (replace(REQUIREMENTS[9], meaning="New governance evidence contract"),),
    )
    changed_path = assess_model(settings, version, tmp_path / "assessments", changed)
    assert changed_path != original
    assert inspect_assessment(changed_path, settings, changed)["production_eligible"] is False
    assert len(list((tmp_path / "assessments").iterdir())) == 2


def test_registry_tag_and_manifest_conflicts_fail_closed(
    registry_evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, tracking = registry_evidence
    copied = tmp_path / candidate.name
    shutil.copytree(candidate, copied)
    settings = registry(registry_evidence, "metadata-conflict")
    version = cast(str, register_candidate(copied, settings)["model_version"])
    client = MlflowClient(tracking_uri=tracking.tracking_uri, registry_uri=tracking.tracking_uri)
    client.set_model_version_tag(settings.model_name, version, "dataset_snapshot_id", "wrong")
    with pytest.raises(ValueError, match="tags differ"):
        assess_model(settings, version, tmp_path / "assessments")
    client.set_model_version_tag(
        settings.model_name,
        version,
        "dataset_snapshot_id",
        cast(
            str,
            json.loads((copied / "candidate_manifest.json").read_bytes())["dataset_snapshot_id"],
        ),
    )
    client.set_model_version_tag(
        settings.model_name, version, "registration_status", "REGISTRATION_INCOMPLETE"
    )
    with pytest.raises(ValueError, match="tags differ"):
        assess_model(settings, version, tmp_path / "assessments")
    client.set_model_version_tag(settings.model_name, version, "registration_status", "COMPLETE")
    manifest_file = copied / "candidate_manifest.json"
    manifest_file.write_bytes(manifest_file.read_bytes() + b"tampered")
    with pytest.raises(ValueError):
        assess_model(settings, version, tmp_path / "assessments")
    assert not (tmp_path / "assessments").exists()


def test_missing_model_source_blocks_readonly_reinspection(
    registry_evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, _ = registry_evidence
    copied = tmp_path / candidate.name
    shutil.copytree(candidate, copied)
    settings = registry(registry_evidence, "missing-source")
    version = cast(str, register_candidate(copied, settings)["model_version"])
    path = assess_model(settings, version, tmp_path / "assessments")
    (copied / "model" / "model.skops").unlink()
    with pytest.raises(ValueError):
        inspect_assessment(path, settings)
    assert path.is_dir()  # Historical dossier remains untouched.


def test_two_candidate_versions_have_distinct_assessments(
    registry_evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    first_candidate, tracking = registry_evidence
    settings = registry(registry_evidence, "two-candidates")
    first_version = cast(str, register_candidate(first_candidate, settings)["model_version"])
    source_manifest = json.loads((first_candidate / "candidate_manifest.json").read_bytes())
    source_run = Path(cast(str, source_manifest["source_training_path"]))
    snapshot = Path(
        cast(str, json.loads((source_run / "manifest.json").read_bytes())["dataset_snapshot_path"])
    )
    second_run = train_baseline(snapshot, config(min_train_rows=10), tmp_path / "runs")
    tracked = track_run(second_run.path, tracking)
    second_candidate = build_candidate(
        second_run.path,
        tmp_path / "candidates",
        tracking_run_id=tracked["mlflow_run_id"],
        tracking=tracking,
    )
    assert first_candidate.name != second_candidate.name
    second_version = cast(str, register_candidate(second_candidate, settings)["model_version"])
    assert first_version != second_version
    first_assessment = assess_model(settings, first_version, tmp_path / "assessments")
    second_assessment = assess_model(settings, second_version, tmp_path / "assessments")
    assert first_assessment != second_assessment
    assert inspect_assessment(second_assessment, settings)["candidate_id"] == second_candidate.name
    comparison = compare_assessments([first_assessment, second_assessment], settings)
    assert len(cast(list[object], comparison["assessments"])) == 2
    assert comparison["selection"] == "none; read-only research comparison"
