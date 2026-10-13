"""Phase 17 uses temporary SQLite Registry backends and local candidate artifacts."""

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient
from test_training import config, make_snapshot

from sentinelaegisforge_control_plane.candidates.run import build_candidate
from sentinelaegisforge_control_plane.registry.run import (
    GOVERNANCE_STATE,
    RegistryConfig,
    RegistryError,
    compare_registered_candidates,
    evaluate_eligibility,
    inspect_registered_candidate,
    register_candidate,
)
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig, track_run
from sentinelaegisforge_control_plane.training.run import train_baseline


@pytest.fixture(scope="module")
def evidence(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, TrackingConfig]:
    root = tmp_path_factory.mktemp("registry-evidence")
    snapshot = make_snapshot(root)
    source = train_baseline(snapshot, config(), root / "runs")
    tracking = TrackingConfig(root / "tracking.db", root / "artifacts", "registry-test-experiment")
    tracked = track_run(source.path, tracking)
    candidate = build_candidate(
        source.path,
        root / "candidates",
        tracking_run_id=tracked["mlflow_run_id"],
        tracking=tracking,
    )
    return candidate, tracking


def registry(evidence: tuple[Path, TrackingConfig], name: str) -> RegistryConfig:
    candidate, tracking = evidence
    return RegistryConfig(tracking, f"test-{name}", candidate.parent.parent / "receipts")


def test_initial_registration_repeat_readback_and_research_governance(
    evidence: tuple[Path, TrackingConfig],
) -> None:
    candidate, tracking = evidence
    settings = registry(evidence, "initial")
    first = register_candidate(candidate, settings)
    second = register_candidate(candidate, settings)
    assert first == second
    assert first["governance_state"] == GOVERNANCE_STATE
    assert first["production_eligible"] is False
    assert first["source_tracking_run_id"]
    assert first["registry_status"] == "READY"
    assert Path(cast(str, first["receipt_path"])).is_file()
    shallow = inspect_registered_candidate(settings, cast(str, first["model_version"]))
    assert shallow["candidate_id"] == candidate.name
    deep = inspect_registered_candidate(settings, cast(str, first["model_version"]), deep=True)
    assert 0 <= cast(float, deep["offline_probability"]) <= 1
    assert deep["positive_class"] == "FRAUD"
    client = MlflowClient(tracking_uri=tracking.tracking_uri, registry_uri=tracking.tracking_uri)
    versions = client.search_model_versions(filter_string=f"name = '{settings.model_name}'")
    assert len(versions) == 1
    assert versions[0].run_id is None
    assert versions[0].aliases == []
    assert client.get_registered_model(settings.model_name).aliases == {}
    assert compare_registered_candidates(settings, [cast(str, first["model_version"])])["selection"]


def test_conflicting_tags_and_incomplete_claims_fail_closed(
    evidence: tuple[Path, TrackingConfig],
) -> None:
    candidate, tracking = evidence
    settings = registry(evidence, "conflicts")
    first = register_candidate(candidate, settings)
    client = MlflowClient(tracking_uri=tracking.tracking_uri, registry_uri=tracking.tracking_uri)
    version = cast(str, first["model_version"])
    client.set_model_version_tag(settings.model_name, version, "dataset_snapshot_id", "wrong")
    with pytest.raises(RegistryError, match="tags differ"):
        register_candidate(candidate, settings)
    client.set_model_version_tag(
        settings.model_name, version, "dataset_snapshot_id", cast(str, first["dataset_snapshot_id"])
    )
    client.set_model_version_tag(
        settings.model_name, version, "registration_status", "REGISTRATION_INCOMPLETE"
    )
    with pytest.raises(RegistryError, match="tags differ"):
        register_candidate(candidate, settings)


def test_interrupted_registration_stays_visible_and_retry_fails_closed(
    evidence: tuple[Path, TrackingConfig], monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, tracking = evidence
    settings = registry(evidence, "interrupted")
    original = MlflowClient.set_model_version_tag

    def fail_completion(
        self: MlflowClient,
        name: str,
        version: str | None = None,
        key: str | None = None,
        value: object = None,
        stage: str | None = None,
    ) -> None:
        raise RuntimeError("simulated tag-write interruption")

    monkeypatch.setattr(MlflowClient, "set_model_version_tag", fail_completion)
    with pytest.raises(RuntimeError, match="interruption"):
        register_candidate(candidate, settings)
    monkeypatch.setattr(MlflowClient, "set_model_version_tag", original)
    client = MlflowClient(tracking_uri=tracking.tracking_uri, registry_uri=tracking.tracking_uri)
    versions = client.search_model_versions(filter_string=f"name = '{settings.model_name}'")
    assert len(versions) == 1
    assert versions[0].tags["registration_status"] == "REGISTRATION_INCOMPLETE"
    with pytest.raises(RegistryError, match="tags differ"):
        register_candidate(candidate, settings)
    assert len(client.search_model_versions(filter_string=f"name = '{settings.model_name}'")) == 1
    assert not list(
        (settings.receipts / settings.backend_identity / settings.model_name).glob("*.json")
    )


def test_duplicate_candidate_claim_fails_without_selecting_highest(
    evidence: tuple[Path, TrackingConfig],
) -> None:
    candidate, tracking = evidence
    settings = registry(evidence, "duplicate")
    first = register_candidate(candidate, settings)
    client = MlflowClient(tracking_uri=tracking.tracking_uri, registry_uri=tracking.tracking_uri)
    client.create_model_version(
        settings.model_name,
        cast(str, first["source_uri"]),
        tags={"candidate_id": candidate.name},
    )
    with pytest.raises(RegistryError, match="Multiple Registry versions"):
        register_candidate(candidate, settings)


def test_missing_and_tampered_source_fail_inspection(
    evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, tracking = evidence
    copied = tmp_path / candidate.name
    shutil.copytree(candidate, copied)
    settings = registry(evidence, "source-integrity")
    first = register_candidate(copied, settings)
    model_file = copied / "model" / "model.skops"
    original = model_file.read_bytes()
    model_file.write_bytes(original + b"tampered")
    with pytest.raises(RegistryError, match="hash differs"):
        inspect_registered_candidate(settings, cast(str, first["model_version"]))
    model_file.write_bytes(original)
    shutil.rmtree(copied)
    with pytest.raises(RegistryError, match="missing"):
        inspect_registered_candidate(settings, cast(str, first["model_version"]))
    _ = tracking


def test_corrupt_receipt_is_not_repaired_on_retry(evidence: tuple[Path, TrackingConfig]) -> None:
    candidate, _ = evidence
    settings = registry(evidence, "receipt-conflict")
    first = register_candidate(candidate, settings)
    path = Path(cast(str, first["receipt_path"]))
    path.write_bytes(path.read_bytes() + b"tampered")
    with pytest.raises(RegistryError, match="receipt"):
        register_candidate(candidate, settings)
    with pytest.raises(RegistryError, match="receipt"):
        inspect_registered_candidate(settings, cast(str, first["model_version"]))


def test_corrupt_or_unknown_candidate_rejected_before_registry_mutation(
    evidence: tuple[Path, TrackingConfig], tmp_path: Path
) -> None:
    candidate, tracking = evidence
    copied = tmp_path / candidate.name
    shutil.copytree(candidate, copied)
    manifest_file = copied / "candidate_manifest.json"
    manifest = json.loads(manifest_file.read_bytes())
    manifest["candidate_contract_version"] = "unknown"
    manifest_file.write_text(json.dumps(manifest))
    settings = registry(evidence, "rejected")
    with pytest.raises(ValueError, match="contract/status differs"):
        register_candidate(copied, settings)
    client = MlflowClient(tracking_uri=tracking.tracking_uri, registry_uri=tracking.tracking_uri)
    assert not client.search_registered_models(filter_string=f"name = '{settings.model_name}'")


def test_unverified_and_promotion_claims_are_ineligible(
    evidence: tuple[Path, TrackingConfig],
) -> None:
    candidate, _ = evidence
    manifest = json.loads((candidate / "candidate_manifest.json").read_bytes())
    assert evaluate_eligibility(manifest).production_eligible is False
    for changes in (
        {"validation_status": "UNVERIFIED"},
        {"input_schema_fingerprint": "unknown"},
        {"data_origin": "real"},
        {"production_fraud_quality": "proven"},
        {"source_mlflow_run_id": None},
    ):
        with pytest.raises(RegistryError):
            evaluate_eligibility({**manifest, **changes})
    assert not hasattr(
        __import__("sentinelaegisforge_control_plane.registry.run", fromlist=["promote"]), "promote"
    )


def test_registry_backend_isolation(evidence: tuple[Path, TrackingConfig], tmp_path: Path) -> None:
    candidate, _ = evidence
    other_tracking = TrackingConfig(tmp_path / "other.db", tmp_path / "artifacts", "other")
    client = MlflowClient(tracking_uri=other_tracking.tracking_uri)
    client.create_experiment("other", artifact_location=other_tracking.artifact_uri)
    settings = RegistryConfig(other_tracking, "isolated-model", tmp_path / "receipts")
    with pytest.raises(MlflowException, match="not found"):
        register_candidate(candidate, settings)  # Phase 15 provenance is absent in this backend.
    assert not client.search_registered_models(filter_string="name = 'isolated-model'")


def test_explicit_numeric_version_required(evidence: tuple[Path, TrackingConfig]) -> None:
    settings = registry(evidence, "numeric")
    for version in ("latest", "0", "Production", "-1"):
        with pytest.raises(RegistryError, match="explicit positive numeric"):
            inspect_registered_candidate(settings, version)
