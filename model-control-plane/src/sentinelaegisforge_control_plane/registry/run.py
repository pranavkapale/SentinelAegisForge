"""Catalog verified local candidates without promotion or model reconstruction."""

import fcntl
import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast
from urllib.parse import unquote, urlparse

import pyarrow.parquet as pq
from mlflow.entities.model_registry import ModelVersion, RegisteredModel
from mlflow.tracking import MlflowClient

from sentinelaegisforge_control_plane.candidates.contract import (
    CONTRACT_VERSION,
    INPUT_CONTRACT,
    INPUT_FINGERPRINT,
    SERIALIZATION_FORMAT,
)
from sentinelaegisforge_control_plane.candidates.run import (
    MODEL_DIR,
    _files,
    _runtime,
    _validated_source,
    _verify_tracking,
    inspect_candidate,
    score_candidate,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig
from sentinelaegisforge_control_plane.training.run import inspect_run

GOVERNANCE_VERSION = "model-registry-governance-v1"
REGISTRY_CONTRACT_VERSION = "research-candidate-registry-v1"
DEFAULT_MODEL_NAME = "sentinelaegisforge-offline-fraud-logistic"
DEFAULT_RECEIPT_ROOT = TrackingConfig().database.parent / "registration-receipts"
GOVERNANCE_STATE = "REGISTERED_RESEARCH_ONLY"
PRODUCTION_REASONS = (
    "Synthetic-only labels are not independently verified fraud outcomes",
    "Representative production quality and probability calibration are unproven",
    "No shadow/canary evidence, business approval, or serving contract exists",
)


class RegistryError(ValueError):
    """Registration or read-back evidence is incomplete or contradictory."""


@dataclass(frozen=True)
class RegistryConfig:
    tracking: TrackingConfig = TrackingConfig()
    model_name: str = DEFAULT_MODEL_NAME
    receipts: Path = DEFAULT_RECEIPT_ROOT

    def __post_init__(self) -> None:
        if self.model_name in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9_.-]+", self.model_name):
            raise RegistryError("Registered-model name must contain only letters, digits, . _ -")
        if not self.receipts.is_absolute():
            raise RegistryError("Receipt root must be an absolute path")

    @property
    def backend_identity(self) -> str:
        return hashlib.sha256(self.tracking.tracking_uri.encode()).hexdigest()


@dataclass(frozen=True)
class Eligibility:
    candidate_id: str
    artifact_integrity: str
    provenance_integrity: str
    input_contract_integrity: str
    prediction_equivalence: str
    data_origin: str
    evaluation_scope: str
    production_quality_evidence: str
    registration_eligible: bool
    production_eligible: bool
    reasons: tuple[str, ...]


def _text(manifest: dict[str, object], key: str) -> str:
    value = manifest.get(key)
    if not isinstance(value, str) or not value:
        raise RegistryError(f"Missing candidate provenance: {key}")
    return value


def evaluate_eligibility(manifest: dict[str, object]) -> Eligibility:
    """Only a fully inspected synthetic candidate is eligible for research cataloging."""
    comparison = manifest.get("comparison")
    if not isinstance(comparison, dict):
        raise RegistryError("Candidate prediction-equivalence evidence is missing")
    if (
        manifest.get("candidate_contract_version") != CONTRACT_VERSION
        or manifest.get("validation_status") != "TECHNICALLY_VERIFIED"
        or manifest.get("input_schema_fingerprint") != INPUT_FINGERPRINT
        or manifest.get("serialization_format") != SERIALIZATION_FORMAT
        or manifest.get("positive_class") != {"label": 1, "meaning": "FRAUD"}
        or comparison.get("outcome") != "EQUIVALENT"
        or comparison.get("mismatches") != 0
        or manifest.get("round_trip_comparison") != comparison
        or not isinstance(comparison.get("compared_predictions"), int)
        or comparison["compared_predictions"] <= 0
        or manifest.get("data_origin") != "synthetic"
        or manifest.get("evaluation_scope") != "integration_verification"
        or manifest.get("production_fraud_quality") != "unproven"
    ):
        raise RegistryError("Candidate is not eligible for research registration")
    for key in (
        "candidate_id",
        "training_run_id",
        "source_mlflow_run_id",
        "dataset_snapshot_id",
        "dataset_logical_fingerprint",
        "source_schema_fingerprint",
        "model_tree_sha256",
    ):
        _text(manifest, key)
    return Eligibility(
        _text(manifest, "candidate_id"),
        "VERIFIED",
        "VERIFIED",
        "VERIFIED",
        "VERIFIED",
        "SYNTHETIC",
        "integration_verification",
        "UNPROVEN",
        True,
        False,
        PRODUCTION_REASONS,
    )


def _tags(manifest: dict[str, object]) -> dict[str, str]:
    return {
        "candidate_id": _text(manifest, "candidate_id"),
        "candidate_contract_version": CONTRACT_VERSION,
        "training_run_id": _text(manifest, "training_run_id"),
        "source_tracking_run_id": _text(manifest, "source_mlflow_run_id"),
        "dataset_snapshot_id": _text(manifest, "dataset_snapshot_id"),
        "dataset_logical_fingerprint": _text(manifest, "dataset_logical_fingerprint"),
        "source_delta_version": str(manifest["source_delta_version"]),
        "source_schema_fingerprint": _text(manifest, "source_schema_fingerprint"),
        "input_contract_fingerprint": INPUT_FINGERPRINT,
        "model_tree_sha256": _text(manifest, "model_tree_sha256"),
        "serialization_format": SERIALIZATION_FORMAT,
        "model_family": "logistic_regression",
        "data_origin": "synthetic",
        "evaluation_scope": "integration_verification",
        "production_fraud_quality": "unproven",
        "registry_governance_version": GOVERNANCE_VERSION,
        "governance_state": GOVERNANCE_STATE,
        "registration_eligible": "true",
        "production_eligible": "false",
    }


def _client(config: RegistryConfig) -> MlflowClient:
    if not config.tracking.database.is_file():
        raise RegistryError("Existing Phase 15 SQLite backend is missing")
    return MlflowClient(
        tracking_uri=config.tracking.tracking_uri,
        registry_uri=config.tracking.tracking_uri,
    )


def _versions(client: MlflowClient, name: str) -> list[ModelVersion]:
    versions: list[ModelVersion] = []
    token: str | None = None
    while True:
        page = client.search_model_versions(
            filter_string=f"name = '{name}'", max_results=100, page_token=token
        )
        versions.extend(page)
        token = page.token
        if not token:
            return versions


def _model(client: MlflowClient, name: str) -> RegisteredModel | None:
    matches = client.search_registered_models(filter_string=f"name = '{name}'")
    return matches[0] if matches else None


def _source_path(uri: str | None) -> Path:
    if uri is None:
        raise RegistryError("Registry model source is missing")
    parsed = urlparse(uri)
    if parsed.scheme != "file" or parsed.netloc or not parsed.path:
        raise RegistryError("Only the verified local file:// model source is supported")
    path = Path(unquote(parsed.path))
    if not path.is_absolute() or path.name != MODEL_DIR:
        raise RegistryError("Registry source is not a local Phase 16 model directory")
    return path


def _source_metadata(path: Path, tracking: TrackingConfig) -> dict[str, object]:
    """Read-only hash/provenance check; does not deserialize or score the model."""
    if path.is_symlink() or not path.is_dir():
        raise RegistryError("Registered candidate source is missing or linked")
    if {entry.name for entry in path.iterdir()} != {
        MODEL_DIR,
        "input_contract.json",
        "equivalence.json",
        "candidate_manifest.json",
    }:
        raise RegistryError("Registered candidate source is incomplete")
    manifest_path = path / "candidate_manifest.json"
    if manifest_path.is_symlink():
        raise RegistryError("Candidate manifest symlink is forbidden")
    try:
        raw: object = json.loads(manifest_path.read_bytes())
        if not isinstance(raw, dict):
            raise RegistryError("Candidate manifest is not an object")
        manifest = cast(dict[str, object], raw)
        if (
            path.name != _text(manifest, "candidate_id")
            or manifest.get("runtime_versions") != _runtime()
            or manifest.get("candidate_contract_version") != CONTRACT_VERSION
            or manifest.get("input_schema_fingerprint") != INPUT_FINGERPRINT
            or manifest.get("validation_status") != "TECHNICALLY_VERIFIED"
        ):
            raise RegistryError("Candidate identity/runtime/contract differs")
        files = _files(path / MODEL_DIR)
        tree_hash = hashlib.sha256(canonical_json(files)).hexdigest()
        if manifest.get("model_files") != files or manifest.get("model_tree_sha256") != tree_hash:
            raise RegistryError("Registered model source hash differs")
        for filename, key, expected in (
            ("input_contract.json", "input_contract_sha256", INPUT_CONTRACT),
            ("equivalence.json", "equivalence_sha256", manifest.get("comparison")),
        ):
            file = path / filename
            if file.is_symlink() or hashlib.sha256(file.read_bytes()).hexdigest() != manifest.get(
                key
            ):
                raise RegistryError(f"Registered source artifact hash differs: {filename}")
            if json.loads(file.read_bytes()) != expected:
                raise RegistryError(f"Registered source artifact content differs: {filename}")
        source = Path(_text(manifest, "source_training_path"))
        run, snapshot, _ = _validated_source(source)
        _ = snapshot
        _verify_tracking(source, _text(manifest, "source_mlflow_run_id"), tracking)
        if (
            run["training_run_id"] != manifest.get("training_run_id")
            or run["dataset_snapshot_id"] != manifest.get("dataset_snapshot_id")
            or run["dataset_logical_fingerprint"] != manifest.get("dataset_logical_fingerprint")
        ):
            raise RegistryError("Registered source training/dataset provenance differs")
        evaluate_eligibility(manifest)
        return manifest
    except (OSError, ValueError, KeyError, TypeError) as error:
        if isinstance(error, RegistryError):
            raise
        raise RegistryError(f"Cannot inspect registered source: {error}") from error


def _verify_version(
    client: MlflowClient,
    config: RegistryConfig,
    version: str,
    *,
    candidate: Path | None = None,
    deep: bool = False,
    expected_status: str = "COMPLETE",
) -> dict[str, object]:
    model = _model(client, config.model_name)
    if model is None or model.tags != {
        "model_family": "logistic_regression",
        "governance_contract": GOVERNANCE_VERSION,
    }:
        raise RegistryError("Registered-model family metadata is missing or incompatible")
    if model.aliases:
        raise RegistryError("Deployment aliases are outside research-only governance")
    record = client.get_model_version(config.model_name, version)
    source = _source_path(record.source)
    if candidate is not None and source.resolve() != (candidate.resolve() / MODEL_DIR):
        raise RegistryError("Registry points to a different candidate artifact")
    manifest = _source_metadata(source.parent, config.tracking)
    claims = [
        item
        for item in _versions(client, config.model_name)
        if item.tags.get("candidate_id") == manifest["candidate_id"] or item.source == record.source
    ]
    if len(claims) != 1 or str(claims[0].version) != str(version):
        raise RegistryError(
            "Registry candidate/source identity has duplicate or conflicting claims"
        )
    expected = {**_tags(manifest), "registration_status": expected_status}
    if record.name != config.model_name or str(record.version) != str(version):
        raise RegistryError("Registry version identity differs")
    if record.status != "READY" or record.run_id is not None or record.aliases:
        raise RegistryError("Version is incomplete, aliased, or falsely claims a producing run")
    if record.tags != expected:
        raise RegistryError("Registry governance/provenance tags differ or are incomplete")
    if source.resolve() != (source.parent / MODEL_DIR).resolve():
        raise RegistryError("Registry source path differs")
    if deep:
        inspected = inspect_candidate(source.parent, tracking=config.tracking)
        if inspected != manifest:
            raise RegistryError("Deep candidate inspection differs from registry source")
        snapshot = Path(
            cast(
                str,
                inspect_run(Path(_text(manifest, "source_training_path")))["dataset_snapshot_path"],
            )
        )
        row = cast(dict[str, object], pq.read_table(snapshot / "dataset.parquet").to_pylist()[0])
        from sentinelaegisforge_control_plane.candidates.contract import FEATURES

        controlled = {name: row[name] for name in FEATURES}
        registered_score = score_candidate(source.parent, [controlled], tracking=config.tracking)[0]
    return {
        "registered_model_name": config.model_name,
        "model_version": str(record.version),
        "registry_status": record.status,
        "candidate_id": manifest["candidate_id"],
        "training_run_id": manifest["training_run_id"],
        "source_tracking_run_id": manifest["source_mlflow_run_id"],
        "dataset_snapshot_id": manifest["dataset_snapshot_id"],
        "source_uri": record.source,
        "model_tree_sha256": manifest["model_tree_sha256"],
        "artifact_integrity": "VERIFIED",
        "input_contract_integrity": "VERIFIED",
        "governance_state": GOVERNANCE_STATE,
        "production_eligible": False,
        "warning": "Synthetic-only evaluation; production fraud quality unproven",
        **({"offline_probability": registered_score, "positive_class": "FRAUD"} if deep else {}),
    }


def inspect_registered_candidate(
    config: RegistryConfig, version: str, *, deep: bool = False
) -> dict[str, object]:
    """Inspect an explicit numeric version; shallow mode never deserializes a model."""
    if not re.fullmatch(r"[1-9][0-9]*", version):
        raise RegistryError("An explicit positive numeric model version is required")
    record = _verify_version(_client(config), config, version, deep=deep)
    source = _source_path(cast(str, record["source_uri"]))
    eligibility = evaluate_eligibility(_source_metadata(source.parent, config.tracking))
    path = _receipt_path(config, eligibility.candidate_id)
    if not path.is_file() or path.read_bytes() != canonical_json(
        _receipt(config, record, eligibility)
    ):
        raise RegistryError("Verified registration receipt is missing or conflicting")
    return record


def compare_registered_candidates(config: RegistryConfig, versions: list[str]) -> dict[str, object]:
    if not versions:
        raise RegistryError("Specify at least one explicit model version")
    records = [inspect_registered_candidate(config, version) for version in versions]
    details: list[dict[str, object]] = []
    for record in records:
        source = _source_path(cast(str, record["source_uri"]))
        manifest = _source_metadata(source.parent, config.tracking)
        run = inspect_run(Path(_text(manifest, "source_training_path")))
        details.append(
            {
                **record,
                "input_contract_fingerprint": manifest["input_schema_fingerprint"],
                "estimator_parameters": run["estimator_parameters"],
                "split_boundaries": run["split_boundaries"],
                "validation_metrics": json.loads(
                    (Path(_text(manifest, "source_training_path")) / "metrics.json").read_bytes()
                )["validation"],
                "test_metrics": json.loads(
                    (Path(_text(manifest, "source_training_path")) / "metrics.json").read_bytes()
                )["test"],
            }
        )
    datasets = {record["dataset_snapshot_id"] for record in records}
    splits = {canonical_json(item["split_boundaries"]) for item in details}
    return {
        "versions": details,
        "warnings": [
            *(
                ["Different dataset snapshots: metrics are not directly comparable"]
                if len(datasets) > 1
                else []
            ),
            *(
                ["Different temporal evaluation periods: metrics are not directly comparable"]
                if len(splits) > 1
                else []
            ),
        ],
        "selection": "none; research-only comparison",
    }


def _receipt_path(config: RegistryConfig, candidate_id: str) -> Path:
    return config.receipts / config.backend_identity / config.model_name / f"{candidate_id}.json"


def _receipt(
    config: RegistryConfig, record: dict[str, object], eligibility: Eligibility
) -> dict[str, object]:
    evidence: dict[str, object] = {
        "registry_contract_version": REGISTRY_CONTRACT_VERSION,
        "registry_backend_identity": config.backend_identity,
        "registered_model_name": config.model_name,
        "model_version": record["model_version"],
        "candidate_id": record["candidate_id"],
        "training_run_id": record["training_run_id"],
        "source_tracking_run_id": record["source_tracking_run_id"],
        "dataset_snapshot_id": record["dataset_snapshot_id"],
        "model_source_uri": record["source_uri"],
        "source_model_tree_sha256": record["model_tree_sha256"],
        "governance_state": GOVERNANCE_STATE,
        "eligibility_result": asdict(eligibility),
        "registration_status": "COMPLETE",
    }
    evidence["receipt_sha256"] = hashlib.sha256(canonical_json(evidence)).hexdigest()
    return evidence


def _publish_receipt(path: Path, receipt: dict[str, object]) -> None:
    payload = canonical_json(receipt)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise RegistryError("Existing immutable receipt conflicts with registry evidence")
        return
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def register_candidate(
    candidate: Path, config: RegistryConfig = RegistryConfig()
) -> dict[str, object]:
    """Cooperating-writer serialized, fail-closed, sequentially idempotent registration."""
    manifest = inspect_candidate(candidate, tracking=config.tracking)
    eligibility = evaluate_eligibility(manifest)
    source = candidate.resolve() / MODEL_DIR
    if _source_metadata(source.parent, config.tracking) != manifest:
        raise RegistryError("Candidate changed between deep and source inspection")
    client = _client(config)
    lock_path = config.tracking.database.with_suffix(".registry.lock")
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            model = _model(client, config.model_name)
            if model is None:
                client.create_registered_model(
                    config.model_name,
                    tags={
                        "model_family": "logistic_regression",
                        "governance_contract": GOVERNANCE_VERSION,
                    },
                    description="Offline research candidates only; no deployment approval",
                )
            elif (
                model.tags
                != {
                    "model_family": "logistic_regression",
                    "governance_contract": GOVERNANCE_VERSION,
                }
                or model.aliases
            ):
                raise RegistryError("Registered model has incompatible family/governance metadata")
            claims = [
                record
                for record in _versions(client, config.model_name)
                if record.tags.get("candidate_id") == eligibility.candidate_id
                or record.source == source.as_uri()
            ]
            if len(claims) > 1:
                raise RegistryError("Multiple Registry versions claim the same candidate/source")
            if claims:
                record = _verify_version(
                    client, config, str(claims[0].version), candidate=candidate
                )
                prior_receipt = _receipt(config, record, eligibility)
                prior_path = _receipt_path(config, eligibility.candidate_id)
                if not prior_path.is_file() or prior_path.read_bytes() != canonical_json(
                    prior_receipt
                ):
                    raise RegistryError("Existing registration has no matching completed receipt")
            else:
                created = client.create_model_version(
                    config.model_name,
                    source.as_uri(),
                    run_id=None,  # Phase 15 run did not produce the Phase 16 model binary.
                    tags={**_tags(manifest), "registration_status": "REGISTRATION_INCOMPLETE"},
                    description="Research-only, synthetic-evaluation offline candidate",
                )
                # Verify the incomplete claim before committing the completion tag.
                _verify_version(
                    client,
                    config,
                    str(created.version),
                    candidate=candidate,
                    expected_status="REGISTRATION_INCOMPLETE",
                )
                client.set_model_version_tag(
                    config.model_name, str(created.version), "registration_status", "COMPLETE"
                )
                record = _verify_version(client, config, str(created.version), candidate=candidate)
            receipt = _receipt(config, record, eligibility)
            path = _receipt_path(config, eligibility.candidate_id)
            _publish_receipt(path, receipt)
            return {
                **record,
                "receipt_path": str(path),
                "receipt_sha256": receipt["receipt_sha256"],
            }
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
