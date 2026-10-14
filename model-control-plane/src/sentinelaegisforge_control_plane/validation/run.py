"""Verify existing model evidence and publish an immutable research-only dossier."""

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import cast

from mlflow.tracking import MlflowClient

from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.registry.run import (
    RegistryConfig,
    _source_path,
    inspect_registered_candidate,
)
from sentinelaegisforge_control_plane.synthetic.corpus import inspect_corpus
from sentinelaegisforge_control_plane.tracking.run import verify_evidence

from .policy import (
    LOGIC_VERSION,
    EvidenceFact,
    EvidenceState,
    ValidationPolicy,
    VerifiedFacts,
    evaluate,
)

ASSESSMENT_CONTRACT_VERSION = "model-validation-assessment-v1"
REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_OUTPUT_ROOT = REPOSITORY_ROOT / ".local/ml/model-validations"
ARTIFACTS = frozenset({"assessment.json", "evidence_manifest.json", "validation_report.md"})


class AssessmentError(ValueError):
    """Existing or upstream evidence is incomplete, corrupt or contradictory."""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise AssessmentError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise AssessmentError(f"{name} must be a nonempty string")
    return value


def _verified(explanation: str, *references: str) -> EvidenceFact:
    return EvidenceFact(EvidenceState.VERIFIED, explanation, references)


def _missing(explanation: str, *references: str) -> EvidenceFact:
    return EvidenceFact(EvidenceState.NOT_ESTABLISHED, explanation, references)


def _facts() -> VerifiedFacts:
    """Only the three technical categories have authoritative Phase 12–17 evidence."""
    return VerifiedFacts(
        _verified(
            "Deep Registry/candidate inspection verified bytes, runtime and source hash.",
            "registry:exact-version",
            "candidate:candidate_manifest.json",
        ),
        _verified(
            "Trusted reconstruction reproduced archived probabilities within tolerance.",
            "candidate:equivalence.json",
            "training:predictions.parquet",
        ),
        _verified(
            "Training, snapshot, labels, metrics and MLflow index were inspected.",
            "training:manifest.json",
            "snapshot:manifest.json",
            "mlflow:source-run",
        ),
        _missing(
            "Only SYNTHETIC_ label provenance is verified; no real fraud outcomes.",
            "snapshot:dataset.parquet[label_source]",
        ),
        _missing(
            "Split mechanics are verified; synthetic cohorts are not representative evidence.",
            "training:manifest.json[split_boundaries,cohort_support]",
            "training:metrics.json",
        ),
        _missing(
            "Synthetic discrimination metrics are not production fraud-quality evidence.",
            "training:metrics.json",
        ),
        _missing(
            "No representative probability calibration or calibration correction is verified.",
            "training:metrics.json",
        ),
        _missing(
            "No representative customer, currency or transaction-category analysis is verified.",
        ),
        _missing(
            "Serving, resource limits, failure handling and monitoring are unverified.",
        ),
        _missing(
            "No model-risk review, approval authority or production-use decision exists.",
        ),
    )


def _corpus_provenance(
    config: RegistryConfig,
    tracking_run_id: str,
    label_source_hash: str,
) -> dict[str, object]:
    """Recheck an explicitly tracked corpus; never infer an ID from a path name."""
    client = MlflowClient(tracking_uri=config.tracking.tracking_uri)
    record = client.get_run(tracking_run_id)
    corpus_id = record.data.tags.get("scenario_corpus_id")
    if corpus_id is None:
        return {}
    if not re.fullmatch(r"[0-9a-f]{64}", corpus_id):
        raise AssessmentError("Tracked scenario corpus identity is malformed")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(client.download_artifacts(tracking_run_id, "provenance.json", directory))
        content = path.read_bytes()
    provenance_hash = record.data.tags.get("tracking.provenance_sha256")
    if provenance_hash != _sha(content):
        raise AssessmentError("Tracked corpus provenance artifact hash differs")
    provenance = _object(json.loads(content), "tracking provenance")
    if provenance.get("scenario_corpus_id") != corpus_id:
        raise AssessmentError("Tracked corpus ID and provenance artifact differ")
    corpus_path = REPOSITORY_ROOT / ".local/ml/scenarios" / corpus_id
    corpus = inspect_corpus(corpus_path)
    if corpus.manifest["labels_sha256"] != label_source_hash:
        raise AssessmentError("Scenario labels differ from the Phase 12 snapshot label source")
    return {
        "scenario_corpus_id": corpus_id,
        "scenario_manifest_sha256": _sha((corpus_path / "manifest.tsv").read_bytes()),
        "scenario_plan_sha256": corpus.manifest["plan_sha256"],
        "scenario_truth_sha256": corpus.manifest["truth_sha256"],
        "tracking_provenance_sha256": provenance_hash,
    }


def _collect(
    config: RegistryConfig, version: str, policy: ValidationPolicy
) -> tuple[dict[str, object], dict[str, object]]:
    if not policy.version.strip():
        raise AssessmentError("Validation policy version must be explicit")
    registered = inspect_registered_candidate(config, version, deep=True)
    candidate = _source_path(_string(registered["source_uri"], "Registry source")).parent
    candidate_bytes = (candidate / "candidate_manifest.json").read_bytes()
    candidate_manifest = _object(json.loads(candidate_bytes), "candidate manifest")
    training = Path(_string(candidate_manifest["source_training_path"], "training path"))
    verified = verify_evidence(training)
    run = verified.manifest
    snapshot_path = Path(_string(run["dataset_snapshot_path"], "snapshot path"))
    snapshot_bytes = (snapshot_path / "manifest.json").read_bytes()
    snapshot = _object(json.loads(snapshot_bytes), "snapshot manifest")
    receipt = (
        config.receipts
        / config.backend_identity
        / config.model_name
        / f"{registered['candidate_id']}.json"
    )
    receipt_bytes = receipt.read_bytes()  # Phase 17 inspector already checked exact content.
    if candidate_manifest["candidate_id"] != registered["candidate_id"]:
        raise AssessmentError("Candidate and Registry identities differ")
    if run["training_run_id"] != registered["training_run_id"]:
        raise AssessmentError("Training and Registry identities differ")
    if snapshot["snapshot_id"] != registered["dataset_snapshot_id"]:
        raise AssessmentError("Dataset and Registry identities differ")

    source_hashes = {
        "registration_receipt_sha256": _sha(receipt_bytes),
        "candidate_manifest_sha256": _sha(candidate_bytes),
        "candidate_equivalence_sha256": _sha((candidate / "equivalence.json").read_bytes()),
        "training_manifest_sha256": _sha(verified.manifest_bytes),
        "training_metrics_sha256": _sha(verified.metrics_bytes),
        "training_predictions_sha256": _string(
            run["predictions_parquet_sha256"], "predictions hash"
        ),
        "snapshot_manifest_sha256": _sha(snapshot_bytes),
        "snapshot_parquet_sha256": _string(snapshot["dataset_parquet_sha256"], "dataset hash"),
        "label_source_sha256": _string(snapshot["label_source_sha256"], "label source hash"),
    }
    corpus = _corpus_provenance(
        config,
        _string(registered["source_tracking_run_id"], "tracking run ID"),
        source_hashes["label_source_sha256"],
    )
    if corpus:
        source_hashes.update(
            {key: cast(str, value) for key, value in corpus.items() if key.endswith("sha256")}
        )
    identity: dict[str, object] = {
        "assessment_contract_version": ASSESSMENT_CONTRACT_VERSION,
        "assessment_logic_version": LOGIC_VERSION,
        "policy_version": policy.version,
        "policy_fingerprint": policy.fingerprint,
        "registry_backend_identity": config.backend_identity,
        "registered_model_name": config.model_name,
        "model_version": version,
        "candidate_id": registered["candidate_id"],
        "model_tree_sha256": registered["model_tree_sha256"],
        "source_tracking_run_id": registered["source_tracking_run_id"],
        "training_run_id": run["training_run_id"],
        "dataset_snapshot_id": snapshot["snapshot_id"],
        "dataset_logical_fingerprint": snapshot["logical_dataset_fingerprint"],
        "source_schema_fingerprint": run["source_schema_fingerprint"],
        "source_delta_version": run["source_delta_version"],
        "source_hashes": source_hashes,
        "scenario_corpus_id": corpus.get("scenario_corpus_id"),
    }
    assessment_id = _sha(canonical_json(identity))
    result = evaluate(_facts(), policy)
    assessment: dict[str, object] = {
        "assessment_id": assessment_id,
        "identity": identity,
        "registered_model_name": config.model_name,
        "model_version": version,
        "candidate_id": registered["candidate_id"],
        "policy_version": policy.version,
        "policy_fingerprint": policy.fingerprint,
        "evidence": {
            "artifact_integrity": "VERIFIED",
            "prediction_equivalence": candidate_manifest["comparison"],
            "temporal_split_mechanics": "VERIFIED",
            "representative_out_of_time_performance": "NOT_ESTABLISHED",
            "data_origin": "synthetic",
            "evaluation_scope": "integration_verification",
            "rule_decisions_used_as_labels": False,
            "label_truth_boundary": (
                "Verified Phase 14 private truth and delayed labels; no Phase 11 decisions"
                if corpus
                else "Verified SYNTHETIC_ label provenance; Phase 11 decisions are not label inputs"
            ),
            "scenario_corpus_id": corpus.get("scenario_corpus_id"),
            "dataset_contract_version": run["dataset_contract_version"],
            "training_contract_version": run["training_contract_version"],
            "split_boundaries": run["split_boundaries"],
            "minimum_rows": run["minimum_rows"],
            "cohort_support": run["cohort_support"],
            "train_period_rows": run["train_period_rows"],
            "excluded_from_fit_due_to_label_delay": run["excluded_from_fit_due_to_label_delay"],
            "source_delta_version": run["source_delta_version"],
            "dataset_as_of_time": run["dataset_as_of_time"],
            "validation_metrics": verified.metrics["validation"],
            "test_metrics": verified.metrics["test"],
            "source_hashes": source_hashes,
        },
        **result,
    }
    manifest: dict[str, object] = {
        "assessment_contract_version": ASSESSMENT_CONTRACT_VERSION,
        "assessment_id": assessment_id,
        "identity": identity,
        "evidence_references": {
            "registry_source_uri": registered["source_uri"],
            "candidate_manifest": str(candidate / "candidate_manifest.json"),
            "candidate_equivalence": str(candidate / "equivalence.json"),
            "training_manifest": str(training / "manifest.json"),
            "training_metrics": str(training / "metrics.json"),
            "training_predictions": str(training / "predictions.parquet"),
            "snapshot_manifest": str(snapshot_path / "manifest.json"),
            "snapshot_dataset": str(snapshot_path / "dataset.parquet"),
            "registration_receipt": str(receipt),
            "tracking_uri": config.tracking.tracking_uri,
        },
    }
    return assessment, manifest


def _report(assessment: dict[str, object]) -> bytes:
    identity = _object(assessment["identity"], "identity")
    evidence = _object(assessment["evidence"], "evidence")
    lines = [
        "# Model validation evidence dossier",
        "",
        f"Assessment: `{assessment['assessment_id']}`",
        f"Policy: `{assessment['policy_version']}` (`{assessment['policy_fingerprint']}`)",
        f"Registry: `{assessment['registered_model_name']}` "
        f"version `{assessment['model_version']}`",
        f"Candidate: `{assessment['candidate_id']}`; training run: `{identity['training_run_id']}`",
        f"Dataset snapshot: `{identity['dataset_snapshot_id']}`; "
        f"Delta version: `{identity['source_delta_version']}`",
        "",
        "**NOT ELIGIBLE FOR PROMOTION.** Not an approval or regulatory certification.",
        "Labels and metrics are synthetic integration evidence, not production evidence.",
        f"Label boundary: {evidence['label_truth_boundary']}.",
        "",
        "## Verified mechanics and cohort support",
        "",
        "Model bytes, archived predictions, lineage and split mechanics were inspected.",
        f"Split boundaries: `{json.dumps(evidence['split_boundaries'], sort_keys=True)}`.",
        f"Cohort support: `{json.dumps(evidence['cohort_support'], sort_keys=True)}`.",
        f"Training-period rows: {evidence['train_period_rows']}; "
        f"delayed/revised-label exclusions: {evidence['excluded_from_fit_due_to_label_delay']}.",
        "",
        "## Synthetic Scenario Evaluation — Not Production Fraud Performance",
        "",
    ]
    for split in ("validation", "test"):
        metrics = _object(evidence[f"{split}_metrics"], f"{split} metrics")
        lines.extend(
            [
                f"### {split.title()}",
                "",
                f"Rows {metrics['rows']} (FRAUD {metrics['positives']}, "
                f"LEGIT {metrics['negatives']}); prevalence {metrics['positive_prevalence']}.",
                f"Average Precision {metrics['average_precision']}; ROC AUC {metrics['roc_auc']}.",
                f"At 0.5: precision {metrics['precision_at_0_5']}, "
                f"recall {metrics['recall_at_0_5']}, F1 {metrics['f1_at_0_5']}; "
                f"confusion {json.dumps(metrics['confusion_matrix'], sort_keys=True)}.",
                "Average Precision versus prevalence is descriptive, not an acceptance threshold.",
                "",
            ]
        )
    lines.extend(["## Requirements and blocking evidence", ""])
    for raw in cast(list[dict[str, object]], assessment["requirements"]):
        lines.append(
            f"- `{raw['requirement_id']}` — **{raw['status']}**: {raw['explanation']} "
            "References: "
            f"{', '.join(cast(list[str], raw['evidence_references'])) or 'none established'}."
        )
    lines.extend(
        [
            "",
            "Missing evidence is NOT_ESTABLISHED, not a pass. "
            "Source corruption fails verification before publication.",
            "The Registry remains research-only; no serving, approval, "
            "shadow/canary or promotion was performed.",
        ]
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _payloads(assessment: dict[str, object], manifest: dict[str, object]) -> dict[str, bytes]:
    assessment_bytes = canonical_json(assessment)
    report_bytes = _report(assessment)
    manifest = {
        **manifest,
        "assessment_sha256": _sha(assessment_bytes),
        "report_sha256": _sha(report_bytes),
    }
    return {
        "assessment.json": assessment_bytes,
        "evidence_manifest.json": canonical_json(manifest),
        "validation_report.md": report_bytes,
    }


def _check_directory(path: Path, expected: dict[str, bytes]) -> None:
    if path.is_symlink() or not path.is_dir():
        raise AssessmentError(f"Assessment directory is missing or linked: {path}")
    if {item.name for item in path.iterdir()} != ARTIFACTS:
        raise AssessmentError("Assessment dossier is incomplete or has unexpected artifacts")
    for name, payload in expected.items():
        file = path / name
        if file.is_symlink() or file.read_bytes() != payload:
            raise AssessmentError(f"Assessment artifact is corrupt/conflicting: {name}")


def assess_model(
    config: RegistryConfig,
    version: str,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    policy: ValidationPolicy = ValidationPolicy(),
) -> Path:
    """Publish once, or verify exact bytes on repeat; never mutate upstream artifacts."""
    assessment, manifest = _collect(config, version, policy)
    payloads = _payloads(assessment, manifest)
    if not output_root.is_absolute():
        raise AssessmentError("Assessment output root must be absolute")
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / cast(str, assessment["assessment_id"])
    try:
        target.mkdir()
    except FileExistsError:
        _check_directory(target, payloads)
        return target
    for name in ("assessment.json", "validation_report.md", "evidence_manifest.json"):
        file = target / name
        descriptor = os.open(file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payloads[name])
            stream.flush()
            os.fsync(stream.fileno())
    _check_directory(target, payloads)
    return target


def inspect_assessment(
    path: Path, config: RegistryConfig, policy: ValidationPolicy = ValidationPolicy()
) -> dict[str, object]:
    """Read-only inspection includes fresh upstream verification and exact dossier read-back."""
    if (
        path.is_symlink()
        or not path.is_dir()
        or {item.name for item in path.iterdir()} != ARTIFACTS
    ):
        raise AssessmentError("Assessment dossier is missing, linked or incomplete")
    raw = _object(json.loads((path / "assessment.json").read_bytes()), "assessment")
    identity = _object(raw.get("identity"), "assessment identity")
    if path.name != _sha(canonical_json(identity)) or raw.get("assessment_id") != path.name:
        raise AssessmentError("Assessment identity does not match canonical inputs")
    version = _string(identity.get("model_version"), "model version")
    assessment, manifest = _collect(config, version, policy)
    _check_directory(path, _payloads(assessment, manifest))
    return assessment


def compare_assessments(
    paths: list[Path], config: RegistryConfig, policy: ValidationPolicy = ValidationPolicy()
) -> dict[str, object]:
    if not paths:
        raise AssessmentError("Specify at least one assessment path")
    assessments = [inspect_assessment(path, config, policy) for path in paths]
    identities = [_object(item["identity"], "identity") for item in assessments]
    evidences = [_object(item["evidence"], "evidence") for item in assessments]
    warnings = ["Synthetic-only metrics are not production model-selection evidence"]
    if len({item["dataset_snapshot_id"] for item in identities}) > 1:
        warnings.append("Dataset snapshots differ; metric comparisons need population review")
    if len({canonical_json(item["split_boundaries"]) for item in evidences}) > 1:
        warnings.append("Temporal evaluation periods differ")
    return {
        "assessments": [
            {
                "assessment_id": item["assessment_id"],
                "registered_model_name": item["registered_model_name"],
                "model_version": item["model_version"],
                "candidate_id": item["candidate_id"],
                "policy_fingerprint": item["policy_fingerprint"],
                "dataset_snapshot_id": identity["dataset_snapshot_id"],
                "split_boundaries": evidence["split_boundaries"],
                "validation_metrics": evidence["validation_metrics"],
                "test_metrics": evidence["test_metrics"],
                "promotion_readiness": item["promotion_readiness"],
                "requirement_statuses": [
                    row["status"] for row in cast(list[dict[str, object]], item["requirements"])
                ],
            }
            for item, identity, evidence in zip(assessments, identities, evidences, strict=True)
        ],
        "warnings": warnings,
        "selection": "none; read-only research comparison",
    }
