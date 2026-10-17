"""Immutable, evidence-first offline monitoring investigations.

This module only reads verified Phase 18–20 artifacts. Measurements remain
descriptive; finding order is presentation order, not incident severity.
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.registry.run import RegistryConfig
from sentinelaegisforge_control_plane.validation.run import inspect_assessment

from .contract import MonitoringError, artifact_directory, identity, object_value, sha256
from .drift import CATEGORICAL, NUMERIC, inspect_drift_snapshot
from .ledger import ROOT, inspect_ledger
from .outcomes import compare_snapshots, inspect_monitoring_snapshot

INVESTIGATION_VERSION = "offline-monitoring-investigation-v1"
RUNBOOK_VERSION = "offline-monitoring-runbook-v1"
LOGIC_VERSION = "evidence-first-diagnostics-v1.1"
DEFAULT_INVESTIGATION_ROOT = ROOT / ".local/ml/investigations"
FILES = {"investigation_manifest.json", "findings.json", "investigation_report.md", "runbook.md"}
STATISTICAL_NULLS = ("prior_amount_mean", "prior_amount_stddev", "amount_zscore")


@dataclass(frozen=True)
class EvidenceReference:
    source_id: str
    artifact: str
    contract_version: str
    field_path: str


@dataclass(frozen=True)
class DiagnosticFinding:
    finding_id: str
    category: str
    title: str
    observation: str
    measurement_context: dict[str, object]
    evidence_references: tuple[EvidenceReference, ...]
    limitations: str
    investigation_steps: tuple[str, ...]


def _reference(source_id: str, artifact: str, contract: str, path: str) -> EvidenceReference:
    return EvidenceReference(source_id, artifact, contract, path)


def _field(document: dict[str, object], path: str) -> object:
    current: object = document
    for part in path.split("."):
        current = object_value(current, path)[part]
    return current


def _source_hashes(path: Path, names: tuple[str, ...]) -> dict[str, str]:
    return {name: sha256((path / name).read_bytes()) for name in names}


def _load_evidence(
    drift_path: Path,
    ledger_path: Path | None,
    earlier_path: Path | None,
    later_path: Path | None,
    dossier_path: Path | None,
    registry: RegistryConfig | None,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    if (earlier_path is None) != (later_path is None):
        raise MonitoringError("Supply both ordered Phase 19 snapshots or neither")
    if dossier_path is not None and registry is None:
        raise MonitoringError("Dossier inspection requires an explicit Registry configuration")
    if drift_path.is_symlink() or (ledger_path is not None and ledger_path.is_symlink()):
        raise MonitoringError("Investigation sources may not be symlinks")
    drift_path = drift_path.resolve()
    drift = inspect_drift_snapshot(drift_path)
    recorded_ledger = Path(cast(str, drift["ledger_path"]))
    ledger_path = recorded_ledger if ledger_path is None else ledger_path.resolve()
    ledger = inspect_ledger(ledger_path)
    if (
        ledger_path != recorded_ledger
        or ledger["ledger_id"] != drift["ledger_id"]
        or sha256((ledger_path / "manifest.json").read_bytes()) != drift["ledger_manifest_sha256"]
        or ledger["predictions_parquet_sha256"] != drift["ledger_predictions_sha256"]
    ):
        raise MonitoringError("Explicit ledger differs from the verified drift source")
    source: dict[str, object] = {
        "drift": {
            "path": str(drift_path),
            "id": drift["drift_snapshot_id"],
            "contract_version": drift["drift_contract_version"],
            "hashes": _source_hashes(
                drift_path, ("drift_manifest.json", "drift_metrics.json", "drift_report.md")
            ),
        },
        "ledger": {
            "path": str(ledger_path),
            "id": ledger["ledger_id"],
            "contract_version": ledger["ledger_contract_version"],
            "hashes": _source_hashes(ledger_path, ("manifest.json", "predictions.parquet")),
        },
    }
    metrics = object_value(json.loads((drift_path / "drift_metrics.json").read_bytes()), "metrics")
    context: dict[str, object] = {"drift": drift, "ledger": ledger, "metrics": metrics}
    if earlier_path is not None and later_path is not None:
        if earlier_path.is_symlink() or later_path.is_symlink():
            raise MonitoringError("Monitoring snapshots may not be symlinks")
        earlier_path, later_path = earlier_path.resolve(), later_path.resolve()
        earlier = inspect_monitoring_snapshot(earlier_path)
        later = inspect_monitoring_snapshot(later_path)
        for snapshot in (earlier, later):
            if (
                any(
                    snapshot[key] != expected
                    for key, expected in (
                        ("ledger_id", ledger["ledger_id"]),
                        ("candidate_id", drift["candidate_id"]),
                        ("registered_model_name", drift["registered_model_name"]),
                        ("registered_model_version", drift["registered_model_version"]),
                        ("source_delta_table_id", drift["source_delta_table_id"]),
                        ("source_delta_version", drift["source_delta_version"]),
                    )
                )
                or Path(cast(str, snapshot["ledger_path"])) != ledger_path
            ):
                raise MonitoringError("Phase 19 snapshot identity is incompatible with drift")
        comparison = compare_snapshots(earlier_path, later_path)
        for name, path, snapshot in (
            ("earlier", earlier_path, earlier),
            ("later", later_path, later),
        ):
            source[name] = {
                "path": str(path),
                "id": snapshot["monitoring_snapshot_id"],
                "contract_version": snapshot["monitoring_contract_version"],
                "hashes": _source_hashes(
                    path,
                    (
                        "monitoring_manifest.json",
                        "quality_metrics.json",
                        "segment_metrics.json",
                        "coverage_report.json",
                        "selected_outcomes.parquet",
                        "report.txt",
                    ),
                ),
            }
        context.update(earlier=earlier, later=later, comparison=comparison)
    if dossier_path is not None:
        if dossier_path.is_symlink():
            raise MonitoringError("Assessment dossier may not be a symlink")
        dossier_path = dossier_path.resolve()
        assert registry is not None
        dossier = inspect_assessment(dossier_path, registry)
        dossier_identity = object_value(dossier["identity"], "dossier identity")
        if (
            dossier["candidate_id"] != drift["candidate_id"]
            or dossier["registered_model_name"] != drift["registered_model_name"]
            or dossier["model_version"] != drift["registered_model_version"]
            or dossier_identity["source_delta_version"] != drift["source_delta_version"]
            or dossier_identity["source_schema_fingerprint"] != drift["source_schema_fingerprint"]
            or dossier["promotion_readiness"] != "NOT_ELIGIBLE_FOR_PROMOTION"
            or dossier["production_eligible"] is not False
        ):
            raise MonitoringError("Phase 18 dossier is incompatible or not research-only")
        source["dossier"] = {
            "path": str(dossier_path),
            "id": dossier["assessment_id"],
            "contract_version": dossier_identity["assessment_contract_version"],
            "hashes": _source_hashes(
                dossier_path,
                ("assessment.json", "evidence_manifest.json", "validation_report.md"),
            ),
        }
        context["dossier"] = dossier
    return source, context, metrics


def _findings(context: dict[str, object], metrics: dict[str, object]) -> list[dict[str, object]]:
    findings: list[DiagnosticFinding] = []

    def add(
        finding_id: str,
        category: str,
        title: str,
        observation: str,
        values: dict[str, object],
        paths: tuple[str, ...],
        limitation: str,
        steps: tuple[str, ...],
        *,
        source: str = "drift",
        artifact: str = "drift_metrics.json",
        extra_references: tuple[EvidenceReference, ...] = (),
    ) -> None:
        document = (
            metrics
            if source == "drift" and artifact == "drift_metrics.json"
            else object_value(context[source], source)
        )
        for path in paths:
            _field(document, path)  # Reject invented evidence paths before publication.
        source_info = object_value(object_value(context["sources"], "sources")[source], source)
        findings.append(
            DiagnosticFinding(
                finding_id,
                category,
                title,
                observation,
                values,
                tuple(
                    _reference(
                        cast(str, source_info["id"]),
                        artifact,
                        cast(str, source_info["contract_version"]),
                        path,
                    )
                    for path in paths
                )
                + extra_references,
                limitation,
                steps,
            )
        )

    numeric = object_value(metrics["numeric"], "numeric")
    for name in STATISTICAL_NULLS:
        item = object_value(numeric[name], name)
        left, right = (
            object_value(item["reference"], "reference"),
            object_value(item["comparison"], "comparison"),
        )
        add(
            f"MISSINGNESS_{name.upper()}",
            "MISSINGNESS",
            f"{name} availability",
            f"Missing fraction {left['missing_fraction']} → {right['missing_fraction']}; "
            f"null rows {left['null']} → {right['null']}.",
            {
                "reference": left["missing_fraction"],
                "comparison": right["missing_fraction"],
                "difference": item["missing_fraction_difference"],
            },
            (
                f"numeric.{name}.reference.missing_fraction",
                f"numeric.{name}.comparison.missing_fraction",
                f"numeric.{name}.missing_fraction_difference",
            ),
            "Missing history has multiple possible causes; this measurement identifies none.",
            (
                "Inspect per-currency prior observation histories and status composition.",
                "Check customer mix, state expiration/restart and event-time "
                "versus observed order.",
            ),
        )
    for name in NUMERIC:
        item = object_value(numeric[name], name)
        left, right = (
            object_value(item["reference"], "reference"),
            object_value(item["comparison"], "comparison"),
        )
        ks, psi = object_value(item["ks_d"], "KS"), object_value(item["psi"], "PSI")
        add(
            f"NUMERIC_{name.upper()}",
            "FEATURE_DISTRIBUTION",
            f"{name} distribution",
            f"Mean {left['mean']} → {right['mean']}; KS D {ks['value']} "
            f"({ks['reason']}); PSI {psi['value']} ({psi['reason']}).",
            {
                "reference_mean": left["mean"],
                "comparison_mean": right["mean"],
                "ks_d": ks["value"],
                "ks_status": ks["status"],
                "ks_reason": ks["reason"],
                "psi": psi["value"],
                "psi_status": psi["status"],
                "psi_reason": psi["reason"],
            },
            (
                f"numeric.{name}.reference.mean",
                f"numeric.{name}.comparison.mean",
                f"numeric.{name}.ks_d",
                f"numeric.{name}.psi",
            ),
            "Descriptive distribution difference, not a calibrated alert or causal conclusion.",
            (
                "Review cohort composition and source feature semantics "
                "before forming a hypothesis.",
            ),
        )
    categories = object_value(metrics["categorical"], "categorical")
    for name in CATEGORICAL:
        item = object_value(categories[name], name)
        add(
            f"CATEGORY_{name.upper()}",
            "POPULATION_COMPOSITION",
            f"{name} composition",
            f"TVD {item['tvd']}; JS divergence {item['js_divergence_base2']}; "
            f"new {item['new_categories']}; disappeared {item['disappeared_categories']}.",
            {
                key: item[key]
                for key in (
                    "reference_counts",
                    "comparison_counts",
                    "tvd",
                    "js_divergence_base2",
                    "new_categories",
                    "disappeared_categories",
                    "status",
                    "reason",
                )
            },
            (
                f"categorical.{name}.reference_counts",
                f"categorical.{name}.comparison_counts",
                f"categorical.{name}.tvd",
                f"categorical.{name}.js_divergence_base2",
                f"categorical.{name}.new_categories",
                f"categorical.{name}.disappeared_categories",
            ),
            "Category movement alone does not establish prediction error; "
            "unknown categories are encoded without failure.",
            ("Inspect category mix and corresponding cohort support.",),
        )
    prediction = object_value(metrics["prediction"], "prediction")
    left, right = (
        object_value(prediction["reference"], "reference"),
        object_value(prediction["comparison"], "comparison"),
    )
    rate = object_value(prediction["fraction_at_or_above_0_5"], "score rate")
    add(
        "FROZEN_SCORE_DISTRIBUTION",
        "PREDICTION_DISTRIBUTION",
        "Frozen score distribution",
        f"Mean {left['mean']} → {right['mean']}; median {left['median']} → {right['median']}; "
        f"score ≥ 0.5 share {rate['reference']} → {rate['comparison']}.",
        {
            "reference_mean": left["mean"],
            "comparison_mean": right["mean"],
            "reference_median": left["median"],
            "comparison_median": right["median"],
            "ks_d": prediction["ks_d"],
            "psi": prediction["psi"],
            "threshold_share": rate,
        },
        (
            "prediction.reference.mean",
            "prediction.comparison.mean",
            "prediction.reference.median",
            "prediction.comparison.median",
            "prediction.ks_d",
            "prediction.psi",
            "prediction.fraction_at_or_above_0_5",
        ),
        "Scores are uncalibrated; threshold share is not fraud prevalence "
        "or proof of worsening discrimination.",
        ("Inspect feature-population changes before hypothesizing model behavior.",),
    )
    strata = object_value(metrics["currency_strata"], "currency strata")
    for currency in sorted(strata):
        group = object_value(strata[currency], currency)
        for name in cast(dict[str, object], group["features"]):
            item = object_value(cast(dict[str, object], group["features"])[name], name)
            a, b = (
                object_value(item["reference"], "reference"),
                object_value(item["comparison"], "comparison"),
            )
            add(
                f"CURRENCY_{currency}_{name.upper()}",
                "CURRENCY_STRATIFICATION",
                f"{currency} {name}",
                f"{currency}: {group['reference_rows']} → {group['comparison_rows']} rows; "
                f"mean {a['mean']} → {b['mean']}; "
                f"KS D {cast(dict[str, object], item['ks_d'])['value']}.",
                {
                    "reference_rows": group["reference_rows"],
                    "comparison_rows": group["comparison_rows"],
                    "reference_mean": a["mean"],
                    "comparison_mean": b["mean"],
                    "ks_d": item["ks_d"],
                    "psi": item["psi"],
                },
                (
                    f"currency_strata.{currency}.reference_rows",
                    f"currency_strata.{currency}.comparison_rows",
                    f"currency_strata.{currency}.features.{name}.reference.mean",
                    f"currency_strata.{currency}.features.{name}.comparison.mean",
                    f"currency_strata.{currency}.features.{name}.ks_d",
                    f"currency_strata.{currency}.features.{name}.psi",
                ),
                "Same-currency descriptive comparison; no FX normalization "
                "or cross-currency financial total.",
                ("Inspect per-currency population and missing-history composition.",),
            )
    if "earlier" in context:
        earlier, later = (
            object_value(context["earlier"], "earlier"),
            object_value(context["later"], "later"),
        )
        comparison = object_value(context["comparison"], "comparison")
        for name, snapshot in (("earlier", earlier), ("later", later)):
            coverage = object_value(snapshot["coverage"], "coverage")
            quality = object_value(snapshot["quality"], "quality")
            add(
                f"LABEL_COVERAGE_{name.upper()}",
                "LABEL_COVERAGE",
                f"{name.title()} AS_OF label coverage",
                f"AS_OF {snapshot['evaluation_as_of']}: scored {coverage['scored_events']}, "
                f"labeled {coverage['labeled_events']}, unlabeled {coverage['unlabeled_events']}, "
                f"coverage {coverage['label_coverage']}; AP {quality['average_precision']}, "
                f"ROC AUC {quality['roc_auc']} (support {quality['rows']}).",
                {
                    "evaluation_as_of": snapshot["evaluation_as_of"],
                    "coverage": coverage,
                    "quality": quality,
                },
                (
                    "coverage.scored_events",
                    "coverage.labeled_events",
                    "coverage.unlabeled_events",
                    "coverage.label_coverage",
                    "quality.rows",
                    "quality.average_precision",
                    "quality.roc_auc",
                ),
                "Quality is conditional on currently known labels and support; "
                "absent metrics remain unavailable.",
                ("Inspect label-observation cutoff, source and support before comparing quality.",),
                source=name,
                artifact="monitoring_manifest.json",
            )
        earlier_info = object_value(
            object_value(context["sources"], "sources")["earlier"], "earlier"
        )
        _field(earlier, "outcome_rows_fingerprint")
        _field(earlier, "evaluation_as_of")
        add(
            "LABEL_REVISION_COMPARISON",
            "LABEL_REVISION",
            "Delayed outcome changes",
            f"Newly labeled {comparison['newly_labeled_events']}; changed revisions "
            f"{comparison['changed_label_revisions']}; flips {comparison['outcome_flips']}; "
            f"unchanged labels {comparison['unchanged_labels']}.",
            comparison,
            ("outcome_rows_fingerprint", "evaluation_as_of"),
            "Predictions did not change. AP/ROC movement can reflect new or revised "
            "labels and nonrandom maturation.",
            (
                "Inspect selected revision histories and label availability "
                "separately from input drift.",
            ),
            source="later",
            artifact="monitoring_manifest.json",
            extra_references=(
                _reference(
                    cast(str, earlier_info["id"]),
                    "monitoring_manifest.json",
                    cast(str, earlier["monitoring_contract_version"]),
                    "outcome_rows_fingerprint",
                ),
                _reference(
                    cast(str, earlier_info["id"]),
                    "monitoring_manifest.json",
                    cast(str, earlier["monitoring_contract_version"]),
                    "evaluation_as_of",
                ),
            ),
        )
    else:
        add(
            "LABEL_EVIDENCE_ABSENT",
            "EVIDENCE_GAP",
            "No delayed-outcome comparison supplied",
            "Only label-free distribution evidence was supplied.",
            {},
            ("drift_snapshot_id",),
            "Fraud quality and label-maturity movement cannot be inferred.",
            ("Supply two compatible Phase 19 snapshots for point-in-time outcome context.",),
            artifact="drift_manifest.json",
            source="drift",
        )
    if "dossier" in context:
        dossier = object_value(context["dossier"], "dossier")
        add(
            "GOVERNANCE_RESEARCH_ONLY",
            "MODEL_QUALITY_CONTEXT",
            "Promotion ineligibility",
            f"Verified Phase 18 dossier remains {dossier['promotion_readiness']}; "
            f"missing evidence {dossier['missing_evidence']}.",
            {
                "technical_integrity_verified": dossier["technical_integrity_verified"],
                "promotion_readiness": dossier["promotion_readiness"],
                "missing_evidence": dossier["missing_evidence"],
            },
            ("promotion_readiness", "technical_integrity_verified", "missing_evidence"),
            "Synthetic replay does not establish real labels, calibration, "
            "operations or business approval.",
            ("Read the unchanged Phase 18 validation report before any governance discussion.",),
            source="dossier",
            artifact="assessment.json",
        )
    else:
        add(
            "GOVERNANCE_DOSSIER_ABSENT",
            "EVIDENCE_GAP",
            "No governance dossier supplied",
            "Phase 18 readiness evidence was not supplied to this packet.",
            {},
            ("candidate_id", "registered_model_version"),
            "Absence is not production eligibility; Registry status remains research-only.",
            (
                "Supply and verify the exact-version Phase 18 dossier "
                "if governance context is needed.",
            ),
            source="drift",
            artifact="drift_manifest.json",
        )
    return [asdict(item) for item in findings]


def _runbook() -> bytes:
    return b"""# Offline monitoring investigation runbook v1

This is a manual, synthetic-only evidence workflow. Order is for readability, not severity.

1. Verify the immutable packet, source hashes, candidate/Registry identity, pinned Delta
   lineage and input contract. Missing or corrupt sources are integrity failures; stop.
2. Confirm the two disjoint ingestion-time populations, row counts and event-time versus
   ingestion-time meaning. Do not interpret them as label-observation periods.
3. Inspect null fractions for prior_amount_mean, prior_amount_stddev and amount_zscore
   alongside statistical_feature_status. Review per-currency prior history, new/recurring
   customer mix if available, Phase 10 status semantics, state expiration/restart and
   event-time versus observed order. None is an asserted cause.
4. Inspect currency, country, transaction type and status proportions, including new or
   disappeared categories. The fitted encoder tolerates unknown categories; appearance
   alone is not an error.
5. Review all numeric means, KS D and PSI with their availability reasons. Unavailable
   support is not an integrity failure or a zero distance.
6. Compare raw monetary magnitudes only within currency. Overall means can conceal
   composition changes; do not sum USD and EUR or infer FX-normalized totals.
7. Review frozen score mean/median, KS D, PSI and score-at-least-0.5 share. Probabilities
   are uncalibrated; threshold share is not fraud prevalence. Examine input mix first.
8. If both Phase 19 cutoffs are supplied, inspect new labels, revisions, flips, unchanged
   outcomes, support and coverage. These are the same frozen predictions at different
   label-observation AS_OF times. Label selection can be nonrandom; AP/ROC changes do not
   prove changed discrimination.
9. Consult the verified Phase 18 dossier if supplied. Technical integrity is not real-world
   quality, calibration, operational validation or business approval; the model remains
   NOT_ELIGIBLE_FOR_PROMOTION.
10. Record further evidence required before asserting a cause or action. No alert,
    incident, retraining, promotion or deployment is triggered here.
"""


def _report(manifest: dict[str, object], findings: list[dict[str, object]]) -> bytes:
    sources = object_value(manifest["sources"], "sources")
    drift = object_value(manifest["drift_context"], "drift context")
    lines = [
        "# Offline monitoring investigation — synthetic historical replay",
        "",
        "Observed measurements and manual investigation avenues only. No severity, "
        "root cause, production-quality or action decision is assigned.",
        "",
        f"Investigation `{manifest['investigation_id']}`; candidate `{drift['candidate_id']}`; "
        f"Registry `{drift['registered_model_name']}` v{drift['registered_model_version']}.",
        f"Ledger `{object_value(sources['ledger'], 'ledger')['id']}`; Delta "
        f"`{drift['source_delta_table_id']}` version {drift['source_delta_version']}.",
        f"Ingestion-time reference {drift['reference_window']} ({drift['reference_rows']} rows); "
        f"comparison {drift['comparison_window']} ({drift['comparison_rows']} rows).",
        "",
        "## Verified evidence and limitations",
    ]
    for name in ("drift", "ledger", "earlier", "later", "dossier"):
        if name in sources:
            item = object_value(sources[name], name)
            lines.append(f"- {name}: `{item['id']}` at `{item['path']}` (verified source hashes).")
    if "earlier" in sources:
        lines.append(
            "- Phase 19 compares the *same frozen predictions* at two label-observation "
            "AS_OF cutoffs; Phase 20 compares *different ingestion-time populations*. "
            "Their changes are not one causal progression."
        )
    lines.extend(["", "## Findings — observed / investigate / not established", ""])
    for finding in findings:
        lines.extend(
            [
                f"### {finding['finding_id']} — {finding['title']}",
                f"Category: `{finding['category']}`. Observed: {finding['observation']}",
                "Evidence: "
                + "; ".join(
                    f"`{ref['source_id']}/{ref['artifact']}:{ref['field_path']}` "
                    f"({ref['contract_version']})"
                    for ref in cast(list[dict[str, str]], finding["evidence_references"])
                ),
                "Investigate: " + " ".join(cast(list[str], finding["investigation_steps"])),
                f"Not established: {finding['limitations']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Manual checklist",
            "",
            _runbook().decode("utf-8"),
            "## Governance boundary",
            "",
            "Research-only Registry entry; Phase 18 remains NOT_ELIGIBLE_FOR_PROMOTION "
            "when verified. No production fraud-performance claim, alert, incident or "
            "automated action follows.",
            "",
        ]
    )
    return "\n".join(lines).encode("utf-8")


def _prepare(
    drift_path: Path,
    ledger_path: Path | None,
    earlier_path: Path | None,
    later_path: Path | None,
    dossier_path: Path | None,
    registry: RegistryConfig | None,
) -> tuple[dict[str, object], dict[str, bytes]]:
    sources, context, metrics = _load_evidence(
        drift_path, ledger_path, earlier_path, later_path, dossier_path, registry
    )
    context["sources"] = sources
    basis: dict[str, object] = {
        "investigation_contract_version": INVESTIGATION_VERSION,
        "runbook_contract_version": RUNBOOK_VERSION,
        "diagnostic_logic_version": LOGIC_VERSION,
        "sources": sources,
        "report_configuration": {
            "order": (
                "missingness,numeric-contract-order,categorical-schema-order,"
                "prediction,currency-alphabetical,labels,governance"
            ),
            "mode": "combined" if "earlier" in sources else "distribution-only",
            "all_available_metrics": True,
        },
    }
    investigation_id = identity(basis)
    drift = object_value(context["drift"], "drift")
    manifest: dict[str, object] = {
        **basis,
        "investigation_id": investigation_id,
        "drift_context": {
            key: drift[key]
            for key in (
                "candidate_id",
                "registered_model_name",
                "registered_model_version",
                "source_delta_table_id",
                "source_delta_version",
                "source_schema_fingerprint",
                "input_contract_fingerprint",
                "reference_window",
                "comparison_window",
                "reference_rows",
                "comparison_rows",
                "algorithm_version",
            )
        },
        "scope": "Offline synthetic diagnostics only; no automatic action or production claim",
    }
    findings = _findings(context, metrics)
    contents = {
        "findings.json": canonical_json({"findings": findings}),
        "runbook.md": _runbook(),
        "investigation_report.md": _report(manifest, findings),
    }
    manifest["artifact_sha256"] = {name: sha256(data) for name, data in contents.items()}
    return manifest, contents


def build_investigation(
    drift_path: Path,
    output_root: Path = DEFAULT_INVESTIGATION_ROOT,
    *,
    ledger_path: Path | None = None,
    earlier_path: Path | None = None,
    later_path: Path | None = None,
    dossier_path: Path | None = None,
    registry: RegistryConfig | None = None,
) -> Path:
    """Verify all sources before exclusively publishing a deterministic packet."""
    manifest, contents = _prepare(
        drift_path, ledger_path, earlier_path, later_path, dossier_path, registry
    )
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / cast(str, manifest["investigation_id"])
    if target.exists():
        if inspect_investigation(target, registry) != manifest:
            raise MonitoringError("Existing investigation conflicts with verified evidence")
        return target
    target.mkdir()  # Exclusive claim; incomplete output remains visible and invalid.
    for name, data in contents.items():
        with (target / name).open("xb") as stream:
            stream.write(data)
    with (target / "investigation_manifest.json").open("xb") as stream:
        stream.write(canonical_json(manifest))
    inspect_investigation(target, registry)
    return target


def inspect_investigation(path: Path, registry: RegistryConfig | None = None) -> dict[str, object]:
    """Read-only full packet and upstream reinspection; never repair corruption."""
    artifact_directory(path, FILES)
    try:
        raw = (path / "investigation_manifest.json").read_bytes()
        stored = object_value(json.loads(raw), "investigation manifest")
        if raw != canonical_json(stored) or stored["investigation_id"] != path.name:
            raise MonitoringError("Investigation manifest identity/canonical form differs")
        if any(
            stored[key] != value
            for key, value in (
                ("investigation_contract_version", INVESTIGATION_VERSION),
                ("runbook_contract_version", RUNBOOK_VERSION),
                ("diagnostic_logic_version", LOGIC_VERSION),
            )
        ):
            raise MonitoringError("Unsupported investigation or runbook contract")
        sources = object_value(stored["sources"], "sources")
        if set(sources) not in (
            {"drift", "ledger"},
            {"drift", "ledger", "dossier"},
            {"drift", "ledger", "earlier", "later"},
            {"drift", "ledger", "earlier", "later", "dossier"},
        ):
            raise MonitoringError("Investigation source inventory differs")

        def path_of(name: str) -> Path:
            return Path(cast(str, object_value(sources[name], name)["path"]))

        expected, contents = _prepare(
            path_of("drift"),
            path_of("ledger"),
            path_of("earlier") if "earlier" in sources else None,
            path_of("later") if "later" in sources else None,
            path_of("dossier") if "dossier" in sources else None,
            registry,
        )
        if (
            stored != expected
            or identity(
                {
                    key: stored[key]
                    for key in (
                        "investigation_contract_version",
                        "runbook_contract_version",
                        "diagnostic_logic_version",
                        "sources",
                        "report_configuration",
                    )
                }
            )
            != path.name
        ):
            raise MonitoringError("Investigation source or semantic identity differs")
        for name, data in contents.items():
            if (path / name).read_bytes() != data:
                raise MonitoringError(f"Investigation artifact differs: {name}")
        return stored
    except MonitoringError:
        raise
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise MonitoringError(f"Cannot inspect investigation: {error}") from error
