"""Read-only Phase 19 oracle comparison and immutable local parity evidence."""

import math
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.monitoring.ledger import inspect_ledger
from sentinelaegisforge_control_plane.registry.run import RegistryConfig

from .contract import (
    PARITY_VERSION,
    ShadowError,
    digest,
    exact_directory,
    file_hash,
    probability,
    read_object,
)
from .execution import inspect_execution
from .plan import ROOT

DEFAULT_PARITY_ROOT = ROOT / ".local/ml/shadow-parity"
PARITY_FILES = {"manifest.json", "parity_report.md"}
REL_TOL = 1e-12
ABS_TOL = 1e-12


def compare_rows(
    results: list[dict[str, object]],
    reference: list[dict[str, object]],
    candidate_id: str,
    *,
    relative_tolerance: float = REL_TOL,
    absolute_tolerance: float = ABS_TOL,
) -> dict[str, object]:
    if any(
        not math.isfinite(value) or value < 0 for value in (relative_tolerance, absolute_tolerance)
    ):
        raise ShadowError("Invalid parity tolerance")
    result_ids = [cast(str, row["event_id"]) for row in results]
    reference_ids = [cast(str, row["event_id"]) for row in reference]
    if len(set(result_ids)) != len(result_ids) or len(set(reference_ids)) != len(reference_ids):
        raise ShadowError("Duplicate parity identity")
    by_reference = {cast(str, row["event_id"]): row for row in reference}
    missing = sorted(set(reference_ids) - set(result_ids))
    unexpected = sorted(set(result_ids) - set(reference_ids))
    if missing or unexpected:
        raise ShadowError(
            f"Parity event IDs differ: missing={len(missing)}, unexpected={len(unexpected)}"
        )
    max_abs = max_rel = 0.0
    mismatches = 0
    for result in results:
        if result["candidate_id"] != candidate_id or result["execution_status"] != "SCORED":
            raise ShadowError("Parity requires successful exact-candidate results")
        prior = by_reference[cast(str, result["event_id"])]
        if (
            prior["candidate_id"] != candidate_id
            or prior["source_feature_row_fingerprint"] != result["source_feature_row_fingerprint"]
        ):
            raise ShadowError("Candidate/source-row fingerprint parity differs")
        observed = probability(result["fraud_probability_estimate"])
        expected = probability(prior["fraud_probability_estimate"])
        difference = abs(observed - expected)
        max_abs = max(max_abs, difference)
        if expected:
            max_rel = max(max_rel, difference / abs(expected))
        mismatches += not math.isclose(
            observed, expected, rel_tol=relative_tolerance, abs_tol=absolute_tolerance
        )
    if mismatches:
        raise ShadowError(f"Probability parity failed for {mismatches} events")
    return {
        "compared_event_count": len(results),
        "missing_reference_ids": [],
        "unexpected_ids": [],
        "maximum_absolute_difference": max_abs,
        "maximum_relative_difference": max_rel,
        "mismatch_count": 0,
        "relative_tolerance": relative_tolerance,
        "absolute_tolerance": absolute_tolerance,
    }


def _comparison(
    execution_path: Path, plan_path: Path, ledger_path: Path, registry: RegistryConfig
) -> tuple[dict[str, object], dict[str, object]]:
    execution = inspect_execution(execution_path, plan_path, registry)
    reference = inspect_ledger(ledger_path)
    if (
        reference["candidate_id"] != execution["candidate_id"]
        or reference["registered_model_name"] != execution["registered_model_name"]
        or reference["registered_model_version"] != execution["registered_model_version"]
        or reference["input_contract_fingerprint"] != execution["input_contract_fingerprint"]
        or reference["source_delta_table_id"] != execution["source_delta_table_id"]
        or reference["source_delta_version"] != execution["source_delta_version"]
        or reference["positive_class"] != execution["positive_class"]
    ):
        raise ShadowError("Reference model/source/positive-class identity differs")
    results = cast(
        list[dict[str, object]], pq.read_table(execution_path / "results.parquet").to_pylist()
    )
    predictions = cast(
        list[dict[str, object]], pq.read_table(ledger_path / "predictions.parquet").to_pylist()
    )
    return execution, {
        "parity_contract_version": PARITY_VERSION,
        "execution_id": execution["execution_id"],
        "reference_ledger_id": reference["ledger_id"],
        "comparison": compare_rows(results, predictions, cast(str, execution["candidate_id"])),
    }


def _report(basis: dict[str, object]) -> bytes:
    comparison = cast(dict[str, object], basis["comparison"])
    return (
        "# Local research shadow parity\n\n"
        f"Execution: {basis['execution_id']}\n\n"
        f"Reference ledger: {basis['reference_ledger_id']}\n\n"
        f"Compared: {comparison['compared_event_count']}; mismatches: 0; "
        f"maximum absolute difference: {comparison['maximum_absolute_difference']}; "
        f"maximum relative difference: {comparison['maximum_relative_difference']}.\n\n"
        "Synthetic historical replay only; feature availability and production quality unproven.\n"
    ).encode()


def verify_parity(
    execution_path: Path,
    plan_path: Path,
    ledger_path: Path,
    registry: RegistryConfig,
    output_root: Path = DEFAULT_PARITY_ROOT,
) -> Path:
    _, basis = _comparison(execution_path, plan_path, ledger_path, registry)
    parity_id = digest(basis)
    target = output_root.resolve() / parity_id
    if target.exists():
        if inspect_parity(target, execution_path, plan_path, ledger_path, registry) != {
            **basis,
            "parity_id": parity_id,
            "report_sha256": file_hash(target / "parity_report.md"),
        }:
            raise ShadowError("Existing parity evidence conflicts")
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir()
    (target / "parity_report.md").write_bytes(_report(basis))
    (target / "manifest.json").write_bytes(
        canonical_json(
            {
                **basis,
                "parity_id": parity_id,
                "report_sha256": file_hash(target / "parity_report.md"),
            }
        )
    )
    inspect_parity(target, execution_path, plan_path, ledger_path, registry)
    return target


def inspect_parity(
    path: Path, execution_path: Path, plan_path: Path, ledger_path: Path, registry: RegistryConfig
) -> dict[str, object]:
    exact_directory(path, PARITY_FILES)
    manifest = read_object(path / "manifest.json")
    _, basis = _comparison(execution_path, plan_path, ledger_path, registry)
    if (path / "parity_report.md").read_bytes() != _report(basis):
        raise ShadowError("Parity report content differs")
    if path.name != digest(basis) or manifest != {
        **basis,
        "parity_id": path.name,
        "report_sha256": file_hash(path / "parity_report.md"),
    }:
        raise ShadowError("Parity evidence identity or report differs")
    return manifest
