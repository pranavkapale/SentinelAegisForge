"""One-model, bounded, non-enforcing local replay with terminal request outcomes."""

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.candidates.contract import (
    FEATURES,
    INPUT_FINGERPRINT,
    CandidateError,
    validate_input,
)
from sentinelaegisforge_control_plane.candidates.run import load_verified_candidate
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.monitoring.contract import row_fingerprint
from sentinelaegisforge_control_plane.registry.run import (
    RegistryConfig,
    _source_path,
    inspect_registered_candidate,
)
from sentinelaegisforge_control_plane.training.baseline import probability_estimates

from .contract import (
    AVAILABILITY,
    EXECUTION_VERSION,
    RESULT_SCHEMA,
    RUNTIME_VERSION,
    SCOPE,
    TERMINAL,
    RequestInferenceError,
    ShadowError,
    digest,
    exact_directory,
    file_hash,
    probability,
    read_object,
)
from .plan import ROOT, inspect_plan

DEFAULT_EXECUTION_ROOT = ROOT / ".local/ml/shadow-executions"
EXECUTION_FILES = {
    "execution_manifest.json",
    "results.parquet",
    "audit.jsonl",
    "execution_report.md",
}
Scorer = Callable[[dict[str, object]], float]


def _line_bytes(records: list[dict[str, object]]) -> bytes:
    return b"".join(canonical_json(record) for record in records)


def run_deliveries(
    deliveries: Iterable[dict[str, object]],
    source_rows: dict[str, dict[str, object]],
    candidate_id: str,
    model_version: str,
    scorer: Scorer,
    *,
    columns: tuple[str, ...],
) -> tuple[list[dict[str, object]], list[dict[str, object]], int]:
    """Local logical idempotence: a repeated delivery returns the first terminal outcome."""
    committed: dict[str, tuple[tuple[object, object, object], dict[str, object]]] = {}
    results: list[dict[str, object]] = []
    audit: list[dict[str, object]] = []
    duplicates = 0
    status: str
    code: str | None
    value: float | None

    def record(name: str, request: dict[str, object]) -> None:
        audit.append(
            {"audit_sequence": len(audit), "event": name, "request_id": request["request_id"]}
        )

    for request in deliveries:
        request_id = request.get("request_id")
        event_id = request.get("event_id")
        sequence = request.get("replay_sequence")
        fingerprint = request.get("source_feature_row_fingerprint")
        if (
            not isinstance(request_id, str)
            or not request_id
            or not isinstance(event_id, str)
            or not event_id
            or type(sequence) is not int
            or sequence < 0
            or not isinstance(fingerprint, str)
        ):
            raise ShadowError("Malformed request envelope")
        identity = (event_id, sequence, fingerprint)
        prior = committed.get(request_id)
        if prior is not None:
            if prior[0] != identity:
                raise ShadowError("Conflicting duplicate request_id")
            duplicates += 1
            record("DUPLICATE_DELIVERY", request)
            continue
        record("REQUEST_ACCEPTED", request)
        row = source_rows.get(event_id)
        try:
            if row is None or row_fingerprint(row, columns) != fingerprint:
                raise CandidateError("Request source row identity differs")
            projection = validate_input({name: row[name] for name in FEATURES})
            record("REQUEST_VALIDATED", request)
        except (CandidateError, KeyError, ValueError, TypeError):
            status, value, code = "INPUT_REJECTED", None, "INVALID_MODEL_INPUT"
            record("INPUT_REJECTED", request)
        else:
            record("INFERENCE_ATTEMPTED", request)
            try:
                value = probability(scorer(projection))
                status, code = "SCORED", None
                record("INFERENCE_SUCCEEDED", request)
            except (RequestInferenceError, ShadowError):
                # No exception text or raw features enter externally inspectable evidence.
                status, value, code = "EXECUTION_FAILED", None, "MODEL_INFERENCE_ERROR"
                record("INFERENCE_FAILED", request)
        result = {
            "request_id": request_id,
            "event_id": event_id,
            "replay_sequence": sequence,
            "candidate_id": candidate_id,
            "model_version": model_version,
            "execution_status": status,
            "fraud_probability_estimate": value,
            "failure_code": code,
            "source_feature_row_fingerprint": fingerprint,
        }
        committed[request_id] = identity, result
        results.append(result)
    audit.append({"audit_sequence": len(audit), "event": "EXECUTION_COMPLETED", "request_id": None})
    return results, audit, duplicates


def _identity(plan: dict[str, object], maximum: int) -> dict[str, object]:
    return {
        "execution_contract_version": EXECUTION_VERSION,
        "logical_runtime_contract_version": RUNTIME_VERSION,
        "plan_id": plan["plan_id"],
        "candidate_id": plan["candidate_id"],
        "registered_model_name": plan["registered_model_name"],
        "registered_model_version": plan["registered_model_version"],
        "model_tree_sha256": plan["model_tree_sha256"],
        "source_delta_path": plan["source_delta_path"],
        "source_delta_table_id": plan["source_delta_table_id"],
        "source_delta_version": plan["source_delta_version"],
        "source_schema_fingerprint": plan["source_schema_fingerprint"],
        "input_contract_fingerprint": INPUT_FINGERPRINT,
        "maximum_requests": maximum,
        "execution_scope": SCOPE,
        "feature_availability_evidence": AVAILABILITY,
    }


def _report(execution_id: str, planned: int, counts: dict[str, int], duplicates: int) -> bytes:
    return (
        "# Local research shadow execution\n\n"
        f"Execution: {execution_id}\n\nPlanned/unique: {planned} / {planned}\n\n"
        f"SCORED: {counts['SCORED']}; INPUT_REJECTED: {counts['INPUT_REJECTED']}; "
        f"EXECUTION_FAILED: {counts['EXECUTION_FAILED']}; duplicate deliveries: {duplicates}; "
        "unresolved: 0.\n\n"
        "Historical feature availability NOT_ESTABLISHED. No enforcement or production inference.\n"
    ).encode()


def execute_plan(
    plan_path: Path,
    registry: RegistryConfig,
    output_root: Path = DEFAULT_EXECUTION_ROOT,
    *,
    maximum: int = 5000,
) -> Path:
    plan, requests = inspect_plan(plan_path, registry)
    if type(maximum) is not int or not 1 <= len(requests) <= maximum <= 5000:
        raise ShadowError("Execution exceeds configured bounded request count")
    basis = _identity(plan, maximum)
    execution_id = digest(basis)
    target = output_root.resolve() / execution_id
    if target.exists():
        inspect_execution(target, plan_path, registry)
        return target
    registration = inspect_registered_candidate(
        registry, cast(str, plan["registered_model_version"])
    )
    if any(registration[key] != plan[key] for key in ("candidate_id", "model_tree_sha256")):
        raise ShadowError("Registered candidate differs from replay plan")
    candidate = _source_path(cast(str, registration["source_uri"])).parent
    verified, model = load_verified_candidate(candidate, tracking=registry.tracking)
    if (
        verified["candidate_id"] != plan["candidate_id"]
        or verified["model_tree_sha256"] != plan["model_tree_sha256"]
    ):
        raise ShadowError("Trusted loaded candidate differs")
    source = StatisticalFeatureSource(
        Path(cast(str, plan["source_delta_path"])), cast(int, plan["source_delta_version"])
    ).read()
    rows = {
        cast(str, row["event_id"]): row
        for row in cast(list[dict[str, object]], source.table.to_pylist())
    }
    source_columns = tuple(source.table.column_names)
    if (
        source.delta_table_id != plan["source_delta_table_id"]
        or source.delta_version != plan["source_delta_version"]
        or source.source_schema_fingerprint != plan["source_schema_fingerprint"]
        or any(
            request["event_id"] not in rows
            or row_fingerprint(rows[cast(str, request["event_id"])], source_columns)
            != request["source_feature_row_fingerprint"]
            for request in requests
        )
    ):
        raise ShadowError("Fixed source changed between plan verification and execution")

    def score(projection: dict[str, object]) -> float:
        return probability_estimates(model, (projection,), FEATURES)[0]

    results, audit, duplicates = run_deliveries(
        requests,
        rows,
        cast(str, plan["candidate_id"]),
        cast(str, plan["registered_model_version"]),
        score,
        columns=source_columns,
    )
    if len(results) != len(requests) or duplicates:
        raise ShadowError("Canonical plan execution has unresolved/duplicate requests")
    counts = {
        status: sum(row["execution_status"] == status for row in results)
        for status in sorted(TERMINAL)
    }
    if sum(counts.values()) != len(results):
        raise ShadowError("Terminal request counts do not reconcile")
    report = _report(execution_id, len(requests), counts, duplicates)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir()  # Exclusive claim; incomplete output remains visible.
    with (target / "results.parquet").open("xb") as stream:
        pq.write_table(
            pa.Table.from_pylist(results, schema=RESULT_SCHEMA),
            stream,
            version="2.6",
            compression="zstd",
            use_dictionary=False,
        )
    (target / "audit.jsonl").write_bytes(_line_bytes(audit))
    (target / "execution_report.md").write_bytes(report)
    manifest = {
        **basis,
        "execution_id": execution_id,
        "plan_path": str(plan_path.resolve()),
        "planned_requests": len(requests),
        "unique_requests": len(results),
        "duplicate_deliveries": duplicates,
        "scored": counts["SCORED"],
        "input_rejected": counts["INPUT_REJECTED"],
        "execution_failed": counts["EXECUTION_FAILED"],
        "unresolved_requests": 0,
        "positive_class": {"label": 1, "meaning": "FRAUD"},
        "output_logical_fingerprint": digest(results),
        "audit_logical_fingerprint": digest(audit),
        "artifact_sha256": {
            name: file_hash(target / name)
            for name in ("results.parquet", "audit.jsonl", "execution_report.md")
        },
        "full_scoring_success": counts["SCORED"] == len(requests),
    }
    (target / "execution_manifest.json").write_bytes(canonical_json(manifest))
    inspect_execution(target, plan_path, registry)
    return target


def inspect_execution(path: Path, plan_path: Path, registry: RegistryConfig) -> dict[str, object]:
    exact_directory(path, EXECUTION_FILES)
    manifest = read_object(path / "execution_manifest.json")
    plan, requests = inspect_plan(plan_path, registry)
    try:
        basis = _identity(plan, cast(int, manifest["maximum_requests"]))
        if (
            path.name != digest(basis)
            or manifest["execution_id"] != path.name
            or any(manifest[key] != value for key, value in basis.items())
            or manifest["plan_path"] != str(plan_path.resolve())
            or manifest["planned_requests"] != len(requests)
            or manifest["unique_requests"] != len(requests)
            or manifest["unresolved_requests"] != 0
            or manifest["positive_class"] != {"label": 1, "meaning": "FRAUD"}
        ):
            raise ShadowError("Execution identity/plan/positive class differs")
        artifact_hashes = cast(dict[str, str], manifest["artifact_sha256"])
        for name, expected_hash in artifact_hashes.items():
            if (
                name not in EXECUTION_FILES - {"execution_manifest.json"}
                or file_hash(path / name) != expected_hash
            ):
                raise ShadowError(f"Execution artifact hash differs: {name}")
        if set(artifact_hashes) != EXECUTION_FILES - {"execution_manifest.json"}:
            raise ShadowError("Execution artifact list differs")
        table = pq.read_table(path / "results.parquet")
        if table.schema != RESULT_SCHEMA:
            raise ShadowError("Execution result schema differs")
        results = cast(list[dict[str, object]], table.to_pylist())
        if (
            len(results) != len(requests)
            or digest(results) != manifest["output_logical_fingerprint"]
        ):
            raise ShadowError("Execution logical output differs")
        counts = {status: 0 for status in TERMINAL}
        for expected, result in zip(requests, results, strict=True):
            if (
                any(
                    result[key] != expected[key]
                    for key in (
                        "request_id",
                        "event_id",
                        "replay_sequence",
                        "source_feature_row_fingerprint",
                    )
                )
                or result["candidate_id"] != plan["candidate_id"]
                or result["model_version"] != plan["registered_model_version"]
            ):
                raise ShadowError("Request-to-result reconciliation differs")
            status = result["execution_status"]
            if status not in TERMINAL:
                raise ShadowError("Nonterminal execution result")
            counts[cast(str, status)] += 1
            if status == "SCORED":
                probability(result["fraud_probability_estimate"])
                if result["failure_code"] is not None:
                    raise ShadowError("Scored result has a failure code")
            elif result["fraud_probability_estimate"] is not None or result["failure_code"] != (
                "INVALID_MODEL_INPUT" if status == "INPUT_REJECTED" else "MODEL_INFERENCE_ERROR"
            ):
                raise ShadowError("Failed result contains a score or invalid failure code")
        if any(
            manifest[key] != counts[status]
            for key, status in (
                ("scored", "SCORED"),
                ("input_rejected", "INPUT_REJECTED"),
                ("execution_failed", "EXECUTION_FAILED"),
            )
        ) or manifest["full_scoring_success"] != (counts["SCORED"] == len(results)):
            raise ShadowError("Execution counts/success assertion differ")
        raw = (path / "audit.jsonl").read_bytes()
        audit = [json.loads(line) for line in raw.splitlines()]
        if (
            raw != _line_bytes(audit)
            or digest(audit) != manifest["audit_logical_fingerprint"]
            or audit[-1]["event"] != "EXECUTION_COMPLETED"
            or [row["audit_sequence"] for row in audit] != list(range(len(audit)))
        ):
            raise ShadowError("Execution audit differs")
        expected_events: list[tuple[str, str | None]] = []
        for result in results:
            request_id = cast(str, result["request_id"])
            expected_events.append(("REQUEST_ACCEPTED", request_id))
            if result["execution_status"] == "INPUT_REJECTED":
                expected_events.append(("INPUT_REJECTED", request_id))
            else:
                expected_events.extend(
                    [
                        ("REQUEST_VALIDATED", request_id),
                        ("INFERENCE_ATTEMPTED", request_id),
                        (
                            "INFERENCE_SUCCEEDED"
                            if result["execution_status"] == "SCORED"
                            else "INFERENCE_FAILED",
                            request_id,
                        ),
                    ]
                )
        expected_events.append(("EXECUTION_COMPLETED", None))
        if (
            manifest["duplicate_deliveries"] != 0
            or [(item["event"], item["request_id"]) for item in audit] != expected_events
        ):
            raise ShadowError("Execution audit lifecycle differs")
        if (path / "execution_report.md").read_bytes() != _report(
            path.name, len(results), counts, 0
        ):
            raise ShadowError("Execution report content differs")
        return manifest
    except ShadowError:
        raise
    except (OSError, ValueError, TypeError, KeyError, IndexError, pa.ArrowException) as error:
        raise ShadowError(f"Cannot inspect execution: {error}") from error
