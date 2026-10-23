"""Bounded independent scorer over a sealed Kafka notification inbox and exact Delta history."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.availability.contract import (
    digest,
    exact_directory,
    file_hash,
    lines,
    publish,
    read_lines,
    read_object,
)
from sentinelaegisforge_control_plane.availability.discovery import discover_versions
from sentinelaegisforge_control_plane.availability.kafka_export import inspect_batch
from sentinelaegisforge_control_plane.candidates.contract import (
    FEATURES,
    INPUT_FINGERPRINT,
    CandidateError,
    validate_input,
)
from sentinelaegisforge_control_plane.candidates.run import load_verified_candidate
from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp, utc_text
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.monitoring.ledger import inspect_ledger
from sentinelaegisforge_control_plane.registry.run import (
    RegistryConfig,
    _source_path,
    inspect_registered_candidate,
)
from sentinelaegisforge_control_plane.shadow.parity import compare_rows
from sentinelaegisforge_control_plane.training.baseline import probability_estimates
from sentinelaegisforge_control_plane.training.run import inspect_run

from .contract import INFERENCE, RUN, RequestInferenceError, ShadowKafkaError, request_id
from .inbox import inspect_inbox
from .results import claim_result, counts, durable_results, inspect_result, result_path

MAXIMUM = 5000
RUN_FILES = {"run_manifest.json", "results.jsonl", "parity.json", "report.md"}
Scorer = Callable[[dict[str, object]], float]


@dataclass(frozen=True)
class WorkerConfig:
    inbox: Path
    source_delta: Path
    registry: RegistryConfig
    model_version: str
    score_start: datetime
    score_end: datetime
    result_root: Path
    run_root: Path
    reference_ledger: Path
    maximum_events: int = MAXIMUM
    maximum_cached_rows: int = 10000
    maximum_attempts: int = MAXIMUM
    inference_batch_size: int = 100

    def __post_init__(self) -> None:
        if not all(
            type(value) is int and 1 <= value <= 10000
            for value in (
                self.maximum_events,
                self.maximum_cached_rows,
                self.maximum_attempts,
                self.inference_batch_size,
            )
        ):
            raise ShadowKafkaError("Local processing bounds must be positive and at most 10000")
        if (
            self.maximum_events > MAXIMUM
            or self.maximum_attempts > MAXIMUM
            or self.inference_batch_size > MAXIMUM
        ):
            raise ShadowKafkaError("Inbox/attempt/batch bound exceeds local contract maximum")
        if self.score_start.tzinfo is None or self.score_end.tzinfo is None:
            raise ShadowKafkaError("Explicit UTC scoring window required")
        try:
            start = parse_utc_timestamp(utc_text(self.score_start))
            end = parse_utc_timestamp(utc_text(self.score_end))
        except ValueError as error:
            raise ShadowKafkaError("Invalid UTC scoring window") from error
        if start >= end:
            raise ShadowKafkaError("Empty/reversed scoring window")


def _source_rows(
    config: WorkerConfig, inbox: dict[str, object], events: list[dict[str, object]]
) -> list[tuple[dict[str, object], dict[str, object]]]:
    batch = inspect_batch(Path(cast(str, inbox["batch_path"])))
    outbox = read_object(Path(cast(str, batch["outbox_path"])) / "outbox_manifest.json")
    if str(config.source_delta.resolve()) != outbox["source_delta_path"]:
        raise ShadowKafkaError("Explicit Delta source differs from verified publication outbox")
    differences = discover_versions(
        config.source_delta,
        cast(int, outbox["start_delta_version"]),
        cast(int, outbox["end_delta_version"]),
        maximum=config.maximum_events,
    )
    if sum(item.current.row_count for item in differences) > config.maximum_cached_rows:
        raise ShadowKafkaError("Exact Delta snapshot cache exceeds explicit local bound")
    by_event: dict[str, tuple[int, str, dict[str, object]]] = {}
    for diff in differences:
        rows = {
            cast(str, row["event_id"]): row
            for row in cast(list[dict[str, object]], diff.current.table.to_pylist())
        }
        if (
            diff.current.delta_table_id != outbox["source_delta_table_id"]
            or diff.current.source_schema_fingerprint != outbox["source_schema_fingerprint"]
        ):
            raise ShadowKafkaError("Pinned Delta table/schema differs from ready source")
        for event_id, fingerprint in diff.new_rows.items():
            by_event[event_id] = (diff.version, fingerprint, rows[event_id])
    if set(by_event) != {item["event_id"] for item in events}:
        raise ShadowKafkaError("Committed source and sealed inbox event membership differ")
    resolved: list[tuple[dict[str, object], dict[str, object]]] = []
    for event in events:
        version, fingerprint, row = by_event[cast(str, event["event_id"])]
        if (
            event["source_delta_table_id"] != outbox["source_delta_table_id"]
            or event["source_schema_fingerprint"] != outbox["source_schema_fingerprint"]
            or event["first_visible_delta_version"] != version
            or event["source_feature_row_fingerprint"] != fingerprint
        ):
            raise ShadowKafkaError("Ready notification points to wrong Delta version/row")
        resolved.append((event, row))
    return sorted(
        resolved,
        key=lambda pair: (pair[0]["first_visible_delta_version"], pair[0]["event_id"]),
    )


def _base(
    config: WorkerConfig,
    inbox: dict[str, object],
    event: dict[str, object],
    candidate_id: str,
    model_hash: str,
) -> dict[str, object]:
    return {
        "inference_contract_version": INFERENCE,
        "inference_request_id": request_id(
            cast(str, event["ready_event_id"]), candidate_id, model_hash
        ),
        "shadow_inbox_id": inbox["inbox_id"],
        "ready_event_id": event["ready_event_id"],
        "event_id": event["event_id"],
        "candidate_id": candidate_id,
        "model_tree_sha256": model_hash,
        "registered_model_version": config.model_version,
        "first_visible_delta_version": event["first_visible_delta_version"],
        "source_feature_row_fingerprint": event["source_feature_row_fingerprint"],
        "score_start": utc_text(config.score_start),
        "score_end": utc_text(config.score_end),
    }


def evaluate_one(
    base: dict[str, object], row: dict[str, object], scorer: Scorer, start: datetime, end: datetime
) -> dict[str, object]:
    """Apply only the saved model input contract; never use labels or rules as outcomes."""
    ingestion = cast(datetime, row["ingestion_time"])
    status: str
    probability: float | None = None
    failure: str | None = None
    if not start <= ingestion < end:
        status = "OUT_OF_SCOPE"
    else:
        try:
            projection = validate_input({name: row[name] for name in FEATURES})
        except (CandidateError, KeyError, TypeError, ValueError):
            status, failure = "INPUT_REJECTED", "INVALID_MODEL_INPUT"
        else:
            try:
                probability = scorer(projection)
                if type(probability) is not float or not 0 <= probability <= 1:
                    raise RequestInferenceError("Invalid probability")
                status = "SCORED"
            except RequestInferenceError:
                status, probability, failure = "INFERENCE_FAILED", None, "MODEL_INFERENCE_ERROR"
    return {
        **base,
        "execution_status": status,
        "fraud_probability_estimate": probability,
        "failure_code": failure,
    }


def _parity(
    config: WorkerConfig,
    results: list[dict[str, object]],
    candidate_id: str,
    source_table_id: str,
    source_schema: str,
) -> dict[str, object]:
    ledger = inspect_ledger(config.reference_ledger)
    if (
        ledger["candidate_id"] != candidate_id
        or ledger["source_delta_table_id"] != source_table_id
        or ledger["source_schema_fingerprint"] != source_schema
        or ledger["score_start"] != utc_text(config.score_start)
        or ledger["score_end"] != utc_text(config.score_end)
        or ledger["input_contract_fingerprint"] != INPUT_FINGERPRINT
    ):
        raise ShadowKafkaError("Phase 19 reference identity/window differs")
    reference = cast(
        list[dict[str, object]],
        pq.read_table(config.reference_ledger / "predictions.parquet").to_pylist(),
    )
    scored = [item for item in results if item["execution_status"] == "SCORED"]
    return {
        "reference_ledger_id": ledger["ledger_id"],
        "comparison": compare_rows(scored, reference, candidate_id),
    }


def _run_basis(
    config: WorkerConfig, inbox_id: str, candidate_id: str, model_hash: str
) -> dict[str, object]:
    return {
        "run_contract_version": RUN,
        "inference_contract_version": INFERENCE,
        "shadow_inbox_id": inbox_id,
        "candidate_id": candidate_id,
        "model_tree_sha256": model_hash,
        "registered_model_name": config.registry.model_name,
        "registered_model_version": config.model_version,
        "score_start": utc_text(config.score_start),
        "score_end": utc_text(config.score_end),
        "reference_ledger_path": str(config.reference_ledger.resolve()),
    }


def process_resolved(
    config: WorkerConfig,
    inbox: dict[str, object],
    resolved: list[tuple[dict[str, object], dict[str, object]]],
    candidate_id: str,
    model_hash: str,
    scorer: Scorer,
    *,
    fail_after_result: int = -1,
) -> tuple[list[dict[str, object]], int]:
    """Advance bounded local work; a restart verifies and skips already durable requests."""
    namespace = config.result_root / cast(str, inbox["inbox_id"])
    expected_ids = [
        request_id(cast(str, event["ready_event_id"]), candidate_id, model_hash)
        for event, _ in resolved
    ]
    completed = durable_results(namespace, candidate_id, expected_ids)
    by_request = {cast(str, item["inference_request_id"]): item for item in completed}
    attempts = 0
    created = 0
    for index in range(0, len(resolved), config.inference_batch_size):
        for event, row in resolved[index : index + config.inference_batch_size]:
            base = _base(config, inbox, event, candidate_id, model_hash)
            identity = cast(str, base["inference_request_id"])
            prior = by_request.get(identity)
            if prior is not None:
                if any(prior.get(key) != value for key, value in base.items()):
                    raise ShadowKafkaError("Persisted inference request/source identity conflict")
                continue
            if attempts >= config.maximum_attempts:
                return durable_results(namespace, candidate_id, expected_ids), created
            result = evaluate_one(base, row, scorer, config.score_start, config.score_end)
            path = result_path(namespace, candidate_id, identity)
            if claim_result(path, result):
                created += 1
            by_request[identity] = inspect_result(path, result)
            attempts += 1
            if created == fail_after_result:
                raise ShadowKafkaError("Injected interruption after durable inference result")
    return durable_results(namespace, candidate_id, expected_ids), created


def execute(
    config: WorkerConfig, *, fail_after_result: int = -1, scorer_override: Scorer | None = None
) -> dict[str, object]:
    """Recheck global evidence before scoring; derive progress solely from durable outcomes."""
    inbox, events = inspect_inbox(config.inbox)
    if len(events) > config.maximum_events:
        raise ShadowKafkaError("Sealed inbox exceeds explicit event bound")
    resolved = _source_rows(config, inbox, events)
    registration = inspect_registered_candidate(config.registry, config.model_version)
    if (
        registration["production_eligible"] is not False
        or registration["governance_state"] != "REGISTERED_RESEARCH_ONLY"
    ):
        raise ShadowKafkaError("Only exact research-only Registry candidates may execute")
    candidate_id = cast(str, registration["candidate_id"])
    model_hash = cast(str, registration["model_tree_sha256"])
    candidate_dir = _source_path(cast(str, registration["source_uri"])).parent
    candidate, model = load_verified_candidate(candidate_dir, tracking=config.registry.tracking)
    if candidate["candidate_id"] != candidate_id or candidate["model_tree_sha256"] != model_hash:
        raise ShadowKafkaError("Loaded trusted candidate differs from Registry identity")
    training = inspect_run(Path(cast(str, candidate["source_training_path"])))
    if cast(dict[str, object], training["split_boundaries"])["train_end"] != utc_text(
        config.score_start
    ):
        raise ShadowKafkaError("Scoring starts before/after original verified training cutoff")
    scorer: Scorer
    if scorer_override is None:

        def scorer(projection: dict[str, object]) -> float:
            return float(probability_estimates(model, (projection,), FEATURES)[0])
    else:
        scorer = scorer_override
    expected_ids = [
        request_id(cast(str, event["ready_event_id"]), candidate_id, model_hash)
        for event, _ in resolved
    ]
    completed, created = process_resolved(
        config,
        inbox,
        resolved,
        candidate_id,
        model_hash,
        scorer,
        fail_after_result=fail_after_result,
    )
    summary = counts(completed, len(events))
    if summary["unresolved"]:
        return {"status": "INCOMPLETE", "counts": summary, "new_results": created}
    if summary["INPUT_REJECTED"] or summary["INFERENCE_FAILED"]:
        return {"status": "DIAGNOSTIC_ONLY", "counts": summary, "new_results": created}
    by_request = {cast(str, item["inference_request_id"]): item for item in completed}
    ordered = [by_request[identity] for identity in expected_ids]
    batch = inspect_batch(Path(cast(str, inbox["batch_path"])))
    parity = _parity(
        config,
        ordered,
        candidate_id,
        cast(str, batch["source_delta_table_id"]),
        cast(str, batch["source_schema_fingerprint"]),
    )
    basis = _run_basis(config, cast(str, inbox["inbox_id"]), candidate_id, model_hash)
    run_id = digest(basis)
    target = config.run_root.resolve() / run_id
    result_bytes = lines(ordered)
    parity_bytes = canonical_json(parity)
    report_bytes = (
        f"# Kafka-triggered research shadow inference\n\nRun: {run_id}\n\n"
        f"Inbox: {len(events)}; SCORED: {summary['SCORED']}; OUT_OF_SCOPE: "
        f"{summary['OUT_OF_SCOPE']}; unresolved: 0.\n\n"
        "Historical synthetic research only. No original online latency, calibration, "
        "production approval or payment enforcement is established.\n"
    ).encode()
    manifest = {
        **basis,
        "run_id": run_id,
        "counts": summary,
        "results_sha256": digest_bytes(result_bytes),
        "parity_sha256": digest_bytes(parity_bytes),
        "report_sha256": digest_bytes(report_bytes),
    }
    if not target.exists():
        publish(
            target,
            {
                "results.jsonl": result_bytes,
                "parity.json": parity_bytes,
                "report.md": report_bytes,
                "run_manifest.json": canonical_json(manifest),
            },
        )
    inspected = inspect_run_artifact(target)
    if inspected != manifest:
        raise ShadowKafkaError("Existing immutable research run conflicts")
    return {
        "status": "COMPLETE",
        "run_path": str(target),
        "counts": summary,
        "new_results": created,
        "parity": parity,
    }


def digest_bytes(raw: bytes) -> str:
    import hashlib

    return hashlib.sha256(raw).hexdigest()


def inspect_run_artifact(path: Path) -> dict[str, object]:
    exact_directory(path, RUN_FILES)
    manifest = read_object(path / "run_manifest.json")
    basis = {
        key: value
        for key, value in manifest.items()
        if key not in {"run_id", "counts", "results_sha256", "parity_sha256", "report_sha256"}
    }
    results = read_lines(path / "results.jsonl")
    parity = read_object(path / "parity.json")
    if (
        path.name != digest(basis)
        or manifest.get("run_id") != path.name
        or manifest.get("results_sha256") != file_hash(path / "results.jsonl")
        or manifest.get("parity_sha256") != file_hash(path / "parity.json")
        or manifest.get("report_sha256") != file_hash(path / "report.md")
        or manifest.get("counts") != counts(results, len(results))
        or cast(dict[str, object], parity.get("comparison"))["mismatch_count"] != 0
    ):
        raise ShadowKafkaError("Immutable research run content/identity differs")
    return manifest
