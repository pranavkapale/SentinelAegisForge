"""Immutable measured evidence over verified live source and research artifacts."""

import hashlib
from pathlib import Path
from typing import cast

from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import (
    AvailabilityError,
    exact_directory,
    publish,
    read_object,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.incremental_shadow.worker import verify_reference
from sentinelaegisforge_control_plane.registry.run import RegistryConfig

from .contract import IntegrationError, inspect_identity, read_plan
from .reconciliation import trace_event, verify_final

CONTRACT = "stateful-streaming-shadow-evidence-v1"
FILES = {
    "integration_manifest.json",
    "source_wave_reconciliation.json",
    "streaming_stage_reconciliation.json",
    "feature_ready_transport_evidence.json",
    "shadow_inference_evidence.json",
    "failure_recovery_evidence.json",
    "timing_observations.json",
    "spark_progress_evidence.json",
    "example_event_traces.json",
    "integration_report.md",
}


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def build(integration: Path, stream: Path, candidate: Path, recovery: Path) -> dict[str, object]:
    identity = inspect_identity(integration)
    final = verify_final(integration, stream)
    parity = verify_reference(stream, candidate, RegistryConfig())
    fault = read_object(recovery)
    required = {
        "stateful_restart",
        "research_worker_pause",
        "backpressure",
        "broker_outage_after_feature_commit",
        "recovered_publication",
        "validated_ingestion_fault_scope",
    }
    if set(fault) != required:
        raise IntegrationError("Incomplete measured failure/recovery evidence")
    for key in required:
        item = fault[key]
        if (
            not isinstance(item, dict)
            or item.get("status") != "verified"
            or not item.get("evidence")
        ):
            raise IntegrationError(f"Missing verified failure/recovery evidence: {key}")
    waves = [read_object(integration / "waves" / f"{wave}.json") for wave in range(3)]
    logs = integration / "logs"
    spark_progress: list[dict[str, object]] = []
    for wave in range(3):
        for stage, filename in (
            ("deduplication", "deduplicate-transactions"),
            ("rolling", "process-rolling-features"),
            ("statistical", "process-statistical-features"),
        ):
            log = logs / f"{filename}-{wave}.log"
            raw = log.read_bytes()
            lines = [line for line in raw.decode().splitlines() if "progress: batchId=" in line]
            if not lines:
                raise IntegrationError(f"Missing real Spark state progress in {log}")
            spark_progress.append(
                {"wave": wave, "stage": stage, "log_sha256": _sha(raw), "progress_lines": lines}
            )
    plan = read_plan(Path(cast(str, identity["corpus_path"])))
    stat_table = DeltaTable(cast(dict[str, str], identity["paths"])["statistical"])
    stat_rows = {
        row["event_id"]: row
        for row in stat_table.to_pyarrow_table(
            columns=["event_id", "prior_amount_observation_count", "statistical_feature_status"]
        ).to_pylist()
    }
    first = next(
        row["event_id"]
        for row in plan.rows
        if stat_rows[row["event_id"]]["statistical_feature_status"] == "NO_HISTORY"
    )
    returning = next(
        row["event_id"]
        for row in plan.rows
        if row["wave"] == "0" and stat_rows[row["event_id"]]["prior_amount_observation_count"] > 0
    )
    prior_keys = {(row["customer_id"], row["currency"]) for row in plan.rows if row["wave"] == "0"}
    later = next(
        row["event_id"]
        for row in plan.rows
        if row["wave"] == "1"
        and (row["customer_id"], row["currency"]) in prior_keys
        and stat_rows[row["event_id"]]["prior_amount_observation_count"] > 0
    )
    traces = [trace_event(integration, stream, event_id) for event_id in (first, returning, later)]
    timing = {
        "clock": "Python time.perf_counter per bounded process invocation",
        "observations": [
            read_object(item) for item in sorted((integration / "timings").glob("*.json"))
        ],
        "interpretation": (
            "local processing diagnostics; synthetic UTC event times are not host latency"
        ),
    }
    if len(timing["observations"]) < 6:
        raise IntegrationError("Missing per-wave processing/handoff observations")
    content: dict[str, bytes] = {
        "source_wave_reconciliation.json": canonical_json(
            {"corpus_id": identity["corpus_id"], "waves": waves}
        ),
        "streaming_stage_reconciliation.json": canonical_json(
            {"stages": final["stages"], "source_event_count": final["source_event_count"]}
        ),
        "feature_ready_transport_evidence.json": canonical_json(
            {
                "ready_event_count": final["ready_event_count"],
                "progress": final["operational_progress"],
            }
        ),
        "shadow_inference_evidence.json": canonical_json(
            {"research": final["research"], "independent_reference": parity}
        ),
        "failure_recovery_evidence.json": canonical_json(fault),
        "timing_observations.json": canonical_json(timing),
        "spark_progress_evidence.json": canonical_json({"observations": spark_progress}),
        "example_event_traces.json": canonical_json({"traces": traces}),
    }
    hashes = {name: _sha(raw) for name, raw in sorted(content.items())}
    final_stages = cast(dict[str, dict[str, object]], final["stages"])
    final_progress = cast(dict[str, object], final["operational_progress"])
    final_research = cast(dict[str, object], final["research"])
    manifest: dict[str, object] = {
        "contract_version": CONTRACT,
        "integration_id": identity["integration_id"],
        "corpus_id": identity["corpus_id"],
        "candidate_id": identity["candidate_id"],
        "transaction_topic_id": identity["transaction_topic_id"],
        "statistical_delta_table_id": final_stages["statistical"]["table_id"],
        "statistical_delta_version": final_stages["statistical"]["version"],
        "stream_id": final_progress["stream_id"],
        "artifact_sha256": hashes,
        "synthetic_research_only": True,
    }
    content["integration_report.md"] = (
        f"# Phase 28 local integration {identity['integration_id']}\n\n"
        f"Corpus: `{identity['corpus_id']}`; "
        f"committed source events: {final['source_event_count']}; "
        f"ready events: {final['ready_event_count']}.\n\n"
        f"Research results: {final_research['terminal_inference_results']}; "
        f"reference comparisons: {parity['compared_scored_events']}; "
        f"probability mismatches: {parity['probability_mismatches']}.\n\n"
        "Local synthetic integration evidence only. No production fraud-quality, payment, "
        "availability or latency claim.\n"
    ).encode()
    hashes["integration_report.md"] = _sha(content["integration_report.md"])
    # Manifest hashes all other artifacts, never itself.
    content["integration_manifest.json"] = canonical_json(manifest)
    target = integration / "evidence"
    if target.exists():
        existing = inspect(target)
        if existing != manifest or any(
            (target / name).read_bytes() != raw for name, raw in content.items()
        ):
            raise IntegrationError("Existing immutable integration evidence differs")
    else:
        publish(target, content)
    return inspect(target)


def inspect(path: Path) -> dict[str, object]:
    exact_directory(path, FILES)
    manifest = read_object(path / "integration_manifest.json")
    if manifest.get("contract_version") != CONTRACT or not manifest.get("synthetic_research_only"):
        raise IntegrationError("Invalid integration evidence identity or scope")
    hashes = cast(dict[str, str], manifest["artifact_sha256"])
    if set(hashes) != FILES - {"integration_manifest.json"}:
        raise IntegrationError("Integration artifact inventory differs")
    for name, expected in hashes.items():
        if _sha((path / name).read_bytes()) != expected:
            raise AvailabilityError(f"Corrupt integration artifact: {name}")
    return manifest
