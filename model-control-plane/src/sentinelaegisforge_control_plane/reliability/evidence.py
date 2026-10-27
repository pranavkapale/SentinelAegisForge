"""Immutable reliability dossier over verified, independent experiment outputs."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.availability.contract import (
    exact_directory,
    publish,
    read_object,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.incremental_shadow.worker import verify_reference
from sentinelaegisforge_control_plane.registry.run import RegistryConfig
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import CANDIDATE_ID

from .contract import CONTRACT, ReliabilityError, matrix
from .observation import _inventory, compare
from .observation import inspect as inspect_observation
from .operations import inspect_recovery
from .outage import verify as verify_outage
from .retention import feature_ready_range
from .retention import inspect as inspect_retention

FILES = {
    "reliability_manifest.json",
    "experiment_configuration.json",
    "source_reconciliation.json",
    "spark_stage_observations.json",
    "statistical_state_evidence.json",
    "checkpoint_recovery.json",
    "kafka_recovery.json",
    "backpressure_observations.json",
    "retention_readiness.json",
    "capacity_measurements.json",
    "model_parity.json",
    "reliability_report.md",
}


def _hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def build(
    integrations: dict[str, Path],
    streams: dict[str, Path],
    outage_evidence: Path,
    candidate: Path,
    current_transaction_fence: Path,
    current_ready_offset_log: Path,
    root: Path,
) -> dict[str, object]:
    if candidate.name != CANDIDATE_ID:
        raise ReliabilityError("Reliability experiments require the preserved research candidate")
    plan = matrix(integrations)
    if set(streams) != set(integrations):
        raise ReliabilityError("Every integration requires its own research stream")
    observations = {
        name: inspect_observation(path, streams[name]) for name, path in integrations.items()
    }
    recovery = inspect_recovery(integrations["C"])
    retention = {
        name: inspect_retention(path, current_transaction_fence)
        for name, path in integrations.items()
    }
    ready_retention = {
        name: feature_ready_range(stream, current_ready_offset_log)
        for name, stream in streams.items()
    }
    outage = read_object(outage_evidence)
    required = {"broker_outage", "research_pause", "backpressure_refusal", "recovered_results"}
    if set(outage) != required or any(
        not isinstance(outage[name], dict)
        or cast(dict[str, object], outage[name]).get("status") != "VERIFIED"
        for name in required
    ):
        raise ReliabilityError("Measured downstream outage/recovery evidence is incomplete")
    if outage != verify_outage(integrations["D"], streams["D"]):
        raise ReliabilityError("Stored downstream outage evidence differs from live artifacts")
    comparison = compare(observations["A"], observations["B"])
    examples = cast(dict[str, object], observations["A"]["state_continuity_examples"])
    if examples.get("retained") is None or examples.get("expired_and_reactivated") is None:
        raise ReliabilityError(
            "Baseline lacks independently checked retention/reactivation examples"
        )
    recovered_continuity = cast(dict[str, object], observations["C"]["state_continuity_examples"])
    if recovered_continuity.get("retained") is None:
        raise ReliabilityError(
            "Recovered statistical lineage lacks independently verified continuing state"
        )
    parity = {
        name: verify_reference(streams[name], candidate, RegistryConfig()) for name in integrations
    }
    if any(item["probability_mismatches"] != 0 for item in parity.values()):
        raise ReliabilityError("Independent research scoring parity differs")
    basis = {
        "contract_version": CONTRACT,
        "experiment_integrations": plan["experiments"],
        "corpora": plan["source_corpora"],
        "state_semantics": "customer-currency-welford-v2",
        "state_fingerprints": {
            name: item["state_compatibility_fingerprint"] for name, item in observations.items()
        },
        "research_stream_ids": {
            name: cast(
                dict[str, object],
                cast(dict[str, object], item["final_reconciliation"])["operational_progress"],
            )["stream_id"]
            for name, item in observations.items()
        },
        "fault_plan": "statistical-postcommit-batch-0-v1",
        "candidate_id": CANDIDATE_ID,
        "collector_version": "phase29-reliability-evidence-v1",
    }
    downstream = cast(dict[str, object], outage["backpressure_refusal"])
    snapshots = cast(dict[str, dict[str, object]], downstream["snapshots"])
    sampled_backlogs = {
        "broker_failed": cast(
            dict[str, object], cast(dict[str, object], outage["broker_outage"])["attempt"]
        )["backlogs"],
        "recovered_published": snapshots["recovered-published"],
        "research_paused": snapshots["research-paused"],
        "projected_refusal": cast(dict[str, object], downstream["refusal"])["backlogs"],
        "final": snapshots["final"],
    }
    sampled_peaks = {
        key: max(
            cast(int, cast(dict[str, object], snapshot)[key])
            for snapshot in sampled_backlogs.values()
        )
        for key in (
            "discovery_backlog_versions",
            "publication_backlog_ready_events",
            "acceptance_backlog_ready_events",
            "inference_backlog_ready_events",
        )
    }
    reliability_id = _hash(canonical_json(basis))
    artifacts: dict[str, bytes] = {
        "experiment_configuration.json": canonical_json(
            {
                "matrix": plan,
                "scenarios": {
                    name: item["scenario_configuration"] for name, item in observations.items()
                },
            }
        ),
        "source_reconciliation.json": canonical_json(
            {name: item["final_reconciliation"] for name, item in observations.items()}
        ),
        "spark_stage_observations.json": canonical_json(
            {name: item["spark_progress"] for name, item in observations.items()}
        ),
        "statistical_state_evidence.json": canonical_json(
            {
                name: {
                    "status_counts": item["statistical_status_counts"],
                    "state_keys": item["customer_currency_state_keys"],
                    "prior_count_distribution": item["prior_observation_count_distribution"],
                    "null_counts": item["null_counts"],
                    "continuity_examples": item["state_continuity_examples"],
                }
                for name, item in observations.items()
            }
        ),
        "checkpoint_recovery.json": canonical_json(recovery),
        "kafka_recovery.json": canonical_json(outage),
        "backpressure_observations.json": canonical_json(
            {
                "final_experiments": {
                    name: cast(dict[str, object], item["final_reconciliation"])[
                        "operational_progress"
                    ]
                    for name, item in observations.items()
                },
                "D_samples": sampled_backlogs,
                "sample_count": len(sampled_backlogs),
                "sampled_peaks": sampled_peaks,
                "method": "Five controlled boundary snapshots; not continuous peak telemetry",
            }
        ),
        "retention_readiness.json": canonical_json(
            {
                "model_artifact_inventory": _inventory(candidate / "model"),
                "experiments": {
                    name: {
                        "source_readiness": retention[name],
                        "feature_ready_kafka": ready_retention[name],
                        "checkpoints": item["checkpoint_inventory"],
                        "delta_storage": item["delta_storage_inventory"],
                        "research_storage": item["research_storage_inventory"],
                    }
                    for name, item in observations.items()
                },
            }
        ),
        "capacity_measurements.json": canonical_json(
            {
                "comparison": comparison,
                "observations": {
                    name: {
                        "source_event_count": item["source_event_count"],
                        "customer_count": item["customer_count"],
                        "partition_counts": item["kafka_partition_counts"],
                        "state_keys": item["customer_currency_state_keys"],
                        "resource_context": item["resource_context"],
                        "invocation_timings": item["invocation_timings"],
                        "timing_decomposition": item["timing_decomposition"],
                    }
                    for name, item in observations.items()
                },
            }
        ),
        "model_parity.json": canonical_json(
            {
                name: {
                    "research": cast(dict[str, object], item["final_reconciliation"])["research"],
                    "independent_reference": parity[name],
                }
                for name, item in observations.items()
            }
        ),
    }
    artifacts["reliability_report.md"] = (
        f"# Phase 29 local reliability dossier {reliability_id}\n\n"
        "Four isolated synthetic integrations. Results are bounded local observations; "
        "they are not production capacity, availability, fraud quality or payment guarantees.\n\n"
        + "\n".join(
            f"- {name}: {item['source_event_count']} source events; "
            f"{item['customer_currency_state_keys']} customer/currency keys"
            for name, item in observations.items()
        )
        + "\n"
    ).encode()
    manifest: dict[str, object] = {
        **basis,
        "reliability_id": reliability_id,
        "artifact_sha256": {name: _hash(raw) for name, raw in sorted(artifacts.items())},
        "synthetic_research_only": True,
    }
    target = root / reliability_id
    if target.exists():
        existing = inspect(target)
        if existing != manifest or any(
            (target / name).read_bytes() != raw for name, raw in artifacts.items()
        ):
            raise ReliabilityError("Existing reliability dossier differs")
        return existing
    artifacts["reliability_manifest.json"] = canonical_json(manifest)
    publish(target, artifacts)
    return inspect(target)


def inspect(path: Path) -> dict[str, object]:
    exact_directory(path, FILES)
    manifest = read_object(path / "reliability_manifest.json")
    if manifest.get("contract_version") != CONTRACT or not manifest.get("synthetic_research_only"):
        raise ReliabilityError("Invalid reliability dossier identity or scope")
    hashes = cast(dict[str, str], manifest["artifact_sha256"])
    if set(hashes) != FILES - {"reliability_manifest.json"}:
        raise ReliabilityError("Reliability artifact inventory differs")
    for name, expected in hashes.items():
        if _hash((path / name).read_bytes()) != expected:
            raise ReliabilityError(f"Corrupt reliability dossier artifact: {name}")
    basis = {
        key: value
        for key, value in manifest.items()
        if key not in {"reliability_id", "artifact_sha256", "synthetic_research_only"}
    }
    if _hash(canonical_json(basis)) != manifest["reliability_id"]:
        raise ReliabilityError("Reliability dossier identity differs")
    return manifest
