"""Factual local timing observations, separate from deterministic stream identity."""

from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.availability.contract import (
    AvailabilityError,
    digest,
    publish,
    read_object,
)
from sentinelaegisforge_control_plane.availability.outbox import canonical

from .operations import inspect, records
from .progress import reconcile


def build_report(root: Path) -> dict[str, object]:
    manifest = inspect(root)
    summary = reconcile(root)
    observations: list[dict[str, object]] = []
    directory = root / "observations"
    for path in sorted(directory.glob("*.json")) if directory.exists() else []:
        item = read_object(path)
        if item.get("observation_contract_version") != "incremental-shadow-timing-v1":
            raise AvailabilityError("Unsupported local timing observation")
        duration = item.get("elapsed_seconds")
        if duration is None and type(item.get("work_tick_seconds")) is float:
            duration = item["work_tick_seconds"]
        if duration is None and type(item.get("elapsed_nanos")) is int:
            duration = cast(int, item["elapsed_nanos"]) / 1_000_000_000
        stage = item.get("stage", "inference_tick" if "work_tick_seconds" in item else None)
        if (
            not isinstance(stage, str)
            or type(duration) not in {int, float}
            or cast(float, duration) < 0
        ):
            raise AvailabilityError("Invalid local timing observation")
        observations.append(
            {
                "stage": stage,
                "elapsed_seconds": duration,
                "sample_count": item.get("sample_count"),
                "model_loads": item.get("model_loads"),
                "model_load_seconds": item.get("model_load_seconds"),
                "publication_batch_id": item.get("publication_batch_id"),
                "observation_sha256": digest(item),
            }
        )
    by_stage: dict[str, list[float]] = {}
    for item in observations:
        by_stage.setdefault(cast(str, item["stage"]), []).append(
            cast(float, item["elapsed_seconds"])
        )
        if type(item["model_load_seconds"]) is float:
            by_stage.setdefault("model_loading", []).append(item["model_load_seconds"])
    stage_summary = {
        stage: {
            "samples": len(values),
            "seconds": values,
            "maximum_seconds": max(values),
        }
        for stage, values in sorted(by_stage.items())
    }
    versions = records(root)
    report: dict[str, object] = {
        "report_contract_version": "incremental-shadow-local-report-v1",
        "stream_id": manifest["stream_id"],
        "source_delta_table_id": manifest["source_delta_table_id"],
        "topic_id": manifest["topic_id"],
        "consumer_group": manifest["group_id"],
        "candidate_id": manifest["candidate_id"],
        "versions": [
            {
                "delta_version": record["delta_version"],
                "ready_event_count": record["ready_event_count"],
                "segment_id": record["segment_id"],
                "publication_batch_id": record["publication_batch_id"],
            }
            for record in versions
        ],
        "reconciliation": summary,
        "timing": stage_summary,
        "scope": "LOCAL_INCREMENTAL_RESEARCH_ONLY",
        "non_guarantees": [
            "No production throughput, availability or latency SLO",
            "No globally exactly-once model execution",
            "No payment enforcement or model promotion",
        ],
    }
    report_id = digest(report)
    target = root / "reports" / report_id
    markdown = (
        "# Phase 27 local incremental shadow report\n\n"
        f"Stream `{manifest['stream_id']}`; source table `{manifest['source_delta_table_id']}`; "
        f"topic ID `{manifest['topic_id']}`.\n\n"
        f"{summary['unique_source_ready_events']} unique ready events reconciled through "
        f"{summary['physical_kafka_messages']} physical Kafka records and "
        f"{summary['terminal_inference_results']} terminal local results.\n\n"
        "Timing samples in `report.json` are monotonic local operation measurements; "
        "missing stages were not measured. No production capacity or SLO is inferred.\n\n"
        "The candidate remains registered for research only; this fixture is not the "
        "historical Scala-generated feature stream and does not prove original "
        "online availability.\n"
    ).encode()
    if target.exists():
        if (
            read_object(target / "report.json") != report
            or (target / "report.md").read_bytes() != markdown
        ):
            raise AvailabilityError("Existing local report differs")
    else:
        publish(target, {"report.json": canonical(report), "report.md": markdown})
    return {"report_id": report_id, "path": str(target), **report}
