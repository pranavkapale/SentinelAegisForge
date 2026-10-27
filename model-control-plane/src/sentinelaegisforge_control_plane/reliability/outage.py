"""Fail-closed local broker interruption and research backlog evidence."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import cast

from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import AvailabilityError, read_object
from sentinelaegisforge_control_plane.availability.kafka_export import inspect_batch
from sentinelaegisforge_control_plane.availability.outbox import inspect_outbox
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.incremental_shadow.operations import (
    inspect as inspect_stream,
)
from sentinelaegisforge_control_plane.incremental_shadow.operations import records
from sentinelaegisforge_control_plane.incremental_shadow.progress import (
    authorize_publication,
    progress,
)
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import inspect_identity
from sentinelaegisforge_control_plane.stateful_shadow_integration.reconciliation import verify_final

from .contract import ReliabilityError

ATTEMPT_CONTRACT = "phase29-broker-failed-publication-attempt-v1"


def backlog_snapshot(stream: Path) -> dict[str, object]:
    """Separate verified acknowledgements from durable consumer acceptance."""
    manifest = inspect_stream(stream)
    summary = progress(stream)
    acknowledged = 0
    for record in records(stream):
        batch_id = record["publication_batch_id"]
        if batch_id is None:
            continue
        batch = inspect_batch(Path(cast(str, record["batch_path"])))
        events = inspect_outbox(Path(cast(str, batch["outbox_path"])))[1]
        by_id = {cast(str, event["ready_event_id"]): event for event in events}
        for path in (stream / "journals" / cast(str, batch_id) / "acknowledgements").glob("*.json"):
            ack = read_object(path)
            if (
                path.stem not in by_id
                or ack.get("ready_event_id") != path.stem
                or ack.get("event_id") != by_id[path.stem]["event_id"]
                or ack.get("publication_batch_id") != batch_id
                or ack.get("topic_id") != manifest["topic_id"]
                or ack.get("cluster_id") != manifest["cluster_id"]
            ):
                raise ReliabilityError("Backlog acknowledgement identity differs")
            acknowledged += 1
    prepared = cast(int, summary["prepared_ready_events"])
    accepted = cast(int, summary["accepted_ready_events"])
    if not 0 <= accepted <= acknowledged <= prepared:
        raise ReliabilityError("Inconsistent layered backlog counts")
    return {
        "discovery_backlog_versions": summary["discovery_backlog_versions"],
        "publication_backlog_ready_events": prepared - acknowledged,
        "acceptance_backlog_ready_events": acknowledged - accepted,
        "inference_backlog_ready_events": summary["inference_backlog_ready_events"],
        "prepared": prepared,
        "acknowledged": acknowledged,
        "accepted": accepted,
        "terminal_results": summary["completed_inference_results"],
    }


def record_backlog(integration: Path, stream: Path, label: str) -> dict[str, object]:
    if label not in {"recovered-published", "research-paused", "final"}:
        raise ReliabilityError("Unsupported controlled backlog observation point")
    identity = inspect_identity(integration)
    manifest = inspect_stream(stream)
    if manifest["source_delta_path"] != cast(dict[str, str], identity["paths"])["statistical"]:
        raise ReliabilityError("Backlog source lineage differs")
    snapshot = backlog_snapshot(stream)
    path = integration / "reliability" / f"backlog-{label}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(canonical_json(snapshot))
    return snapshot


def record_backpressure(integration: Path, stream: Path, batch_id: str) -> dict[str, object]:
    identity = inspect_identity(integration)
    manifest = inspect_stream(stream)
    if manifest["source_delta_path"] != cast(dict[str, str], identity["paths"])["statistical"]:
        raise ReliabilityError("Backpressure source lineage differs")
    snapshot = backlog_snapshot(stream)
    try:
        authorize_publication(stream, batch_id)
    except AvailabilityError as error:
        if "PAUSED_BACKPRESSURE" not in str(error):
            raise
        refusal = str(error)
    else:
        raise ReliabilityError("Publication was not refused by the real backpressure gate")
    record = next(item for item in records(stream) if item["publication_batch_id"] == batch_id)
    batch = inspect_batch(Path(cast(str, record["batch_path"])))
    value = {
        "batch_id": batch_id,
        "refusal": refusal,
        "backlogs": snapshot,
        "projected_inference_backlog": cast(int, snapshot["inference_backlog_ready_events"])
        + cast(int, batch["ready_event_count"]),
        "configured_limit": cast(dict[str, int], manifest["limits"])[
            "maximum_accepted_unprocessed_events"
        ],
    }
    path = integration / "reliability" / "backpressure-refusal.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(canonical_json(value))
    return value


def record_failed_publication(
    integration: Path, stream: Path, batch_id: str, log: Path, exit_code: int
) -> dict[str, object]:
    """Bind an actual nonzero broker publication attempt before recovery."""
    identity = inspect_identity(integration)
    manifest = inspect_stream(stream)
    if (
        exit_code == 0
        or manifest["source_delta_path"] != cast(dict[str, str], identity["paths"])["statistical"]
    ):
        raise ReliabilityError("Broker outage attempt did not fail on this feature lineage")
    pending = next(
        (item for item in records(stream) if item["publication_batch_id"] == batch_id), None
    )
    if pending is None or pending["batch_path"] is None:
        raise ReliabilityError("No verified committed-feature outbox for the failed publication")
    if (stream / "acceptance" / f"{batch_id}.json").exists():
        raise ReliabilityError("Publication already has a durable research acceptance")
    raw = log.read_bytes()
    if not raw or not any(
        marker in raw
        for marker in (
            b"TimeoutException",
            b"KafkaException",
            b"Connection to node",
            b"Unable to connect",
        )
    ):
        raise ReliabilityError("Failed log does not identify a Kafka connectivity failure")
    source = DeltaTable(cast(dict[str, str], identity["paths"])["statistical"])
    record: dict[str, object] = {
        "contract_version": ATTEMPT_CONTRACT,
        "integration_id": identity["integration_id"],
        "stream_id": manifest["stream_id"],
        "batch_id": batch_id,
        "outbox_path": pending["batch_path"],
        "statistical_table_id": str(source.metadata().id),
        "statistical_delta_version": source.version(),
        "exit_code": exit_code,
        "failure_log_path": str(log.resolve()),
        "failure_log_sha256": hashlib.sha256(raw).hexdigest(),
        "acceptance_before_recovery": False,
        "delivery_certainty": "UNKNOWN; a timeout alone does not prove non-delivery",
        "backlogs": backlog_snapshot(stream),
    }
    path = integration / "reliability" / "broker-failed-attempt.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(canonical_json(record))
    return record


def verify(integration: Path, stream: Path) -> dict[str, object]:
    identity = inspect_identity(integration)
    final = verify_final(integration, stream)
    attempt = read_object(integration / "reliability" / "broker-failed-attempt.json")
    if (
        attempt.get("contract_version") != ATTEMPT_CONTRACT
        or attempt.get("integration_id") != identity["integration_id"]
    ):
        raise ReliabilityError("Broker outage attempt identity differs")
    log = Path(cast(str, attempt["failure_log_path"]))
    if hashlib.sha256(log.read_bytes()).hexdigest() != attempt["failure_log_sha256"]:
        raise ReliabilityError("Broker outage failure log changed")
    batch = cast(str, attempt["batch_id"])
    acceptance = stream / "acceptance" / f"{batch}.json"
    if not acceptance.is_file():
        raise ReliabilityError("Failed broker publication has no later durable acceptance")
    if not any(item["publication_batch_id"] == batch for item in records(stream)):
        raise ReliabilityError("Recovered batch identity differs from failed outbox")
    first = read_object(integration / "handoffs" / "0.json")
    second = read_object(integration / "handoffs" / "1.json")
    first_progress = cast(dict[str, object], first["progress"])
    second_progress = cast(dict[str, object], second["progress"])
    if (
        not first.get("inference_intentionally_paused")
        or cast(int, first_progress["inference_backlog_ready_events"]) <= 0
    ):
        raise ReliabilityError("Research pause has no accepted backlog evidence")
    if not second.get("backpressure_pause_observed"):
        raise ReliabilityError("Projected research backpressure refusal was not observed")
    refusal = read_object(integration / "reliability" / "backpressure-refusal.json")
    if cast(int, refusal["projected_inference_backlog"]) <= cast(int, refusal["configured_limit"]):
        raise ReliabilityError("Backpressure evidence has no exceeded projected bound")
    snapshots = {
        label: read_object(integration / "reliability" / f"backlog-{label}.json")
        for label in ("recovered-published", "research-paused", "final")
    }
    if cast(int, snapshots["recovered-published"]["acceptance_backlog_ready_events"]) <= 0:
        raise ReliabilityError("Missing acknowledged-but-unaccepted backlog observation")
    if any(
        cast(int, snapshots["final"][key]) != 0
        for key in (
            "discovery_backlog_versions",
            "publication_backlog_ready_events",
            "acceptance_backlog_ready_events",
            "inference_backlog_ready_events",
        )
    ):
        raise ReliabilityError("Final layered backlog did not drain")
    final_research = cast(dict[str, object], final["research"])
    if final_research["terminal_inference_results"] != final["source_event_count"]:
        raise ReliabilityError("Recovered research results do not reconcile")
    return {
        "broker_outage": {"status": "VERIFIED", "attempt": attempt, "acceptance": str(acceptance)},
        "research_pause": {"status": "VERIFIED", "progress": first_progress},
        "backpressure_refusal": {
            "status": "VERIFIED",
            "progress": second_progress,
            "refusal": refusal,
            "snapshots": snapshots,
        },
        "recovered_results": {"status": "VERIFIED", "final": final_research},
    }
