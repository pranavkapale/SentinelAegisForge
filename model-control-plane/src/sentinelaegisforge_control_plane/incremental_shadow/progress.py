"""Read-only multi-batch receipt/result reconciliation and backpressure gates."""

import hashlib
import math
from pathlib import Path
from typing import cast

from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import (
    AvailabilityError,
    digest,
    read_object,
)
from sentinelaegisforge_control_plane.availability.kafka_export import inspect_batch
from sentinelaegisforge_control_plane.availability.outbox import canonical, inspect_outbox

from .operations import (
    ACCEPTANCE_CONTRACT,
    CONTRACT,
    GROUP,
    RESULT_CONTRACT,
    Limits,
    inspect,
    logical_request_id,
    records,
)


def _offsets(record: dict[str, object], prefix: str) -> dict[int, int]:
    values = {part: record.get(f"{prefix}_{part}") for part in range(3)}
    if any(type(value) is not int or value < 0 for value in values.values()):
        raise AvailabilityError("Invalid operational per-partition offset evidence")
    return cast(dict[int, int], values)


def _starting_offsets(root: Path, manifest: dict[str, object]) -> dict[int, int]:
    fence = read_object(Path(cast(str, manifest["fence_path"])))
    return _offsets(fence, "end")


def inspect_acceptance(
    root: Path, record: dict[str, object], manifest: dict[str, object], start: dict[int, int]
) -> tuple[dict[str, object], list[dict[str, object]]]:
    batch_id = cast(str, record["publication_batch_id"])
    batch_path = Path(cast(str, record["batch_path"]))
    batch = inspect_batch(batch_path)
    outbox, events = inspect_outbox(Path(cast(str, batch["outbox_path"])))
    path = root / "acceptance" / f"{batch_id}.json"
    accepted = read_object(path)
    expected = {
        "contract_version": ACCEPTANCE_CONTRACT,
        "group_id": GROUP,
        "cluster_id": manifest["cluster_id"],
        "topic_id": manifest["topic_id"],
        "topic": manifest["topic"],
        "publication_batch_id": batch_id,
        "outbox_id": outbox["outbox_id"],
        "ready_event_count": len(events),
    }
    if any(accepted.get(key) != value for key, value in expected.items()):
        raise AvailabilityError("Operational acceptance identity differs")
    if set(accepted) != set(expected) | {"physical_record_count"} | {
        f"{prefix}_{part}" for prefix in ("start", "end") for part in range(3)
    }:
        raise AvailabilityError("Operational acceptance fields differ")
    actual_start = _offsets(accepted, "start")
    end = _offsets(accepted, "end")
    if actual_start != start or any(end[part] < start[part] for part in range(3)):
        raise AvailabilityError("Operational batch skips or overlaps offset boundary")
    by_id = {cast(str, event["ready_event_id"]): event for event in events}
    receipt_root = root / "receipts" / batch_id
    identity = read_object(receipt_root / "receiver_identity.json")
    if identity != {
        "receipt_store_contract_version": "kafka-feature-ready-receipt-store-v1",
        "publication_batch_id": batch_id,
        "outbox_id": outbox["outbox_id"],
        "group_id": GROUP,
        "topic": manifest["topic"],
        "cluster_id": manifest["cluster_id"],
        "topic_id": manifest["topic_id"],
    }:
        raise AvailabilityError("Operational receipt root binding differs")
    receipt_files = list((receipt_root / "receipts").glob("*.json"))
    if {item.stem for item in receipt_files} != set(by_id):
        raise AvailabilityError("Operational receipt inventory is incomplete")
    observed: dict[tuple[int, int], str] = {}
    for path in (receipt_root / "observations").glob("*.json"):
        item = read_object(path)
        part = item.get("partition")
        offset = item.get("offset")
        ready_id = item.get("ready_event_id")
        if (
            type(part) is not int
            or type(offset) is not int
            or part not in start
            or not start[part] <= offset < end[part]
            or path.name != f"{part}-{offset}.json"
            or ready_id not in by_id
            or item.get("topic_id") != manifest["topic_id"]
            or item.get("envelope_fingerprint")
            != hashlib.sha256(canonical(by_id[cast(str, ready_id)])).hexdigest()
        ):
            raise AvailabilityError("Invalid operational Kafka observation")
        observed[(part, offset)] = cast(str, ready_id)
    expected_coordinates = {
        (part, offset) for part in range(3) for offset in range(start[part], end[part])
    }
    if (
        set(observed) != expected_coordinates
        or set(observed.values()) != set(by_id)
        or accepted["physical_record_count"] != len(expected_coordinates)
    ):
        raise AvailabilityError("Operational accepted physical range does not reconcile")
    for path in receipt_files:
        item = read_object(path)
        ready_id = path.stem
        if (
            item.get("ready_event_id") != ready_id
            or item.get("event_id") != by_id[ready_id]["event_id"]
            or item.get("outbox_id") != outbox["outbox_id"]
            or item.get("topic_id") != manifest["topic_id"]
            or observed.get(
                (
                    cast(int, item.get("first_topic_partition")),
                    cast(int, item.get("first_topic_offset")),
                )
            )
            != ready_id
            or item.get("receipt_id")
            != digest(
                {
                    "ready_event_id": ready_id,
                    "outbox_id": outbox["outbox_id"],
                    "topic_id": manifest["topic_id"],
                    "partition": item.get("first_topic_partition"),
                    "offset": item.get("first_topic_offset"),
                    "envelope_fingerprint": hashlib.sha256(canonical(by_id[ready_id])).hexdigest(),
                }
            )
        ):
            raise AvailabilityError("Operational receipt differs from source or Kafka observation")
    journal = root / "journals" / batch_id
    journal_identity = read_object(journal / "journal_identity.json")
    if any(
        journal_identity.get(key) != value
        for key, value in {
            "publication_batch_id": batch_id,
            "outbox_id": outbox["outbox_id"],
            "cluster_id": manifest["cluster_id"],
            "topic_id": manifest["topic_id"],
        }.items()
    ):
        raise AvailabilityError("Publication journal identity changed")
    acks = list((journal / "acknowledgements").glob("*.json"))
    if {item.stem for item in acks} != set(by_id):
        raise AvailabilityError("Publication acknowledgements incomplete")
    for path in acks:
        ack = read_object(path)
        if (
            ack.get("ready_event_id") != path.stem
            or ack.get("event_id") != by_id[path.stem]["event_id"]
            or ack.get("topic_id") != manifest["topic_id"]
            or observed.get((cast(int, ack.get("partition")), cast(int, ack.get("offset"))))
            != path.stem
        ):
            raise AvailabilityError("Publication acknowledgement absent from accepted range")
    return accepted, events


def _results(root: Path, manifest: dict[str, object]) -> dict[str, dict[str, object]]:
    directory = root / "results" / cast(str, manifest["candidate_id"])
    results: dict[str, dict[str, object]] = {}
    for path in directory.glob("*.json") if directory.exists() else []:
        item = read_object(path)
        ready_id = item.get("ready_event_id")
        status = item.get("execution_status")
        probability = item.get("fraud_probability_estimate")
        failure = item.get("failure_code")
        if (
            set(item)
            != {
                "contract_version",
                "stream_id",
                "request_id",
                "ready_event_id",
                "event_id",
                "publication_batch_id",
                "candidate_id",
                "model_tree_sha256",
                "first_visible_delta_version",
                "source_feature_row_fingerprint",
                "score_start",
                "score_end",
                "execution_status",
                "fraud_probability_estimate",
                "failure_code",
            }
            or status not in {"SCORED", "OUT_OF_SCOPE", "INPUT_REJECTED", "INFERENCE_FAILED"}
            or item.get("contract_version") != RESULT_CONTRACT
            or item.get("stream_id") != manifest["stream_id"]
            or item.get("candidate_id") != manifest["candidate_id"]
            or item.get("model_tree_sha256") != manifest["model_tree_sha256"]
            or item.get("request_id")
            != logical_request_id(
                str(ready_id),
                cast(str, manifest["candidate_id"]),
                cast(str, manifest["model_tree_sha256"]),
            )
            or path.stem != item["request_id"]
            or ready_id in results
            or (
                status == "SCORED"
                and (
                    type(probability) is not float
                    or not math.isfinite(probability)
                    or not 0 <= probability <= 1
                    or failure is not None
                )
            )
            or (
                status != "SCORED"
                and (
                    probability is not None
                    or failure
                    != {
                        "OUT_OF_SCOPE": None,
                        "INPUT_REJECTED": "INVALID_MODEL_INPUT",
                        "INFERENCE_FAILED": "MODEL_INFERENCE_ERROR",
                    }.get(str(status))
                )
            )
        ):
            raise AvailabilityError("Invalid or conflicting operational inference result")
        results[cast(str, ready_id)] = item
    return results


def progress(root: Path) -> dict[str, object]:
    manifest = inspect(root)
    versions = records(root)
    limits = Limits(**cast(dict[str, int], manifest["limits"]))
    start = _starting_offsets(root, manifest)
    accepted_ids: set[str] = set()
    prepared_ids: set[str] = set()
    accepted_batches = 0
    active_batches = 0
    physical = 0
    acceptance_closed = False
    for record in versions:
        if record["batch_path"] is None:
            continue
        batch = inspect_batch(Path(cast(str, record["batch_path"])))
        events = inspect_outbox(Path(cast(str, batch["outbox_path"])))[1]
        ids = {cast(str, item["ready_event_id"]) for item in events}
        if prepared_ids & ids:
            raise AvailabilityError("Ready-event identity reused across operational batches")
        prepared_ids.update(ids)
        acceptance = root / "acceptance" / f"{batch['publication_batch_id']}.json"
        if acceptance.exists():
            if acceptance_closed:
                raise AvailabilityError("Later batch accepted before earlier active batch")
            item, accepted_events = inspect_acceptance(root, record, manifest, start)
            start = _offsets(item, "end")
            physical += cast(int, item["physical_record_count"])
            accepted_ids.update(cast(str, event["ready_event_id"]) for event in accepted_events)
            accepted_batches += 1
        else:
            acceptance_closed = True
            journal = root / "journals" / cast(str, batch["publication_batch_id"])
            if journal.exists():
                active_batches += 1
    if active_batches > limits.maximum_active_publication_batches:
        raise AvailabilityError("More than one active publication batch")
    results = _results(root, manifest)
    if set(results) - accepted_ids:
        raise AvailabilityError("Result exists without a durable accepted notification")
    for ready_id, result in results.items():
        if result.get("ready_event_id") != ready_id:
            raise AvailabilityError("Operational result/request identity mismatch")
    source = Path(cast(str, manifest["source_delta_path"]))
    latest = DeltaTable(source).version()
    undiscovered = latest - (cast(int, manifest["first_delta_version"]) + len(versions)) + 1
    if undiscovered < 0:
        raise AvailabilityError("Source version regressed below discovered progress")
    pending = len(accepted_ids - results.keys())
    unaccepted = len(prepared_ids - accepted_ids)
    state = (
        "PAUSED_BACKPRESSURE"
        if pending >= limits.maximum_accepted_unprocessed_events
        else "PROCESSING"
        if undiscovered or unaccepted or pending
        else "IDLE"
    )
    return {
        "contract_version": CONTRACT,
        "stream_id": manifest["stream_id"],
        "source_delta_table_id": manifest["source_delta_table_id"],
        "last_fully_discovered_delta_version": cast(int, manifest["first_delta_version"])
        + len(versions)
        - 1,
        "latest_source_delta_version": latest,
        "discovery_backlog_versions": undiscovered,
        "transport_backlog_ready_events": unaccepted,
        "inference_backlog_ready_events": pending,
        "prepared_ready_events": len(prepared_ids),
        "accepted_ready_events": len(accepted_ids),
        "completed_inference_results": len(results),
        "accepted_batches": accepted_batches,
        "active_publication_batches": active_batches,
        "physical_kafka_records": physical,
        "duplicate_physical_notifications": physical - len(accepted_ids),
        "operational_state": state,
    }


def authorize_publication(root: Path, batch_id: str) -> dict[str, object]:
    """Fail closed before sending any bytes; inference may drain an accepted backlog."""
    summary = progress(root)
    manifest = inspect(root)
    limits = Limits(**cast(dict[str, int], manifest["limits"]))
    versions = records(root)
    pending = next((item for item in versions if item["publication_batch_id"] == batch_id), None)
    if pending is None:
        raise AvailabilityError("Unknown operational publication batch")
    if (root / "acceptance" / f"{batch_id}.json").exists():
        raise AvailabilityError("Batch already accepted; no automatic transport replay")
    first_pending = next(
        (
            item
            for item in versions
            if item["batch_path"] is not None
            and not (root / "acceptance" / f"{item['publication_batch_id']}.json").exists()
        ),
        None,
    )
    if first_pending != pending:
        raise AvailabilityError("Only the earliest unaccepted batch may be activated")
    batch = inspect_batch(Path(cast(str, pending["batch_path"])))
    active = cast(int, summary["active_publication_batches"])
    if active and not (root / "journals" / batch_id).exists():
        raise AvailabilityError("Another publication batch remains active")
    if (
        cast(int, summary["inference_backlog_ready_events"]) + cast(int, batch["ready_event_count"])
        > limits.maximum_accepted_unprocessed_events
    ):
        raise AvailabilityError("PAUSED_BACKPRESSURE: accepted inference capacity exhausted")
    if (
        cast(int, summary["transport_backlog_ready_events"])
        > limits.maximum_unpublished_ready_events
    ):
        raise AvailabilityError("Prepared transport backlog exceeds local bound")
    return batch


def reconcile(root: Path) -> dict[str, object]:
    """Require exact source, accepted-notification and terminal-result ID equality."""
    summary = progress(root)
    manifest = inspect(root)
    versions = records(root)
    source_ids: set[str] = set()
    accepted_ids: set[str] = set()
    provenance: dict[str, tuple[dict[str, object], str]] = {}
    start = _starting_offsets(root, manifest)
    for record in versions:
        if record["batch_path"] is None:
            continue
        batch = inspect_batch(Path(cast(str, record["batch_path"])))
        events = inspect_outbox(Path(cast(str, batch["outbox_path"])))[1]
        source_ids.update(cast(str, event["ready_event_id"]) for event in events)
        provenance.update(
            {
                cast(str, event["ready_event_id"]): (
                    event,
                    cast(str, batch["publication_batch_id"]),
                )
                for event in events
            }
        )
        if (root / "acceptance" / f"{batch['publication_batch_id']}.json").exists():
            accepted, accepted_events = inspect_acceptance(root, record, manifest, start)
            start = _offsets(accepted, "end")
            accepted_ids.update(cast(str, event["ready_event_id"]) for event in accepted_events)
    results = _results(root, manifest)
    for ready_id, result in results.items():
        if ready_id not in provenance:
            raise AvailabilityError("Operational result lacks a committed source ready event")
        event, batch_id = provenance[ready_id]
        if any(
            result.get(key) != value
            for key, value in {
                "event_id": event["event_id"],
                "publication_batch_id": batch_id,
                "first_visible_delta_version": event["first_visible_delta_version"],
                "source_feature_row_fingerprint": event["source_feature_row_fingerprint"],
            }.items()
        ):
            raise AvailabilityError("Operational result differs from accepted source provenance")
    if (
        summary["discovery_backlog_versions"] != 0
        or source_ids != accepted_ids
        or accepted_ids != set(results)
        or any(
            item["execution_status"] in {"INPUT_REJECTED", "INFERENCE_FAILED"}
            for item in results.values()
        )
    ):
        raise AvailabilityError(
            "Incremental source/publication/acceptance/result sets are incomplete or failed"
        )
    return {
        "stream_id": manifest["stream_id"],
        "unique_source_ready_events": len(source_ids),
        "published_and_accepted_ready_events": len(accepted_ids),
        "terminal_inference_results": len(results),
        "scored_results": sum(item["execution_status"] == "SCORED" for item in results.values()),
        "out_of_scope_results": sum(
            item["execution_status"] == "OUT_OF_SCOPE" for item in results.values()
        ),
        "physical_kafka_messages": summary["physical_kafka_records"],
        "duplicate_physical_messages": summary["duplicate_physical_notifications"],
        "reconciled": True,
    }
