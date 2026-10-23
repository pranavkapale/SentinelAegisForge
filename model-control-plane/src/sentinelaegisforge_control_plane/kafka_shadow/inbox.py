"""Read-only verification of the Scala-sealed, fully reconciled shadow inbox."""

import hashlib
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.availability.contract import (
    AvailabilityError,
    digest,
    exact_directory,
    file_hash,
    read_lines,
    read_object,
)
from sentinelaegisforge_control_plane.availability.kafka_export import inspect_batch
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

CONTRACT = "research-shadow-inbox-v1"
VERIFICATION = "research-shadow-inbox-verification-v1"
GROUP = "sentinel-feature-ready-shadow-v1"
FILES = {"inbox_manifest.json", "ready_events.jsonl"}


def _receipt_inventory(
    root: Path, events: list[dict[str, object]], manifest: dict[str, object]
) -> str:
    if root.is_symlink() or not root.is_dir():
        raise AvailabilityError("Shadow receipt root is missing or linked")
    if (root / "receiver_identity.json").is_symlink():
        raise AvailabilityError("Linked shadow receiver identity")
    identity = read_object(root / "receiver_identity.json")
    expected_identity = {
        "receipt_store_contract_version": "kafka-feature-ready-receipt-store-v1",
        "publication_batch_id": manifest["publication_batch_id"],
        "outbox_id": manifest["outbox_id"],
        "group_id": GROUP,
        "topic": manifest["topic"],
        "cluster_id": manifest["cluster_id"],
        "topic_id": manifest["topic_id"],
    }
    if identity != expected_identity:
        raise AvailabilityError("Shadow receipt-store binding differs")
    receipts = root / "receipts"
    if receipts.is_symlink() or not receipts.is_dir():
        raise AvailabilityError("Shadow receipts directory is missing or linked")
    by_id = {cast(str, event["ready_event_id"]): event for event in events}
    files = sorted(receipts.iterdir(), key=lambda item: item.name)
    if {item.name for item in files} != {f"{key}.json" for key in by_id}:
        raise AvailabilityError("Incomplete or unexpected shadow receipt membership")
    inventory: list[str] = []
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise AvailabilityError("Linked/non-file receipt")
        receipt = read_object(path)
        event = by_id[path.stem]
        envelope_hash = hashlib.sha256(canonical_json(event)).hexdigest()
        expected = {
            "receipt_contract_version": "kafka-feature-ready-receipt-v1",
            "ready_event_id": path.stem,
            "event_id": event["event_id"],
            "outbox_id": manifest["outbox_id"],
            "topic": manifest["topic"],
            "topic_id": manifest["topic_id"],
            "envelope_fingerprint": envelope_hash,
        }
        if any(receipt.get(key) != value for key, value in expected.items()):
            raise AvailabilityError("Shadow receipt content differs from source envelope")
        partition = receipt.get("first_topic_partition")
        offset = receipt.get("first_topic_offset")
        if (
            type(partition) is not int
            or not 0 <= partition < 3
            or type(offset) is not int
            or offset < 0
        ):
            raise AvailabilityError("Invalid shadow receipt Kafka coordinates")
        receipt_id = digest(
            {
                "ready_event_id": path.stem,
                "outbox_id": manifest["outbox_id"],
                "topic_id": manifest["topic_id"],
                "partition": partition,
                "offset": offset,
                "envelope_fingerprint": envelope_hash,
            }
        )
        if receipt.get("receipt_id") != receipt_id or set(receipt) != set(expected) | {
            "receipt_id",
            "first_topic_partition",
            "first_topic_offset",
        }:
            raise AvailabilityError("Shadow receipt identity differs")
        inventory.append(f"{path.name}:{file_hash(path)}")
    return digest(inventory)


def inspect_inbox(path: Path) -> tuple[dict[str, object], list[dict[str, object]]]:
    """Recheck immutable bytes, source batch, receipt identities and semantic inbox ID."""
    exact_directory(path, FILES)
    manifest = read_object(path / "inbox_manifest.json")
    events = read_lines(path / "ready_events.jsonl")
    required = {
        "inbox_contract_version",
        "verification_contract_version",
        "publication_batch_id",
        "outbox_id",
        "cluster_id",
        "topic_id",
        "topic",
        "group_id",
        "ready_event_count",
        "ready_event_ids_sha256",
        "source_envelopes_sha256",
        "inbox_id",
        "batch_path",
        "receipt_root",
        "receipt_inventory_sha256",
        "broker_physical_records",
        "broker_duplicate_records",
        "consumer_duplicate_deliveries",
    } | {
        f"{prefix}_{partition}"
        for prefix in ("broker_beginning", "broker_end", "group_committed_next")
        for partition in range(3)
    }
    if set(manifest) != required:
        raise AvailabilityError("Shadow inbox manifest fields differ")
    if (
        manifest["inbox_contract_version"] != CONTRACT
        or manifest["verification_contract_version"] != VERIFICATION
        or manifest["group_id"] != GROUP
        or manifest["topic"] != "features.ready"
        or type(manifest["ready_event_count"]) is not int
        or manifest["ready_event_count"] != len(events)
        or not 1 <= len(events) <= 5000
    ):
        raise AvailabilityError("Unsupported or incomplete shadow inbox")
    batch_path = Path(cast(str, manifest["batch_path"]))
    batch = inspect_batch(batch_path)
    raw = (batch_path / "records.jsonl").read_bytes()
    if (
        batch["publication_batch_id"] != manifest["publication_batch_id"]
        or batch["outbox_id"] != manifest["outbox_id"]
        or batch["topic"] != manifest["topic"]
        or (path / "ready_events.jsonl").read_bytes() != raw
        or file_hash(path / "ready_events.jsonl") != manifest["source_envelopes_sha256"]
        or digest(sorted(cast(str, item["ready_event_id"]) for item in events))
        != manifest["ready_event_ids_sha256"]
    ):
        raise AvailabilityError("Shadow inbox differs from verified source batch")
    if len({item["ready_event_id"] for item in events}) != len(events):
        raise AvailabilityError("Duplicate shadow ready-event identity")
    basis_keys = (
        "inbox_contract_version",
        "verification_contract_version",
        "publication_batch_id",
        "outbox_id",
        "cluster_id",
        "topic_id",
        "topic",
        "group_id",
        "ready_event_count",
        "ready_event_ids_sha256",
        "source_envelopes_sha256",
    )
    basis = {key: manifest[key] for key in basis_keys}
    if path.name != digest(basis) or manifest["inbox_id"] != path.name:
        raise AvailabilityError("Shadow inbox semantic identity differs")
    if (
        _receipt_inventory(Path(cast(str, manifest["receipt_root"])), events, manifest)
        != manifest["receipt_inventory_sha256"]
    ):
        raise AvailabilityError("Shadow receipt inventory changed")
    observed_physical = 0
    for partition in range(3):
        beginning = manifest[f"broker_beginning_{partition}"]
        end = manifest[f"broker_end_{partition}"]
        committed = manifest[f"group_committed_next_{partition}"]
        if any(type(value) is not int for value in (beginning, end, committed)) or not (
            0 <= cast(int, beginning) <= cast(int, end) == cast(int, committed)
        ):
            raise AvailabilityError("Shadow inbox broker/offset evidence is invalid")
        observed_physical += cast(int, end) - cast(int, beginning)
    physical = manifest["broker_physical_records"]
    duplicate = manifest["broker_duplicate_records"]
    if (
        type(physical) is not int
        or type(duplicate) is not int
        or physical != observed_physical
        or physical - duplicate != len(events)
    ):
        raise AvailabilityError("Shadow inbox physical/unique broker count differs")
    return manifest, events
