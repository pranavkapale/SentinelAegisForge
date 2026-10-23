"""Phase 26 sealed-notification, exact-source and crash-safe logical result contracts."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from test_availability import feature, published

from sentinelaegisforge_control_plane.availability.contract import (
    AvailabilityError,
    digest,
    file_hash,
    read_lines,
)
from sentinelaegisforge_control_plane.availability.kafka_export import prepare_batch
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.kafka_shadow.contract import (
    INFERENCE,
    RequestInferenceError,
    ShadowKafkaError,
    request_id,
)
from sentinelaegisforge_control_plane.kafka_shadow.inbox import inspect_inbox
from sentinelaegisforge_control_plane.kafka_shadow.results import (
    claim_result,
    counts,
    inspect_result,
    result_path,
)
from sentinelaegisforge_control_plane.kafka_shadow.worker import (
    WorkerConfig,
    _source_rows,
    evaluate_one,
    process_resolved,
)
from sentinelaegisforge_control_plane.registry.run import RegistryConfig


def _sealed(tmp_path: Path) -> tuple[Path, Path, list[dict[str, object]]]:
    source, _, outbox = published(tmp_path)
    batch = prepare_batch(outbox, tmp_path / "batches", maximum=5)
    events = read_lines(batch / "records.jsonl")
    batch_manifest = json.loads((batch / "publish_batch_manifest.json").read_bytes())
    root = tmp_path / "receipts"
    receipts = root / "receipts"
    receipts.mkdir(parents=True)
    binding = {
        "receipt_store_contract_version": "kafka-feature-ready-receipt-store-v1",
        "publication_batch_id": batch.name,
        "outbox_id": batch_manifest["outbox_id"],
        "group_id": "sentinel-feature-ready-shadow-v1",
        "topic": "features.ready",
        "cluster_id": "test-cluster",
        "topic_id": "test-topic",
    }
    (root / "receiver_identity.json").write_bytes(canonical_json(binding))
    for offset, event in enumerate(events):
        envelope_hash = digest(event)
        ready = cast(str, event["ready_event_id"])
        receipt_basis = {
            "ready_event_id": ready,
            "outbox_id": batch_manifest["outbox_id"],
            "topic_id": "test-topic",
            "partition": 0,
            "offset": offset,
            "envelope_fingerprint": envelope_hash,
        }
        receipt = {
            "receipt_contract_version": "kafka-feature-ready-receipt-v1",
            "receipt_id": digest(receipt_basis),
            "ready_event_id": ready,
            "event_id": event["event_id"],
            "outbox_id": batch_manifest["outbox_id"],
            "topic": "features.ready",
            "topic_id": "test-topic",
            "first_topic_partition": 0,
            "first_topic_offset": offset,
            "envelope_fingerprint": envelope_hash,
        }
        (receipts / f"{ready}.json").write_bytes(canonical_json(receipt))
    inventory = [f"{path.name}:{file_hash(path)}" for path in sorted(receipts.iterdir())]
    basis = {
        "inbox_contract_version": "research-shadow-inbox-v1",
        "verification_contract_version": "research-shadow-inbox-verification-v1",
        "publication_batch_id": batch.name,
        "outbox_id": batch_manifest["outbox_id"],
        "cluster_id": "test-cluster",
        "topic_id": "test-topic",
        "topic": "features.ready",
        "group_id": "sentinel-feature-ready-shadow-v1",
        "ready_event_count": len(events),
        "ready_event_ids_sha256": digest(
            sorted(cast(str, item["ready_event_id"]) for item in events)
        ),
        "source_envelopes_sha256": file_hash(batch / "records.jsonl"),
    }
    inbox = tmp_path / "inboxes" / digest(basis)
    inbox.mkdir(parents=True)
    manifest: dict[str, object] = {
        **basis,
        "inbox_id": inbox.name,
        "batch_path": str(batch.resolve()),
        "receipt_root": str(root.resolve()),
        "receipt_inventory_sha256": digest(inventory),
        "broker_physical_records": len(events),
        "broker_duplicate_records": 0,
        "consumer_duplicate_deliveries": 0,
    }
    for partition in range(3):
        manifest[f"broker_beginning_{partition}"] = 0
        manifest[f"broker_end_{partition}"] = len(events) if partition == 0 else 0
        manifest[f"group_committed_next_{partition}"] = len(events) if partition == 0 else 0
    (inbox / "ready_events.jsonl").write_bytes((batch / "records.jsonl").read_bytes())
    (inbox / "inbox_manifest.json").write_bytes(canonical_json(manifest))
    return source, inbox, events


def test_sealed_inbox_binds_source_receipts_and_deterministic_identity(tmp_path: Path) -> None:
    _, inbox, events = _sealed(tmp_path)
    first, accepted = inspect_inbox(inbox)
    assert accepted == events
    assert first["inbox_id"] == inbox.name
    assert first["ready_event_count"] == 5
    assert inspect_inbox(inbox) == (first, accepted)
    assert len({event["ready_event_id"] for event in accepted}) == 5


def test_incomplete_and_corrupt_receipts_fail_closed(tmp_path: Path) -> None:
    _, inbox, events = _sealed(tmp_path)
    receipts = tmp_path / "receipts" / "receipts"
    (receipts / f"{events[0]['ready_event_id']}.json").unlink()
    with pytest.raises(AvailabilityError, match="membership"):
        inspect_inbox(inbox)
    (tmp_path / "other").mkdir()
    _, another, other_events = _sealed(tmp_path / "other")
    path = (
        tmp_path / "other" / "receipts" / "receipts" / f"{other_events[0]['ready_event_id']}.json"
    )
    content = path.read_bytes().replace(b"test-topic", b"wrong-topic")
    path.write_bytes(content)
    with pytest.raises(AvailabilityError, match="receipt"):
        inspect_inbox(another)


def test_inbox_rejects_mutated_identity_or_source_bytes(tmp_path: Path) -> None:
    _, inbox, _ = _sealed(tmp_path)
    manifest_path = inbox / "inbox_manifest.json"
    raw = manifest_path.read_bytes()
    manifest_path.write_bytes(raw.replace(b"sentinel-feature-ready-shadow-v1", b"wrong-group"))
    with pytest.raises(AvailabilityError, match="inbox"):
        inspect_inbox(inbox)


def test_inbox_requires_exact_committed_offsets_and_physical_range(tmp_path: Path) -> None:
    _, inbox, _ = _sealed(tmp_path)
    path = inbox / "inbox_manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["group_committed_next_0"] += 1
    path.write_bytes(canonical_json(manifest))
    with pytest.raises(AvailabilityError, match="broker/offset"):
        inspect_inbox(inbox)
    manifest["group_committed_next_0"] -= 1
    manifest["broker_physical_records"] += 1
    manifest["broker_duplicate_records"] += 1
    path.write_bytes(canonical_json(manifest))
    with pytest.raises(AvailabilityError, match="physical/unique"):
        inspect_inbox(inbox)


def test_exact_first_visible_source_resolution_and_wrong_fingerprint(tmp_path: Path) -> None:
    source, inbox, events = _sealed(tmp_path)
    manifest, _ = inspect_inbox(inbox)
    config = WorkerConfig(
        inbox,
        source,
        RegistryConfig(),
        "1",
        datetime(2030, 1, 1, tzinfo=UTC),
        datetime(2030, 1, 2, tzinfo=UTC),
        tmp_path / "results",
        tmp_path / "runs",
        tmp_path / "ledger",
        maximum_events=5,
    )
    resolved = _source_rows(config, manifest, events)
    assert [item[0]["first_visible_delta_version"] for item in resolved] == [0, 0, 1, 2, 2]
    assert {item[1]["event_id"] for item in resolved} == {item["event_id"] for item in events}
    corrupt = [{**event, "source_feature_row_fingerprint": "0" * 64} for event in events]
    with pytest.raises(ShadowKafkaError, match="wrong Delta version/row"):
        _source_rows(config, manifest, corrupt)


def _base() -> dict[str, object]:
    ready, candidate, model = "a" * 64, "b" * 64, "c" * 64
    return {
        "inference_contract_version": INFERENCE,
        "inference_request_id": request_id(ready, candidate, model),
        "shadow_inbox_id": "d" * 64,
        "ready_event_id": ready,
        "event_id": "event",
        "candidate_id": candidate,
        "model_tree_sha256": model,
        "registered_model_version": "1",
        "first_visible_delta_version": 0,
        "source_feature_row_fingerprint": "e" * 64,
        "score_start": "2030-01-01T00:00:00.000000Z",
        "score_end": "2030-01-02T00:00:00.000000Z",
    }


def test_request_identity_and_terminal_statuses_are_deterministic() -> None:
    base = _base()
    assert request_id("a" * 64, "b" * 64, "c" * 64) == base["inference_request_id"]
    assert request_id("a" * 64, "f" * 64, "c" * 64) != base["inference_request_id"]
    start = datetime(2030, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    row = feature(1)
    scored = evaluate_one(base, row, lambda _: 0.25, start, end)
    assert (scored["execution_status"], scored["fraud_probability_estimate"]) == ("SCORED", 0.25)
    assert {
        "fraud_label",
        "risk_disposition",
        "customer_id",
        "amount",
        "currency",
        "prior_amount_mean",
        "scenario_profile",
    }.isdisjoint(scored)
    assert (
        evaluate_one(base, row, lambda _: 0.25, end, end + timedelta(days=1))["execution_status"]
        == "OUT_OF_SCOPE"
    )
    assert (
        evaluate_one(base, {**row, "currency": "bad"}, lambda _: 0.25, start, end)[
            "execution_status"
        ]
        == "INPUT_REJECTED"
    )

    def failure(_: dict[str, object]) -> float:
        raise RequestInferenceError("private diagnostic")

    failed = evaluate_one(base, row, failure, start, end)
    assert failed["execution_status"] == "INFERENCE_FAILED"
    assert "private diagnostic" not in str(failed)


def test_durable_result_restart_collision_and_corruption(tmp_path: Path) -> None:
    result = evaluate_one(
        _base(),
        feature(1),
        lambda _: 0.25,
        datetime(2030, 1, 1, tzinfo=UTC),
        datetime(2030, 1, 2, tzinfo=UTC),
    )
    path = result_path(
        tmp_path, cast(str, result["candidate_id"]), cast(str, result["inference_request_id"])
    )
    assert claim_result(path, result)
    assert not claim_result(path, result)
    assert inspect_result(path, result) == result
    assert counts([result], 2)["unresolved"] == 1
    with pytest.raises(ShadowKafkaError, match="Conflicting"):
        claim_result(path, {**result, "fraud_probability_estimate": 0.75})
    path.write_bytes(b"{}\n")
    with pytest.raises(ShadowKafkaError, match="contract"):
        inspect_result(path)


def test_interrupted_worker_reuses_durable_result_without_rescoring(tmp_path: Path) -> None:
    source, inbox_path, events = _sealed(tmp_path)
    inbox, _ = inspect_inbox(inbox_path)
    config = WorkerConfig(
        inbox_path,
        source,
        RegistryConfig(),
        "1",
        datetime(2030, 1, 1, tzinfo=UTC),
        datetime(2030, 1, 2, tzinfo=UTC),
        tmp_path / "results",
        tmp_path / "runs",
        tmp_path / "ledger",
        maximum_events=5,
        inference_batch_size=2,
    )
    resolved = _source_rows(config, inbox, events)
    calls = 0

    def scorer(_: dict[str, object]) -> float:
        nonlocal calls
        calls += 1
        return 0.25

    with pytest.raises(ShadowKafkaError, match="Injected interruption"):
        process_resolved(config, inbox, resolved, "b" * 64, "c" * 64, scorer, fail_after_result=1)
    assert calls == 1
    completed, new = process_resolved(config, inbox, resolved, "b" * 64, "c" * 64, scorer)
    assert (new, len(completed), calls) == (4, 5, 5)
    repeated, new = process_resolved(config, inbox, resolved, "b" * 64, "c" * 64, scorer)
    assert (new, repeated, calls) == (0, completed, 5)
    assert counts(completed, 5)["unresolved"] == 0
