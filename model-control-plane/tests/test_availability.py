"""Commit-visible features, immutable projection, and restartable local receipts."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from sentinelaegisforge_control_plane.availability.contract import AvailabilityError
from sentinelaegisforge_control_plane.availability.discovery import discover_versions
from sentinelaegisforge_control_plane.availability.handoff import LocalResearchReceiver
from sentinelaegisforge_control_plane.availability.outbox import (
    build_outbox,
    build_segments,
    inspect_outbox,
    inspect_segment,
)
from sentinelaegisforge_control_plane.datasets.dataset_contract import SOURCE_SCHEMA
from sentinelaegisforge_control_plane.datasets.feature_source import FeatureSnapshot


def feature(number: int, **changes: object) -> dict[str, object]:
    instant = datetime(2030, 1, 1, tzinfo=UTC)
    return {
        "event_id": str(UUID(int=number)),
        "transaction_id": f"transaction-{number}",
        "customer_id": "customer-a",
        "merchant_id": "merchant-a",
        "event_time": instant,
        "ingestion_time": instant,
        "amount": Decimal("123.4567"),
        "currency": "USD",
        "country": "US",
        "device_id": "device-a",
        "ip_address": "192.0.2.1",
        "transaction_type": "CARD_PAYMENT",
        "schema_version": 1,
        "kafka_key": "customer-a",
        "kafka_topic": "transactions.raw",
        "kafka_partition": 0,
        "kafka_offset": number,
        "kafka_timestamp": instant,
        "prior_transaction_count_5m": 0,
        "prior_amount_sum_10m": Decimal("0.0000"),
        "prior_amount_observation_count": 0,
        "prior_amount_mean": None,
        "prior_amount_stddev": None,
        "amount_zscore": None,
        "statistical_feature_status": "NO_HISTORY",
        **changes,
    }


def append(path: Path, rows: list[dict[str, object]]) -> None:
    write_deltalake(
        path,
        pa.Table.from_pylist(rows, schema=SOURCE_SCHEMA),
        mode="append" if path.exists() else "error",
    )


def fixture(tmp_path: Path) -> Path:
    path = tmp_path / "statistical"
    append(path, [feature(1), feature(2)])
    append(path, [feature(3)])
    append(path, [feature(4), feature(5)])
    return path


def published(tmp_path: Path) -> tuple[Path, list[Path], Path]:
    source = fixture(tmp_path)
    segments = build_segments(source, 0, 2, tmp_path / "segments")
    outbox = build_outbox(segments, tmp_path / "outboxes")
    return source, segments, outbox


def test_fresh_commits_first_visible_versions_and_no_future_leakage(tmp_path: Path) -> None:
    source, segments, outbox = published(tmp_path)
    differences = discover_versions(source, 0, 2)
    assert [len(diff.new_rows) for diff in differences] == [2, 1, 2]
    assert [len(diff.current_rows) for diff in differences] == [2, 3, 5]
    manifest, ready = inspect_outbox(outbox)
    assert manifest["ready_event_count"] == 5
    assert len({event["ready_event_id"] for event in ready}) == 5
    assert [event["first_visible_delta_version"] for event in ready] == [0, 0, 1, 2, 2]
    assert all(event["availability_state"] == "DELTA_COMMITTED" for event in ready)
    assert set(ready[0]) == {
        "availability_contract_version",
        "ready_event_id",
        "event_id",
        "source_delta_table_id",
        "first_visible_delta_version",
        "source_schema_fingerprint",
        "source_feature_row_fingerprint",
        "availability_state",
    }
    assert {event["event_id"] for event in ready[:2]} == set(differences[0].new_rows)
    assert {event["event_id"] for event in ready[2:3]} == set(differences[1].new_rows)
    assert [inspect_segment(segment)[0]["new_event_count"] for segment in segments] == [2, 1, 2]
    for event in ready:
        version = cast(int, event["first_visible_delta_version"])
        event_id = cast(str, event["event_id"])
        assert event_id in discover_versions(source, version, version)[0].current_rows
        if version > 0:
            assert event_id not in discover_versions(source, version, version)[0].previous_rows
    assert build_segments(source, 0, 2, tmp_path / "segments") == segments
    assert build_outbox(segments, tmp_path / "outboxes") == outbox


def test_range_identity_and_invalid_or_unreadable_bounds(tmp_path: Path) -> None:
    source, segments, outbox = published(tmp_path)
    later = build_outbox(segments[1:], tmp_path / "outboxes")
    assert later != outbox
    assert inspect_outbox(later)[0]["ready_event_count"] == 3
    with pytest.raises(AvailabilityError, match="nonnegative"):
        discover_versions(source, 2, 1)
    with pytest.raises(AvailabilityError, match="version 3"):
        discover_versions(source, 0, 3)
    with pytest.raises(AvailabilityError, match="64 versions"):
        discover_versions(source, 0, 64)


def test_metadata_only_commit_has_explicit_empty_segment(tmp_path: Path) -> None:
    source = fixture(tmp_path)
    DeltaTable(source).alter.set_table_description("metadata-only local fixture")
    version = DeltaTable(source).version()
    segments = build_segments(source, version, version, tmp_path / "segments")
    manifest, ready = inspect_segment(segments[0])
    assert manifest["new_event_count"] == 0 and ready == []
    assert manifest["previous_event_count"] == manifest["current_event_count"] == 5


def test_committed_before_outbox_recovery_and_partial_publication(tmp_path: Path) -> None:
    source = fixture(tmp_path)  # Commit has returned; no availability artifacts exist yet.
    differences = discover_versions(source, 0, 2)
    first_ids = set(differences[2].new_rows)
    segments = build_segments(source, 0, 2, tmp_path / "segments")
    assert set(inspect_segment(segments[2])[1][i]["event_id"] for i in range(2)) == first_ids
    outbox = build_outbox(segments, tmp_path / "outboxes")
    assert len(inspect_outbox(outbox)[1]) == 5
    incomplete = tmp_path / "interrupted"
    incomplete.mkdir()
    (incomplete / "ready_events.jsonl").write_bytes(b"incomplete")
    with pytest.raises(AvailabilityError, match="Incomplete"):
        inspect_outbox(incomplete)
    with pytest.raises(AvailabilityError, match="already exists"):
        from sentinelaegisforge_control_plane.availability.contract import publish

        publish(incomplete, {"outbox_manifest.json": b"never overwrite"})


def test_append_only_conflicting_duplicate_update_delete_and_schema_fail(tmp_path: Path) -> None:
    source = fixture(tmp_path)
    append(source, [feature(2)])
    with pytest.raises(AvailabilityError, match="Duplicate source event_id"):
        discover_versions(source, 0, 3)

    update_path = tmp_path / "updated"
    append(update_path, [feature(1)])
    write_deltalake(
        update_path,
        pa.Table.from_pylist([feature(1, merchant_id="other")], schema=SOURCE_SCHEMA),
        mode="overwrite",
    )
    with pytest.raises(AvailabilityError, match="changed=1"):
        discover_versions(update_path, 0, 1)

    deleted = tmp_path / "deleted"
    append(deleted, [feature(1), feature(2)])
    DeltaTable(deleted).delete(predicate=f"event_id = '{UUID(int=1)}'")
    with pytest.raises(AvailabilityError, match="removed=1"):
        discover_versions(deleted, 0, 1)

    schema_path = tmp_path / "schema-change"
    append(schema_path, [feature(1)])
    evolved = pa.Table.from_pylist([feature(2)], schema=SOURCE_SCHEMA).append_column(
        "extra", pa.array(["unexpected"])
    )
    write_deltalake(schema_path, evolved, mode="overwrite", schema_mode="overwrite")
    with pytest.raises(AvailabilityError, match="25-column"):
        discover_versions(schema_path, 0, 1)


def test_table_identity_and_missing_history_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = fixture(tmp_path)
    from sentinelaegisforge_control_plane.availability import discovery

    original = discovery._read

    def changed(path: Path, version: int) -> FeatureSnapshot:
        snapshot = original(path, version)
        if version == 1:
            from dataclasses import replace

            return replace(snapshot, delta_table_id="replaced-table")
        return snapshot

    monkeypatch.setattr(discovery, "_read", changed)
    with pytest.raises(AvailabilityError, match="Table ID"):
        discover_versions(source, 0, 1)

    def changed_protocol(path: Path, version: int) -> FeatureSnapshot:
        from dataclasses import replace

        snapshot = original(path, version)
        return replace(snapshot, protocol={"unexpected": 1}) if version == 1 else snapshot

    monkeypatch.setattr(discovery, "_read", changed_protocol)
    with pytest.raises(AvailabilityError, match="protocol"):
        discover_versions(source, 0, 1)
    monkeypatch.setattr(discovery, "_read", original)
    (source / "_delta_log" / "00000000000000000001.json").unlink()
    with pytest.raises(AvailabilityError, match="Unverifiable Delta version 1"):
        discover_versions(source, 0, 2)


def test_corrupt_or_missing_segment_outbox_fail_closed(tmp_path: Path) -> None:
    _, segments, outbox = published(tmp_path)
    target = segments[1] / "ready_events.jsonl"
    target.write_bytes(b"{}\n")
    with pytest.raises(AvailabilityError):
        inspect_segment(segments[1])
    with pytest.raises(AvailabilityError):
        inspect_outbox(outbox)

    source, segments2, outbox2 = published(tmp_path / "second")
    assert source.exists()
    (outbox2 / "ready_events.jsonl").write_bytes(b"{}\n")
    with pytest.raises(AvailabilityError):
        inspect_outbox(outbox2)
    (segments2[0] / "segment_manifest.json").unlink()
    with pytest.raises(AvailabilityError):
        inspect_segment(segments2[0])


def test_bounded_partial_failure_restart_replay_and_receipt_reconciliation(tmp_path: Path) -> None:
    _, _, outbox = published(tmp_path)
    receiver_path = tmp_path / "receiver"
    receiver = LocalResearchReceiver(outbox, receiver_path)
    summary = receiver.handoff(2)
    assert summary["accepted_receipts"] == 2
    assert summary["last_fully_acknowledged_delta_version"] == 0
    assert summary["outstanding_events"] == 3
    with pytest.raises(AvailabilityError, match="incomplete"):
        receiver.verify_complete()

    failed_id = cast(str, receiver.ordered[3]["ready_event_id"])
    summary = receiver.handoff(3, fail_on_ready_event_id=failed_id)
    assert summary["accepted_receipts"] == 3
    assert summary["failed_deliveries"] == 1
    assert summary["last_fully_acknowledged_delta_version"] == 1
    restarted = LocalResearchReceiver(outbox, receiver_path)
    assert restarted.handoff(5)["accepted_receipts"] == 5
    assert restarted.verify_complete()["last_fully_acknowledged_delta_version"] == 2
    replay = restarted.handoff(5, replay=True)
    assert replay["accepted_receipts"] == 5
    assert replay["duplicate_deliveries"] == 5
    assert replay["accepted_attempts"] == 5
    assert replay["failed_deliveries"] == 1


def test_conflicting_redelivery_and_receiver_outbox_isolation(tmp_path: Path) -> None:
    _, segments, outbox = published(tmp_path)
    receiver = LocalResearchReceiver(outbox, tmp_path / "receiver")
    event = receiver.ordered[0]
    assert receiver.accept(event)
    assert not receiver.accept(event)
    with pytest.raises(AvailabilityError, match="conflicting"):
        receiver.accept({**event, "source_feature_row_fingerprint": "wrong"})
    other = build_outbox(segments[:2], tmp_path / "outboxes")
    with pytest.raises(AvailabilityError, match="different outbox"):
        LocalResearchReceiver(other, tmp_path / "receiver")


def test_no_model_invocation_on_delivery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, outbox = published(tmp_path)
    import sentinelaegisforge_control_plane.candidates.run as candidate_run

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("model loading must not be invoked")

    monkeypatch.setattr(candidate_run, "load_verified_candidate", forbidden)
    receiver = LocalResearchReceiver(outbox, tmp_path / "receiver")
    assert receiver.handoff(5)["accepted_receipts"] == 5
