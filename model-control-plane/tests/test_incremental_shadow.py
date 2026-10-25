"""Docker-independent incremental source, transport-evidence and worker recovery tests."""

from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import (
    AvailabilityError,
    digest,
    read_object,
)
from sentinelaegisforge_control_plane.availability.kafka_export import inspect_batch
from sentinelaegisforge_control_plane.availability.outbox import canonical, inspect_outbox
from sentinelaegisforge_control_plane.datasets.feature_source import FeatureSnapshot
from sentinelaegisforge_control_plane.incremental_shadow.fixture import (
    append_wave,
    fixture_rows,
    initialize_source,
)
from sentinelaegisforge_control_plane.incremental_shadow.operations import (
    GROUP,
    Limits,
    discover_next,
    initialize,
    logical_request_id,
    records,
)
from sentinelaegisforge_control_plane.incremental_shadow.progress import (
    authorize_publication,
    progress,
    reconcile,
)
from sentinelaegisforge_control_plane.incremental_shadow.telemetry import build_report
from sentinelaegisforge_control_plane.incremental_shadow.worker import (
    process_resolved,
    resolved_work,
)

BASE = datetime(2030, 1, 5, tzinfo=UTC)
END = datetime(2030, 2, 1, tzinfo=UTC)


def _write(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical(value))


def _stream(tmp_path: Path, *, accepted_limit: int = 8, group: str = GROUP) -> tuple[Path, Path]:
    source = tmp_path / "source"
    initialize_source(source)
    fence = tmp_path / "fence.json"
    _write(
        fence,
        {
            "contract_version": "feature-ready-operational-fence-v1",
            "group_id": group,
            "cluster_id": "cluster-a",
            "topic_id": "topic-a",
            "topic": "features.ready",
            **{f"beginning_{part}": 0 for part in range(3)},
            **{f"end_{part}": 0 for part in range(3)},
        },
    )
    manifest = initialize(
        source,
        tmp_path / "streams",
        fence,
        0,
        "a" * 64,
        "b" * 64,
        Limits(maximum_accepted_unprocessed_events=accepted_limit),
        group,
    )
    return source, tmp_path / "streams" / cast(str, manifest["stream_id"])


def _accept(stream: Path, record: dict[str, object], start: int) -> None:
    """Mock Phase 25's durable transport artifacts, not a substitute for live Kafka tests."""
    import hashlib

    batch_id = cast(str, record["publication_batch_id"])
    batch = inspect_batch(Path(cast(str, record["batch_path"])))
    outbox, events = inspect_outbox(Path(cast(str, batch["outbox_path"])))
    identity = read_object(stream / "identity" / "stream_manifest.json")
    receiver = stream / "receipts" / batch_id
    journal = stream / "journals" / batch_id
    _write(
        receiver / "receiver_identity.json",
        {
            "receipt_store_contract_version": "kafka-feature-ready-receipt-store-v1",
            "publication_batch_id": batch_id,
            "outbox_id": outbox["outbox_id"],
            "group_id": GROUP,
            "topic": "features.ready",
            "cluster_id": "cluster-a",
            "topic_id": "topic-a",
        },
    )
    _write(
        journal / "journal_identity.json",
        {
            "journal_contract_version": "feature-ready-kafka-publication-journal-v1",
            "publication_batch_id": batch_id,
            "outbox_id": outbox["outbox_id"],
            "topic": "features.ready",
            "cluster_id": "cluster-a",
            "topic_id": "topic-a",
        },
    )
    for offset, event in enumerate(events, start):
        ready_id = cast(str, event["ready_event_id"])
        envelope = hashlib.sha256(canonical(event)).hexdigest()
        _write(
            receiver / "receipts" / f"{ready_id}.json",
            {
                "receipt_contract_version": "kafka-feature-ready-receipt-v1",
                "receipt_id": digest(
                    {
                        "ready_event_id": ready_id,
                        "outbox_id": outbox["outbox_id"],
                        "topic_id": "topic-a",
                        "partition": 0,
                        "offset": offset,
                        "envelope_fingerprint": envelope,
                    }
                ),
                "ready_event_id": ready_id,
                "event_id": event["event_id"],
                "outbox_id": outbox["outbox_id"],
                "topic": "features.ready",
                "topic_id": "topic-a",
                "first_topic_partition": 0,
                "first_topic_offset": offset,
                "envelope_fingerprint": envelope,
            },
        )
        _write(
            receiver / "observations" / f"0-{offset}.json",
            {
                "observation_contract_version": "kafka-feature-ready-observation-v1",
                "ready_event_id": ready_id,
                "event_id": event["event_id"],
                "topic": "features.ready",
                "topic_id": "topic-a",
                "partition": 0,
                "offset": offset,
                "envelope_fingerprint": envelope,
            },
        )
        _write(
            journal / "acknowledgements" / f"{ready_id}.json",
            {
                "ack_contract_version": "feature-ready-kafka-ack-v1",
                "publication_batch_id": batch_id,
                "outbox_id": outbox["outbox_id"],
                "cluster_id": "cluster-a",
                "topic_id": "topic-a",
                "ready_event_id": ready_id,
                "event_id": event["event_id"],
                "topic": "features.ready",
                "partition": 0,
                "offset": offset,
                "attempt_id": "mock-attempt",
            },
        )
    _write(
        stream / "acceptance" / f"{batch_id}.json",
        {
            "contract_version": "feature-ready-operational-acceptance-v1",
            "group_id": GROUP,
            "cluster_id": identity["cluster_id"],
            "topic_id": identity["topic_id"],
            "topic": "features.ready",
            "publication_batch_id": batch_id,
            "outbox_id": outbox["outbox_id"],
            "ready_event_count": len(events),
            "physical_record_count": len(events),
            "start_0": start,
            "end_0": start + len(events),
            "start_1": 0,
            "end_1": 0,
            "start_2": 0,
            "end_2": 0,
        },
    )


def test_fixture_is_deterministic_valid_and_commits_new_versions(tmp_path: Path) -> None:
    source, stream = _stream(tmp_path)
    assert fixture_rows(7, 0, 2, BASE) == fixture_rows(7, 0, 2, BASE)
    assert fixture_rows(8, 0, 2, BASE)[0]["event_id"] != fixture_rows(7, 0, 2, BASE)[0]["event_id"]
    assert discover_next(stream)["ready_event_count"] == 0  # type: ignore[index]
    first = append_wave(source, 7, 0, 2, BASE)
    assert first["delta_version"] == 1
    discovered = discover_next(stream)
    assert discovered is not None and discovered["ready_event_count"] == 2
    assert discover_next(stream) is None
    assert DeltaTable(source).count() == 2


def test_incremental_versions_and_immutable_segments(tmp_path: Path) -> None:
    source, stream = _stream(tmp_path)
    discover_next(stream)
    append_wave(source, 7, 0, 2, BASE)
    one = discover_next(stream)
    assert one is not None
    original = Path(cast(str, one["segment_path"])) / "segment_manifest.json"
    content = original.read_bytes()
    append_wave(source, 7, 2, 3, BASE)
    two = discover_next(stream)
    assert two is not None
    assert [item["delta_version"] for item in records(stream)] == [0, 1, 2]
    assert original.read_bytes() == content
    assert one["publication_batch_id"] != two["publication_batch_id"]
    assert progress(stream)["discovery_backlog_versions"] == 0


def test_backpressure_and_durable_worker_restart(tmp_path: Path) -> None:
    source, stream = _stream(tmp_path, accepted_limit=3)
    discover_next(stream)
    append_wave(source, 7, 0, 2, BASE)
    one = discover_next(stream)
    assert one is not None
    authorize_publication(stream, cast(str, one["publication_batch_id"]))
    _accept(stream, one, 0)
    assert progress(stream)["inference_backlog_ready_events"] == 2
    append_wave(source, 7, 2, 2, BASE)
    two = discover_next(stream)
    assert two is not None
    with pytest.raises(AvailabilityError, match="PAUSED_BACKPRESSURE"):
        authorize_publication(stream, cast(str, two["publication_batch_id"]))
    work = resolved_work(stream, 5000)
    calls = 0

    def score(_row: dict[str, object]) -> float:
        nonlocal calls
        calls += 1
        return 0.3

    with pytest.raises(AvailabilityError, match="Injected"):
        process_resolved(stream, work, BASE, END, score, 2, fail_after_result=1)
    assert progress(stream)["completed_inference_results"] == 1
    process_resolved(stream, work, BASE, END, score, 2)
    assert calls == 2
    process_resolved(stream, work, BASE, END, score, 2)
    assert calls == 2
    authorize_publication(stream, cast(str, two["publication_batch_id"]))
    _accept(stream, two, 2)
    assert progress(stream)["inference_backlog_ready_events"] == 2
    process_resolved(stream, resolved_work(stream, 5000), BASE, END, score, 2)
    assert progress(stream)["completed_inference_results"] == 4
    assert calls == 4


def test_corrupt_progress_and_acceptance_fail_closed(tmp_path: Path) -> None:
    source, stream = _stream(tmp_path)
    discover_next(stream)
    append_wave(source, 7, 0, 1, BASE)
    record = discover_next(stream)
    assert record is not None
    _accept(stream, record, 0)
    assert progress(stream)["accepted_ready_events"] == 1
    acceptance = stream / "acceptance" / f"{record['publication_batch_id']}.json"
    value = read_object(acceptance)
    value["end_0"] = 2
    acceptance.write_bytes(canonical(value))
    with pytest.raises(AvailabilityError, match="physical range"):
        progress(stream)


def test_missing_history_blocks_incremental_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, stream = _stream(tmp_path)
    discover_next(stream)
    append_wave(source, 7, 0, 1, BASE)
    record = discover_next(stream)
    assert record is not None
    _accept(stream, record, 0)
    from sentinelaegisforge_control_plane.availability import discovery

    original = discovery._read

    def unavailable(path: Path, version: int) -> FeatureSnapshot:
        if version == 1:
            raise AvailabilityError("Required historical version unavailable")
        return original(path, version)

    monkeypatch.setattr(discovery, "_read", unavailable)
    with pytest.raises(AvailabilityError, match="historical version"):
        resolved_work(stream, 5000)


def test_retained_delta_history_is_required_for_pending_work(tmp_path: Path) -> None:
    """A controlled temporary fixture cannot fall back to its latest snapshot."""
    source, stream = _stream(tmp_path)
    discover_next(stream)
    append_wave(source, 7, 0, 1, BASE)
    record = discover_next(stream)
    assert record is not None
    _accept(stream, record, 0)
    assert len(resolved_work(stream, 5000)) == 1

    committed_log = source / "_delta_log" / f"{1:020d}.json"
    temporarily_unavailable = tmp_path / "retained-version-1.json"
    committed_log.rename(temporarily_unavailable)
    try:
        with pytest.raises(AvailabilityError, match="Unverifiable Delta version 1"):
            resolved_work(stream, 5000)
        assert not (stream / "results").exists()
    finally:
        temporarily_unavailable.rename(committed_log)
    assert len(resolved_work(stream, 5000)) == 1


def test_request_identity_is_stream_level_not_batch_level() -> None:
    a = logical_request_id("1" * 64, "a" * 64, "b" * 64)
    assert a == logical_request_id("1" * 64, "a" * 64, "b" * 64)
    assert a != logical_request_id("1" * 64, "c" * 64, "b" * 64)


def test_isolated_operational_group_has_distinct_identity(tmp_path: Path) -> None:
    group = f"{GROUP}-phase28-example"
    _source, stream = _stream(tmp_path, group=group)
    assert read_object(stream / "identity" / "stream_manifest.json")["group_id"] == group
    with pytest.raises(AvailabilityError, match="starting fence"):
        initialize(
            tmp_path / "source",
            tmp_path / "other-streams",
            tmp_path / "fence.json",
            0,
            "a" * 64,
            "b" * 64,
            Limits(),
            GROUP,
        )


def test_invalid_bounds_and_unsupported_fixture_inputs(tmp_path: Path) -> None:
    with pytest.raises(AvailabilityError, match="backpressure bound"):
        Limits(maximum_accepted_unprocessed_events=0)
    with pytest.raises(AvailabilityError, match="one active"):
        Limits(maximum_active_publication_batches=2)
    with pytest.raises(AvailabilityError, match="bounded indices"):
        fixture_rows(1, 0, 201, BASE)
    source, _ = _stream(tmp_path)
    append_wave(source, 1, 0, 1, BASE)
    with pytest.raises(AvailabilityError, match="duplicate"):
        append_wave(source, 1, 0, 1, BASE)


def test_missing_version_record_and_conflicting_artifacts_fail(tmp_path: Path) -> None:
    source, stream = _stream(tmp_path)
    discover_next(stream)
    append_wave(source, 7, 0, 1, BASE)
    record = discover_next(stream)
    assert record is not None
    (stream / "versions" / "1.json").rename(stream / "versions" / "3.json")
    with pytest.raises(AvailabilityError, match="version gap"):
        records(stream)


def test_incomplete_reconciliation_and_foreign_result_fail(tmp_path: Path) -> None:
    source, stream = _stream(tmp_path)
    discover_next(stream)
    append_wave(source, 7, 0, 1, BASE)
    record = discover_next(stream)
    assert record is not None
    with pytest.raises(AvailabilityError, match="incomplete"):
        reconcile(stream)
    _accept(stream, record, 0)
    process_resolved(stream, resolved_work(stream, 5000), BASE, END, lambda _: 0.2, 1)
    assert reconcile(stream)["terminal_inference_results"] == 1
    result_path = next((stream / "results" / ("a" * 64)).glob("*.json"))
    value = read_object(result_path)
    value["source_feature_row_fingerprint"] = "f" * 64
    result_path.write_bytes(canonical(value))
    with pytest.raises(AvailabilityError, match="source provenance"):
        reconcile(stream)


def test_observational_timing_does_not_change_stream_identity(tmp_path: Path) -> None:
    source, stream = _stream(tmp_path)
    discover_next(stream)
    append_wave(source, 7, 0, 1, BASE)
    record = discover_next(stream)
    assert record is not None
    _accept(stream, record, 0)
    process_resolved(stream, resolved_work(stream, 5000), BASE, END, lambda _: 0.2, 1)
    before = progress(stream)["stream_id"]
    _write(
        stream / "observations" / "timing.json",
        {
            "observation_contract_version": "incremental-shadow-timing-v1",
            "stage": "delta_write",
            "elapsed_seconds": 0.04,
            "sample_count": 1,
        },
    )
    report = build_report(stream)
    assert report["stream_id"] == before
    assert report["timing"]["delta_write"]["samples"] == 1  # type: ignore[index]
    assert progress(stream)["stream_id"] == before
