"""Phase 22 local feedback is revision-aware and never becomes model ground truth by fiat."""

# ruff: noqa: F811 -- importing a pytest fixture makes its name available for injection.

import json
import shutil
from datetime import timedelta
from pathlib import Path
from typing import cast
from uuid import UUID

import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake
from test_monitoring import replay  # noqa: F401 -- shared verified frozen-ledger fixture
from test_training import event_id, moment

from sentinelaegisforge_control_plane.datasets.labels import Outcome, parse_utc_timestamp
from sentinelaegisforge_control_plane.feedback.contract import (
    FeedbackError,
    FeedbackStatus,
    parse_jsonl,
    parse_record,
)
from sentinelaegisforge_control_plane.feedback.fixture import (
    build_local_fixture,
    inspect_local_fixture,
)
from sentinelaegisforge_control_plane.feedback.ingestion import (
    build_feedback_ledger,
    inspect_feedback_ledger,
)
from sentinelaegisforge_control_plane.feedback.monitoring import (
    evaluate_feedback_outcomes,
    inspect_feedback_monitoring,
)
from sentinelaegisforge_control_plane.feedback.resolution import (
    EffectiveState,
    confirmed_projection,
    resolve_feedback,
)


def _record(
    event: str,
    revision: int = 1,
    status: str = "PENDING",
    *,
    case: str = "case-a",
    delivery: str | None = None,
    received: str = "2030-01-11T00:00:00Z",
    updated: str | None = None,
    provider: str = "LOCAL_PROVIDER_FIXTURE",
) -> dict[str, object]:
    return {
        "provider_id": provider,
        "provider_record_id": delivery or f"{case}-{revision}",
        "provider_case_id": case,
        "source_revision": revision,
        "event_id": event,
        "status": status,
        "source_updated_at": updated or received,
        "received_at": received,
    }


def _batch(path: Path, records: list[dict[str, object]]) -> Path:
    path.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")
    return path


def test_strict_envelope_and_timestamp_contract() -> None:
    valid = _record(event_id(1))
    assert parse_record(valid).status is FeedbackStatus.PENDING
    for bad in (
        {**valid, "extra": "x"},
        {**valid, "source_revision": True},
        {**valid, "source_revision": 0},
        {**valid, "status": "TRUSTED_BANK"},
        {**valid, "event_id": "not-uuid"},
        {**valid, "received_at": "2030-01-11T00:00:00+05:30"},
        {**valid, "received_at": "2030-01-11T00:00:00.1234567Z"},
        {**valid, "source_updated_at": "2030-01-12T00:00:00Z"},
    ):
        with pytest.raises(FeedbackError):
            parse_record(bad)
    with pytest.raises(FeedbackError, match="Duplicate JSON field"):
        parse_jsonl(b'{"provider_id":"a","provider_id":"b"}\n')
    with pytest.raises(FeedbackError, match="newline-terminated"):
        parse_jsonl(json.dumps(valid).encode())


def test_cumulative_revision_availability_and_states() -> None:
    event = event_id(1)
    records = tuple(
        parse_record(row)
        for row in (
            _record(event, 1, "CONFIRMED_FRAUD", received="2030-01-02T00:00:00Z"),
            _record(
                event,
                2,
                "PENDING",
                received="2030-01-07T00:00:00Z",
                updated="2030-01-03T00:00:00Z",
            ),
            _record(
                event,
                3,
                "CONFIRMED_LEGIT",
                received="2030-01-05T00:00:00Z",
                updated="2030-01-04T00:00:00Z",
            ),
        )
    )
    early = resolve_feedback(records, moment(5), (event,))
    assert early[event].state is EffectiveState.FRAUD
    assert early[event].selected[0].record.source_revision == 1
    late = resolve_feedback(records, moment(8), (event,))
    assert late[event].state is EffectiveState.LEGIT
    assert late[event].selected[0].record.source_revision == 3
    assert late[event].selected[0].effective_available_at == moment(7)
    assert confirmed_projection(early[event]) is Outcome.FRAUD
    assert confirmed_projection(late[event]) is Outcome.LEGIT
    assert Outcome.FRAUD.fraud_label == 1
    assert Outcome.LEGIT.fraud_label == 0
    assert (
        resolve_feedback(records, moment(1), (event,))[event].state
        is EffectiveState.NOT_YET_OBSERVED
    )
    assert confirmed_projection(resolve_feedback(records, moment(1), (event,))[event]) is None
    assert (
        resolve_feedback(records, moment(8), (event_id(2),))[event_id(2)].state
        is EffectiveState.NO_CASE
    )


def test_cross_provider_conflict_is_explicit() -> None:
    event = event_id(1)
    records = tuple(
        parse_record(row)
        for row in (
            _record(event, status="CONFIRMED_FRAUD", provider="provider-a"),
            _record(event, status="CONFIRMED_LEGIT", provider="provider-b"),
        )
    )
    result = resolve_feedback(records, moment(12), (event,))[event]
    assert result.state is EffectiveState.AMBIGUOUS
    assert {item.record.provider_id for item in result.selected} == {"provider-a", "provider-b"}


def test_fixture_immutable_ledgers_and_temporal_monitoring(
    replay: tuple[Path, object, Path, Path], tmp_path: Path
) -> None:
    _, _, source, prediction = replay
    fixture = build_local_fixture(prediction, tmp_path / "fixtures")
    assert build_local_fixture(prediction, tmp_path / "fixtures") == fixture
    fixture_manifest = inspect_local_fixture(fixture)
    t1_cutoff = parse_utc_timestamp(cast(str, fixture_manifest["t1"]))
    t2_cutoff = parse_utc_timestamp(cast(str, fixture_manifest["t2"]))
    t3_cutoff = parse_utc_timestamp(cast(str, fixture_manifest["t3"]))
    version = DeltaTable(source).version()
    first = build_feedback_ledger(fixture / "F1.jsonl", source, version, tmp_path / "feedback")
    assert (
        build_feedback_ledger(fixture / "F1.jsonl", source, version, tmp_path / "feedback") == first
    )
    second = build_feedback_ledger(
        fixture / "F2.jsonl", source, version, tmp_path / "feedback", base_ledger=first
    )
    first_manifest, second_manifest = (
        inspect_feedback_ledger(first),
        inspect_feedback_ledger(second),
    )
    assert first_manifest["record_count"] == 8
    assert first_manifest["duplicate_deliveries_suppressed_in_build"] == 1
    assert second_manifest["record_count"] == 16
    assert second_manifest["duplicate_deliveries_suppressed_in_build"] == 1
    assert inspect_feedback_ledger(first) == first_manifest  # Increment never mutates F1.
    reordered = tmp_path / "reordered.jsonl"
    reordered.write_text(
        "\n".join(reversed((fixture / "F1.jsonl").read_text().splitlines())) + "\n"
    )
    reordered_ledger = build_feedback_ledger(reordered, source, version, tmp_path / "feedback")
    assert reordered_ledger != first  # Same normalized records, changed raw-byte provenance.
    assert (
        inspect_feedback_ledger(reordered_ledger)["logical_records_fingerprint"]
        == first_manifest["logical_records_fingerprint"]
    )
    event = cast(list[str], fixture_manifest["selected_event_ids"])[0]
    before = (prediction / "predictions.parquet").read_bytes()
    t1 = evaluate_feedback_outcomes(
        prediction, first, t1_cutoff, tmp_path / "monitoring", min_rows=2, min_per_class=1
    )
    t2 = evaluate_feedback_outcomes(
        prediction, second, t2_cutoff, tmp_path / "monitoring", min_rows=2, min_per_class=1
    )
    t3 = evaluate_feedback_outcomes(
        prediction, second, t3_cutoff, tmp_path / "monitoring", min_rows=2, min_per_class=1
    )
    snapshots = [inspect_feedback_monitoring(path) for path in (t1, t2, t3)]
    assert all(
        cast(dict[str, object], item["quality"])["data_origin"] == "unverified_feedback"
        for item in snapshots
    )
    assert [
        cast(dict[str, object], item["coverage"])["eligible_labeled_events"] for item in snapshots
    ] == [6, 6, 9]
    assert [cast(dict[str, object], item["coverage"])["retracted_cases"] for item in snapshots] == [
        0,
        1,
        0,
    ]
    assert [cast(dict[str, object], item["coverage"])["pending_cases"] for item in snapshots] == [
        2,
        1,
        0,
    ]
    states = [
        {row["event_id"]: row for row in json.loads((path / "resolved_outcomes.json").read_bytes())}
        for path in (t1, t2, t3)
    ]
    assert [state[event]["state"] for state in states] == ["FRAUD", "RETRACTED", "LEGIT"]
    assert [state[event]["selected_cases"][0]["source_revision"] for state in states] == [1, 2, 3]
    assert (prediction / "predictions.parquet").read_bytes() == before
    assert pq.read_table(prediction / "predictions.parquet").num_rows == 24
    assert (
        evaluate_feedback_outcomes(
            prediction, second, t3_cutoff, tmp_path / "monitoring", min_rows=2, min_per_class=1
        )
        == t3
    )


def test_bad_delivery_revision_source_and_corruption_fail_closed(
    replay: tuple[Path, object, Path, Path], tmp_path: Path
) -> None:
    _, _, source, prediction = replay
    version = DeltaTable(source).version()
    event = pq.read_table(prediction / "predictions.parquet")["event_id"][0].as_py()
    base = _record(event)
    invalid_batches = (
        [base, {**base, "status": "CONFIRMED_FRAUD"}],
        [base, {**base, "provider_record_id": "other"}],
        [base, _record(event, 3, "CONFIRMED_FRAUD")],
        [base, _record(event_id(999999), case="missing")],
        [base, _record(event_id(2), 2)],
        [_record(event, received="2030-01-01T00:00:00Z")],
    )
    for index, records in enumerate(invalid_batches):
        batch = _batch(tmp_path / f"invalid-{index}.jsonl", records)
        with pytest.raises(FeedbackError):
            build_feedback_ledger(batch, source, version, tmp_path / f"bad-{index}")
        assert not list((tmp_path / f"bad-{index}").glob("*/manifest.json"))
    good = _batch(tmp_path / "good.jsonl", [base])
    ledger = build_feedback_ledger(good, source, version, tmp_path / "good-output")
    collision = _batch(
        tmp_path / "colon-identities.jsonl",
        [
            _record(event, case="c", provider="a:b"),
            _record(event_id(2), case="b:c", provider="a"),
        ],
    )
    distinct = build_feedback_ledger(collision, source, version, tmp_path / "distinct")
    assert inspect_feedback_ledger(distinct)["case_count"] == 2
    assert len(cast(list[object], inspect_feedback_ledger(distinct)["case_revision_coverage"])) == 2
    other = tmp_path / "different-delta"
    write_deltalake(other, DeltaTable(source).to_pyarrow_table())
    with pytest.raises(FeedbackError, match="incompatible"):
        build_feedback_ledger(good, other, 0, tmp_path / "mismatch", base_ledger=ledger)
    copy = tmp_path / "corrupted" / ledger.name
    shutil.copytree(ledger, copy)
    (copy / "feedback.jsonl").write_text("broken\n")
    with pytest.raises(FeedbackError):
        inspect_feedback_ledger(copy)
    with pytest.raises(FeedbackError):
        build_feedback_ledger(good, source, version, tmp_path / "later", base_ledger=copy)
    incomplete = tmp_path / "incomplete" / ledger.name
    incomplete.mkdir(parents=True)
    with pytest.raises(FeedbackError, match="Incomplete|incomplete"):
        build_feedback_ledger(good, source, version, tmp_path / "incomplete")


def test_out_of_order_complete_history_and_retraction_reconfirmation() -> None:
    event = event_id(1)
    records = tuple(
        parse_record(row)
        for row in (
            _record(event, 3, "CONFIRMED_LEGIT", received="2030-01-13T00:00:00Z"),
            _record(event, 1, "CONFIRMED_FRAUD", received="2030-01-11T00:00:00Z"),
            _record(event, 2, "RETRACTED", received="2030-01-12T00:00:00Z"),
        )
    )
    assert [
        resolve_feedback(records, moment(day), (event,))[event].state for day in (11, 12, 13)
    ] == [EffectiveState.FRAUD, EffectiveState.RETRACTED, EffectiveState.LEGIT]
    assert confirmed_projection(resolve_feedback(records, moment(12), (event,))[event]) is None
    assert UUID(event) == records[0].event_id


def test_monitoring_snapshot_corruption_and_one_class_unavailable(
    replay: tuple[Path, object, Path, Path], tmp_path: Path
) -> None:
    _, _, source, prediction = replay
    scored = pq.read_table(prediction / "predictions.parquet")["event_id"]
    batch = _batch(
        tmp_path / "one.jsonl",
        [
            _record(scored[0].as_py(), status="CONFIRMED_FRAUD"),
            _record(scored[1].as_py(), status="CONFIRMED_FRAUD", case="case-b"),
        ],
    )
    feedback = build_feedback_ledger(batch, source, 0, tmp_path / "feedback")
    snapshot = evaluate_feedback_outcomes(
        prediction, feedback, moment(12), tmp_path / "snapshots", min_rows=2, min_per_class=1
    )
    manifest = inspect_feedback_monitoring(snapshot)
    assert cast(dict[str, object], manifest["quality"])["reason"] == "INSUFFICIENT_CLASS_SUPPORT"
    copy = tmp_path / "bad-snapshot" / snapshot.name
    shutil.copytree(snapshot, copy)
    (copy / "coverage_report.json").write_text("{}")
    with pytest.raises(FeedbackError, match="hash differs"):
        inspect_feedback_monitoring(copy)


def test_conflicting_providers_block_monitoring_without_rescoring(
    replay: tuple[Path, object, Path, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, source, prediction = replay
    event = pq.read_table(prediction / "predictions.parquet")["event_id"][0].as_py()
    conflicting = _batch(
        tmp_path / "conflict.jsonl",
        [
            _record(event, status="CONFIRMED_FRAUD", provider="provider-a"),
            _record(event, status="CONFIRMED_LEGIT", provider="provider-b"),
        ],
    )
    feedback = build_feedback_ledger(conflicting, source, 0, tmp_path / "feedback")
    import sentinelaegisforge_control_plane.monitoring.ledger as scoring_module

    monkeypatch.setattr(
        scoring_module,
        "score_candidate",
        lambda *_args, **_kwargs: pytest.fail("Feedback evaluation rescored a model"),
    )
    with pytest.raises(FeedbackError, match="Conflicting current confirmed providers"):
        evaluate_feedback_outcomes(
            prediction,
            feedback,
            moment(12),
            tmp_path / "snapshots",
            min_rows=2,
            min_per_class=1,
        )
    assert not list((tmp_path / "snapshots").glob("*/manifest.json"))


def test_received_at_not_backdated_by_source_update() -> None:
    record = parse_record(
        _record(
            event_id(1),
            status="CONFIRMED_FRAUD",
            received="2030-01-11T00:00:00Z",
            updated="2030-01-01T00:00:00Z",
        )
    )
    assert (
        resolve_feedback((record,), moment(10), (event_id(1),))[event_id(1)].state
        is EffectiveState.NOT_YET_OBSERVED
    )
    assert (
        resolve_feedback((record,), moment(11), (event_id(1),))[event_id(1)].state
        is EffectiveState.FRAUD
    )
    assert record.received_at - record.source_updated_at == timedelta(days=10)
