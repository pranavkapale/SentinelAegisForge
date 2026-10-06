"""Controlled temporary Delta fixtures. Live evidence uses the real Scala pipeline instead."""

import hashlib
import json
import math
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import write_deltalake

from sentinelaegisforge_control_plane.datasets.cli import main
from sentinelaegisforge_control_plane.datasets.dataset_builder import (
    build_dataset,
    inspect_snapshot,
)
from sentinelaegisforge_control_plane.datasets.dataset_contract import (
    CANDIDATE_FEATURE_COLUMNS,
    DEFERRED_IDENTIFIER_COLUMNS,
    FORBIDDEN_RISK_COLUMNS,
    NON_FEATURE_COLUMNS,
    SOURCE_SCHEMA,
    feature_rows,
    schema_fingerprint,
    validate_schema,
)
from sentinelaegisforge_control_plane.datasets.errors import (
    FeatureDataError,
    FeatureSourceError,
    LabelContractError,
    SnapshotConflictError,
    SourceSchemaError,
)
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.labels import (
    Outcome,
    TransactionOutcomeLabel,
    load_labels,
    parse_utc_timestamp,
    resolve_labels,
)


def time(day: int) -> datetime:
    return datetime(2030, 1, day, tzinfo=UTC)


def event_id(n: int) -> str:
    return str(UUID(int=n))


def feature(n: int, **changes: object) -> dict[str, object]:
    return {
        "event_id": event_id(n),
        "transaction_id": f"transaction-{n}",
        "customer_id": "customer-a",
        "merchant_id": "merchant-a",
        "event_time": time(1),
        "ingestion_time": time(1),
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
        "kafka_offset": n,
        "kafka_timestamp": time(1),
        "prior_transaction_count_5m": 0,
        "prior_amount_sum_10m": Decimal("0.0000"),
        "prior_amount_observation_count": 0,
        "prior_amount_mean": None,
        "prior_amount_stddev": None,
        "amount_zscore": None,
        "statistical_feature_status": "NO_HISTORY",
        **changes,
    }


def label(
    n: int, outcome: str = "LEGIT", day: int = 3, revision: int = 1, **changes: object
) -> dict[str, object]:
    return {
        "event_id": event_id(n),
        "outcome": outcome,
        "label_observed_at": f"2030-01-{day:02d}T00:00:00Z",
        "label_source": "SYNTHETIC_FIXTURE",
        "label_revision": revision,
        **changes,
    }


def write_labels(root: Path, labels: list[dict[str, object]]) -> Path:
    path = root / "labels.jsonl"
    path.write_text("".join(json.dumps(record) + "\n" for record in labels), encoding="utf-8")
    return path


def source(root: Path, rows: list[dict[str, object]]) -> Path:
    path = root / "transaction_statistical_features_v2"
    write_deltalake(path, pa.Table.from_pylist(rows, schema=SOURCE_SCHEMA))
    return path


def dataset_rows(path: Path) -> list[dict[str, object]]:
    return cast(list[dict[str, object]], pq.read_table(path / "dataset.parquet").to_pylist())


def test_delta_reader_pins_version_and_reads_all_columns(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1)])
    snapshot = StatisticalFeatureSource(path).read()
    write_deltalake(path, pa.Table.from_pylist([feature(2)], schema=SOURCE_SCHEMA), mode="append")
    assert snapshot.delta_version == 0 and snapshot.row_count == 1
    assert StatisticalFeatureSource(path, version=0).read().row_count == 1
    assert StatisticalFeatureSource(path).read().delta_version == 1
    assert snapshot.table.column_names == SOURCE_SCHEMA.names
    assert snapshot.source_schema_fingerprint == schema_fingerprint(SOURCE_SCHEMA)


def test_explicit_outcomes_and_unlabeled_exclusions_preserve_decimal_evidence(
    tmp_path: Path,
) -> None:
    path = source(tmp_path, [feature(3), feature(2, currency="EUR"), feature(1)])
    labels = write_labels(tmp_path, [label(1, "FRAUD"), label(2, "LEGIT")])
    snapshot = build_dataset(path, labels, time(6), tmp_path / "output")
    rows = dataset_rows(snapshot.path)
    assert [row["event_id"] for row in rows] == [event_id(1), event_id(2)]
    assert [row["fraud_label"] for row in rows] == [1, 0]
    assert rows[0]["amount"] == Decimal("123.4567")
    assert rows[1]["currency"] == "EUR"
    for name, expected in {
        "feature_rows_in_delta_version": 3,
        "feature_rows_available_as_of": 3,
        "labeled_dataset_rows": 2,
        "positive_rows": 1,
        "negative_rows": 1,
        "unlabeled_feature_rows_excluded": 1,
        "unmatched_label_rows": 0,
    }.items():
        assert snapshot.manifest[name] == expected
    assert set(snapshot.path.iterdir()) == {
        snapshot.path / "dataset.parquet",
        snapshot.path / "manifest.json",
    }
    assert (
        snapshot.manifest["label_source_sha256"] == hashlib.sha256(labels.read_bytes()).hexdigest()
    )


def test_delayed_fraud_label_cannot_leak_into_earlier_snapshot(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1)])
    labels = write_labels(tmp_path, [label(1, "FRAUD", day=5)])
    early = build_dataset(path, labels, time(3), tmp_path / "output")
    later = build_dataset(path, labels, time(6), tmp_path / "output")
    assert early.manifest["labeled_dataset_rows"] == 0
    assert early.manifest["positive_rows"] == 0
    assert early.manifest["unlabeled_feature_rows_excluded"] == 1
    assert later.manifest["labeled_dataset_rows"] == 1 and later.manifest["positive_rows"] == 1
    assert dataset_rows(early.path) == []


def test_future_revision_is_excluded_and_old_snapshot_remains_immutable(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1)])
    labels = write_labels(tmp_path, [label(1, "LEGIT", 3, 1), label(1, "FRAUD", 8, 2)])
    early = build_dataset(path, labels, time(5), tmp_path / "output")
    before = {p.name: p.read_bytes() for p in early.path.iterdir()}
    later = build_dataset(path, labels, time(10), tmp_path / "output")
    assert dataset_rows(early.path)[0]["fraud_label"] == 0
    assert dataset_rows(early.path)[0]["label_revision"] == 1
    assert dataset_rows(later.path)[0]["fraud_label"] == 1
    assert dataset_rows(later.path)[0]["label_revision"] == 2
    assert before == {p.name: p.read_bytes() for p in early.path.iterdir()}
    assert early.manifest["eligible_label_records"] == 1
    assert later.manifest["eligible_label_records"] == 2


def test_ingestion_as_of_filter_is_inclusive_not_event_time_filter(tmp_path: Path) -> None:
    path = source(
        tmp_path, [feature(1, ingestion_time=time(4)), feature(2, ingestion_time=time(6))]
    )
    labels = write_labels(tmp_path, [label(1, day=4), label(2, day=6)])
    snapshot = build_dataset(path, labels, time(4), tmp_path / "output")
    assert snapshot.manifest["feature_rows_available_as_of"] == 1
    assert [row["event_id"] for row in dataset_rows(snapshot.path)] == [event_id(1)]


def test_label_before_ingestion_fails_even_when_not_eligible_yet(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1, ingestion_time=time(8))])
    labels = write_labels(tmp_path, [label(1, "FRAUD", day=5)])
    with pytest.raises(LabelContractError, match="precedes ingestion_time"):
        build_dataset(path, labels, time(3), tmp_path / "output")


def test_duplicate_event_ids_fail_for_entire_version_even_future_rows(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1), feature(1, ingestion_time=time(10))])
    labels = write_labels(tmp_path, [])
    with pytest.raises(FeatureDataError, match="Duplicate source event_id"):
        build_dataset(path, labels, time(5), tmp_path / "output")


def test_eligible_unmatched_labels_fail_but_future_unknown_labels_are_not_joined(
    tmp_path: Path,
) -> None:
    path = source(tmp_path, [feature(1)])
    labels = write_labels(tmp_path, [label(2, day=8)])
    assert (
        build_dataset(path, labels, time(5), tmp_path / "output").manifest["selected_labels"] == 0
    )
    with pytest.raises(LabelContractError, match="no available feature"):
        build_dataset(path, labels, time(10), tmp_path / "output")


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": "bad"},
        {"outcome": "REVIEW"},
        {"outcome": "CLEAR"},
        {"label_observed_at": "2030-01-01T00:00:00"},
        {"label_observed_at": "2030-01-01T00:00:00+01:00"},
        {"label_observed_at": "2030-01-01T00:00:00.0000001Z"},
        {"label_observed_at": "2030-02-30T00:00:00Z"},
        {"label_source": " "},
        {"label_revision": 0},
        {"label_revision": -1},
        {"label_revision": True},
        {"label_revision": 1.5},
        {"unexpected": "field"},
    ],
)
def test_invalid_label_values_are_rejected(tmp_path: Path, changes: dict[str, object]) -> None:
    path = write_labels(tmp_path, [{**label(1), **changes}])
    with pytest.raises(LabelContractError):
        load_labels(path)


@pytest.mark.parametrize(
    "history",
    [
        [label(1), label(1)],
        [label(1, day=8), label(1, day=3, revision=2)],
        [label(1, revision=2)],
        [label(1), label(1, revision=3)],
    ],
)
def test_invalid_revision_histories_fail(tmp_path: Path, history: list[dict[str, object]]) -> None:
    with pytest.raises(LabelContractError):
        load_labels(write_labels(tmp_path, history))


def test_revision_order_is_independent_of_file_order_and_equal_observation_times_are_defined(
    tmp_path: Path,
) -> None:
    records = load_labels(
        write_labels(tmp_path, [label(1, "FRAUD", 3, 2), label(1, "LEGIT", 3, 1)])
    )
    selected = resolve_labels(records, time(3))
    assert selected[event_id(1)].outcome is Outcome.FRAUD
    assert selected[event_id(1)].label_revision == 2


def test_duplicate_json_fields_missing_fields_and_invalid_utf8_fail(tmp_path: Path) -> None:
    path = tmp_path / "labels.jsonl"
    for raw in (b'{"event_id":"a","event_id":"b"}\n', b"{}\n", b"\xff", b"\n"):
        path.write_bytes(raw)
        with pytest.raises(LabelContractError):
            load_labels(path)


def test_utc_normalization_and_typed_label_contract() -> None:
    assert parse_utc_timestamp("2030-01-01T00:00:00+00:00") == time(1)
    record = TransactionOutcomeLabel(UUID(int=1), Outcome.FRAUD, time(1), "SYNTHETIC_FIXTURE", 1)
    assert record.outcome.fraud_label == 1 and record.label_observed_at.tzinfo is UTC
    with pytest.raises(LabelContractError):
        TransactionOutcomeLabel(UUID(int=1), Outcome.LEGIT, datetime(2030, 1, 1), "fixture", 1)


@pytest.mark.parametrize("field", ["event_id", "ingestion_time"])
def test_missing_source_columns_fail(field: str) -> None:
    with pytest.raises(SourceSchemaError):
        validate_schema(pa.schema([f for f in SOURCE_SCHEMA if f.name != field]))


@pytest.mark.parametrize(
    "field,wrong_type",
    [
        ("amount", pa.float64()),
        ("amount", pa.decimal128(18, 2)),
        ("prior_transaction_count_5m", pa.float64()),
        ("ingestion_time", pa.timestamp("us")),
    ],
)
def test_wrong_logical_types_fail(field: str, wrong_type: pa.DataType) -> None:
    schema = pa.schema([(f.name, wrong_type if f.name == field else f.type) for f in SOURCE_SCHEMA])
    with pytest.raises(SourceSchemaError):
        validate_schema(schema)


def test_risk_table_and_schema_order_are_not_accepted_as_feature_source() -> None:
    with pytest.raises(SourceSchemaError):
        validate_schema(pa.schema([*SOURCE_SCHEMA, ("risk_disposition", pa.string())]))
    with pytest.raises(SourceSchemaError):
        validate_schema(pa.schema(list(SOURCE_SCHEMA)[::-1]))
    assert schema_fingerprint(SOURCE_SCHEMA) != schema_fingerprint(
        pa.schema(list(SOURCE_SCHEMA)[::-1])
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"amount_zscore": 1.0},
        {"statistical_feature_status": "READY"},
        {
            "statistical_feature_status": "ZERO_VARIANCE",
            "prior_amount_observation_count": 2,
            "prior_amount_mean": 100.0,
            "prior_amount_stddev": 0.0,
            "amount_zscore": 3.0,
        },
        {
            "statistical_feature_status": "INSUFFICIENT_VARIANCE_HISTORY",
            "prior_amount_observation_count": 1,
            "prior_amount_mean": 100.0,
            "amount_zscore": 4.0,
        },
        {"prior_transaction_count_5m": -1},
        {"prior_amount_observation_count": -1},
        {"amount_zscore": float("nan")},
        {"prior_amount_mean": float("inf")},
        {"prior_amount_stddev": -1.0},
        {"statistical_feature_status": "UNKNOWN"},
        {"amount": Decimal("0")},
        {"prior_amount_sum_10m": Decimal("-1")},
    ],
)
def test_corrupt_statistical_or_monetary_rows_are_not_repaired(changes: dict[str, object]) -> None:
    with pytest.raises(FeatureDataError):
        feature_rows(pa.Table.from_pylist([feature(1, **changes)], schema=SOURCE_SCHEMA))


@pytest.mark.parametrize("mean", [-1.0, 0.0])
def test_nonpositive_prior_mean_with_history_fails(mean: float) -> None:
    row = feature(
        1,
        prior_amount_observation_count=1,
        prior_amount_mean=mean,
        statistical_feature_status="INSUFFICIENT_VARIANCE_HISTORY",
    )
    with pytest.raises(FeatureDataError, match="prior_amount_mean must be positive"):
        feature_rows(pa.Table.from_pylist([row], schema=SOURCE_SCHEMA))


def test_ready_row_with_materially_incorrect_finite_zscore_fails() -> None:
    row = feature(
        1,
        prior_amount_observation_count=2,
        prior_amount_mean=100.0,
        prior_amount_stddev=10.0,
        amount_zscore=4.0,  # (123.4567 - 100.0) / 10.0 is about 2.34567.
        statistical_feature_status="READY",
    )
    with pytest.raises(FeatureDataError, match="amount_zscore is inconsistent"):
        feature_rows(pa.Table.from_pylist([row], schema=SOURCE_SCHEMA))


def test_consistent_ready_row_passes_without_changing_exact_amount_or_zscore() -> None:
    amount = Decimal("123.4567")
    zscore = (float(amount) - 100.0) / 10.0
    row = feature(
        1,
        amount=amount,
        prior_amount_observation_count=2,
        prior_amount_mean=100.0,
        prior_amount_stddev=10.0,
        amount_zscore=zscore,
        statistical_feature_status="READY",
    )
    checked = feature_rows(pa.Table.from_pylist([row], schema=SOURCE_SCHEMA))[0]
    assert checked["amount"] == amount
    assert checked["amount_zscore"] == zscore


@pytest.mark.parametrize(
    "zscore",
    [1.054829874926953, math.nextafter(1.054829874926953, math.inf)],
)
def test_real_phase10_double_zscore_and_one_ulp_roundoff_pass(zscore: float) -> None:
    # Observed in the five-event Phase 10.1 Delta verification lineage.
    row = feature(
        1,
        amount=Decimal("7298.0100"),
        prior_amount_observation_count=2,
        prior_amount_mean=7222.475,
        prior_amount_stddev=71.60870373076101,
        amount_zscore=zscore,
        statistical_feature_status="READY",
    )
    assert (
        feature_rows(pa.Table.from_pylist([row], schema=SOURCE_SCHEMA))[0]["amount_zscore"]
        == zscore
    )


def test_ready_statistics_and_large_exact_monetary_sum_are_preserved(tmp_path: Path) -> None:
    value = feature(
        1,
        prior_amount_observation_count=2,
        prior_amount_mean=223.4567,
        prior_amount_stddev=10.0,
        amount_zscore=-10.0,
        statistical_feature_status="READY",
        prior_amount_sum_10m=Decimal("100000000000000000000.1234"),
    )
    path = source(tmp_path, [value])
    labels = write_labels(tmp_path, [label(1)])
    snapshot = build_dataset(path, labels, time(5), tmp_path / "output")
    assert dataset_rows(snapshot.path)[0]["prior_amount_sum_10m"] == value["prior_amount_sum_10m"]
    assert dataset_rows(snapshot.path)[0]["amount_zscore"] == -10.0


def test_identical_inputs_rebuild_verify_same_artifacts_and_logical_fingerprint(
    tmp_path: Path,
) -> None:
    path = source(tmp_path, [feature(1)])
    labels = write_labels(tmp_path, [label(1)])
    first = build_dataset(path, labels, time(5), tmp_path / "output")
    before = {p.name: p.read_bytes() for p in first.path.iterdir()}
    repeat = build_dataset(path, labels, time(5), tmp_path / "output")
    other_root = build_dataset(path, labels, time(5), tmp_path / "other-output")
    assert first.manifest == repeat.manifest == other_root.manifest
    assert first.path == repeat.path
    assert before == {p.name: p.read_bytes() for p in first.path.iterdir()}
    assert (first.path / "dataset.parquet").read_bytes() == (
        other_root.path / "dataset.parquet"
    ).read_bytes()
    assert inspect_snapshot(first.path) == first.manifest


def test_label_byte_change_changes_snapshot_identity_not_logical_selections(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1)])
    labels = write_labels(tmp_path, [label(1)])
    first = build_dataset(path, labels, time(5), tmp_path / "output")
    labels.write_bytes(labels.read_bytes().replace(b'"outcome":', b'"outcome" :', 1))
    second = build_dataset(path, labels, time(5), tmp_path / "output")
    assert first.manifest["snapshot_id"] != second.manifest["snapshot_id"]
    assert (
        first.manifest["logical_dataset_fingerprint"]
        == second.manifest["logical_dataset_fingerprint"]
    )


def test_fixed_old_delta_version_can_rebuild_after_source_append(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1)])
    labels = write_labels(tmp_path, [label(1)])
    first = build_dataset(path, labels, time(5), tmp_path / "output", source_version=0)
    write_deltalake(path, pa.Table.from_pylist([feature(2)], schema=SOURCE_SCHEMA), mode="append")
    old = build_dataset(path, labels, time(5), tmp_path / "output", source_version=0)
    latest = build_dataset(path, labels, time(5), tmp_path / "output")
    assert old.manifest == first.manifest
    assert latest.manifest["source_delta_version"] == 1
    assert latest.manifest["snapshot_id"] != old.manifest["snapshot_id"]


def test_existing_corrupt_or_incomplete_snapshot_is_not_overwritten(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1)])
    labels = write_labels(tmp_path, [label(1)])
    snapshot = build_dataset(path, labels, time(5), tmp_path / "output")
    parquet = snapshot.path / "dataset.parquet"
    parquet.write_bytes(b"corrupt")
    with pytest.raises(SnapshotConflictError):
        build_dataset(path, labels, time(5), tmp_path / "output")
    assert parquet.read_bytes() == b"corrupt"
    manifest = snapshot.path / "manifest.json"
    manifest.unlink()
    with pytest.raises(SnapshotConflictError, match="Incomplete"):
        build_dataset(path, labels, time(5), tmp_path / "output")
    assert not manifest.exists()


def test_source_failures_are_explicit(tmp_path: Path) -> None:
    with pytest.raises(FeatureSourceError):
        StatisticalFeatureSource(tmp_path / "missing").read()
    with pytest.raises(FeatureSourceError):
        StatisticalFeatureSource(tmp_path, version=-1)


def test_candidate_allowlist_excludes_identity_lineage_labels_and_risk_outputs() -> None:
    assert not set(CANDIDATE_FEATURE_COLUMNS) & set(NON_FEATURE_COLUMNS)
    assert not set(CANDIDATE_FEATURE_COLUMNS) & set(FORBIDDEN_RISK_COLUMNS)
    assert not set(CANDIDATE_FEATURE_COLUMNS) & set(DEFERRED_IDENTIFIER_COLUMNS)
    assert "currency" in CANDIDATE_FEATURE_COLUMNS and "amount_zscore" in CANDIDATE_FEATURE_COLUMNS
    assert set(CANDIDATE_FEATURE_COLUMNS) | set(NON_FEATURE_COLUMNS) == set(SOURCE_SCHEMA.names) | {
        "fraud_label",
        "label_observed_at",
        "label_source",
        "label_revision",
    }


def test_event_time_then_event_id_determines_dataset_order(tmp_path: Path) -> None:
    path = source(tmp_path, [feature(1, event_time=time(2)), feature(3), feature(2)])
    labels = write_labels(tmp_path, [label(1), label(2), label(3)])
    snapshot = build_dataset(path, labels, time(5), tmp_path / "output")
    assert [row["event_id"] for row in dataset_rows(snapshot.path)] == [
        event_id(2),
        event_id(3),
        event_id(1),
    ]


def test_cli_build_inspect_and_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = source(tmp_path, [feature(1)])
    labels = write_labels(tmp_path, [label(1)])
    args = [
        "build",
        "--features-delta",
        str(path),
        "--labels",
        str(labels),
        "--as-of",
        "2030-01-05T00:00:00Z",
        "--output-root",
        str(tmp_path / "output"),
        "--source-version",
        "0",
    ]
    assert main(args) == 0
    snapshot = next((tmp_path / "output").iterdir())
    assert main(["inspect", "--snapshot", str(snapshot)]) == 0
    assert '"labeled_dataset_rows": 1' in capsys.readouterr().out
    assert main([*args[:], "--as-of", "2030-01-05T00:00:00"]) == 1
    assert "Dataset contract failure" in capsys.readouterr().err
