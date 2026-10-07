"""Controlled Phase 12 snapshots prove mechanics, never fraud-model quality."""

import json
import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import write_deltalake

from sentinelaegisforge_control_plane.datasets.dataset_builder import build_dataset
from sentinelaegisforge_control_plane.datasets.dataset_contract import (
    CANDIDATE_FEATURE_COLUMNS,
    NON_FEATURE_COLUMNS,
    SOURCE_SCHEMA,
)
from sentinelaegisforge_control_plane.datasets.errors import SnapshotConflictError
from sentinelaegisforge_control_plane.training.baseline import (
    build_pipeline,
    evaluate,
    feature_frame,
    fit_baseline,
    probability_estimates,
)
from sentinelaegisforge_control_plane.training.cli import main
from sentinelaegisforge_control_plane.training.errors import (
    InsufficientTrainingDataError,
    TrainingContractError,
)
from sentinelaegisforge_control_plane.training.run import inspect_run, train_baseline
from sentinelaegisforge_control_plane.training.temporal import TemporalConfig, partition_rows


def moment(day: int, minute: int = 0) -> datetime:
    return datetime(2030, 1, day, tzinfo=UTC) + timedelta(minutes=minute)


def event_id(number: int) -> str:
    return str(UUID(int=number))


def config(**changes: object) -> TemporalConfig:
    params: dict[str, object] = {
        "train_end": moment(3),
        "validation_end": moment(5),
        "test_end": moment(7),
        **changes,
    }
    return TemporalConfig(**params)  # type: ignore[arg-type]


def feature(number: int, ingestion: datetime, amount: Decimal, currency: str) -> dict[str, object]:
    ready = number % 3 != 0
    return {
        "event_id": event_id(number),
        "transaction_id": f"transaction-{number}",
        "customer_id": f"customer-{number % 4}",
        "merchant_id": f"merchant-{number}",
        "event_time": ingestion,
        "ingestion_time": ingestion,
        "amount": amount,
        "currency": currency,
        "country": "US",
        "device_id": f"device-{number}",
        "ip_address": "192.0.2.1",
        "transaction_type": "CARD_PAYMENT",
        "schema_version": 1,
        "kafka_key": f"customer-{number % 4}",
        "kafka_topic": "transactions.raw",
        "kafka_partition": 0,
        "kafka_offset": number,
        "kafka_timestamp": ingestion,
        "prior_transaction_count_5m": number % 5,
        "prior_amount_sum_10m": Decimal("100.0000"),
        "prior_amount_observation_count": 2 if ready else 0,
        "prior_amount_mean": 100.0 if ready else None,
        "prior_amount_stddev": 10.0 if ready else None,
        "amount_zscore": (float(amount) - 100.0) / 10.0 if ready else None,
        "statistical_feature_status": "READY" if ready else "NO_HISTORY",
    }


def make_snapshot(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, object]] = []
    labels: list[dict[str, object]] = []
    for number in range(1, 51):
        if number <= 25:
            ingestion = moment(1, number)
            observed = moment(9) if number == 25 else moment(2)
            amount = Decimal(100 + number).quantize(Decimal("0.0001"))
            currency = "USD"
        elif number <= 37:
            ingestion = moment(3, number - 26)  # First validation row is exactly train_end.
            observed = moment(8)
            amount = Decimal(10000 + number).quantize(Decimal("0.0001"))
            currency = "EUR"
        elif number <= 49:
            ingestion = moment(5, number - 38)  # First test row is exactly validation_end.
            observed = moment(8)
            amount = Decimal(1000 + number).quantize(Decimal("0.0001"))
            currency = "JPY"
        else:
            ingestion = moment(7)  # Exactly test_end: outside every configured split.
            observed = moment(8)
            amount = Decimal("1500.0000")
            currency = "JPY"
        records.append(feature(number, ingestion, amount, currency))
        labels.append(
            {
                "event_id": event_id(number),
                "outcome": "FRAUD" if number % 2 == 0 else "LEGIT",
                "label_observed_at": observed.isoformat().replace("+00:00", "Z"),
                "label_source": "SYNTHETIC_FIXTURE",
                "label_revision": 1,
            }
        )
    source = root / "transaction_statistical_features_v2"
    write_deltalake(source, pa.Table.from_pylist(records, schema=SOURCE_SCHEMA))
    label_path = root / "labels.jsonl"
    label_path.write_text("".join(json.dumps(value) + "\n" for value in labels), encoding="utf-8")
    return build_dataset(source, label_path, moment(10), root / "snapshots").path


@pytest.fixture(scope="module")
def snapshot(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return make_snapshot(tmp_path_factory.mktemp("baseline-fixture"))


def snapshot_rows(snapshot: Path) -> tuple[list[dict[str, object]], dict[str, object]]:
    rows = cast(list[dict[str, object]], pq.read_table(snapshot / "dataset.parquet").to_pylist())
    manifest = cast(dict[str, object], json.loads((snapshot / "manifest.json").read_text()))
    return rows, manifest


def test_half_open_ingestion_boundaries_and_delayed_fit_label(snapshot: Path) -> None:
    rows, manifest = snapshot_rows(snapshot)
    cohorts = partition_rows(rows, manifest, config())
    assert (len(cohorts.train), len(cohorts.validation), len(cohorts.test)) == (24, 12, 12)
    assert cohorts.train_period_rows == 25
    assert cohorts.excluded_from_fit_due_to_label_delay == 1
    assert cohorts.outside_test_window_rows == 1
    assert event_id(25) not in {row["event_id"] for row in cohorts.train}
    assert cohorts.validation[0]["event_id"] == event_id(26)
    assert cohorts.test[0]["event_id"] == event_id(38)
    assert event_id(50) not in {
        row["event_id"]
        for group in (cohorts.train, cohorts.validation, cohorts.test)
        for row in group
    }
    assert not (
        {row["event_id"] for row in cohorts.train}
        & {row["event_id"] for row in cohorts.validation + cohorts.test}
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"validation_end": moment(3)},
        {"test_end": moment(5)},
        {"train_end": datetime(2030, 1, 3)},
        {"min_train_rows": 0},
        {"min_test_rows": True},
    ],
)
def test_invalid_boundary_configuration_fails(changes: dict[str, object]) -> None:
    with pytest.raises(TrainingContractError):
        config(**changes)


def test_test_end_cannot_exceed_snapshot_as_of(snapshot: Path) -> None:
    rows, manifest = snapshot_rows(snapshot)
    with pytest.raises(TrainingContractError, match="test_end must not exceed"):
        partition_rows(rows, manifest, config(test_end=moment(11)))


def test_candidate_contract_and_duplicate_identity_are_checked(snapshot: Path) -> None:
    rows, manifest = snapshot_rows(snapshot)
    with pytest.raises(TrainingContractError, match="candidate feature contract"):
        partition_rows(rows, {**manifest, "candidate_feature_columns": ["event_id"]}, config())
    with pytest.raises(TrainingContractError, match="Duplicate event_id"):
        partition_rows([*rows, rows[0]], manifest, config())


def test_class_support_and_minimum_rows_fail_before_fit(snapshot: Path) -> None:
    rows, manifest = snapshot_rows(snapshot)
    with pytest.raises(InsufficientTrainingDataError, match="training has 24 rows"):
        partition_rows(rows, manifest, config(min_train_rows=25))
    one_class = [
        {**row, "fraud_label": 0} if cast(datetime, row["ingestion_time"]) < moment(3) else row
        for row in rows
    ]
    with pytest.raises(
        InsufficientTrainingDataError, match="training requires both FRAUD and LEGIT"
    ):
        partition_rows(one_class, manifest, config())


def test_training_only_encoder_scaler_and_feature_projection(snapshot: Path) -> None:
    rows, manifest = snapshot_rows(snapshot)
    cohorts = partition_rows(rows, manifest, config())
    candidates = tuple(CANDIDATE_FEATURE_COLUMNS)
    frame = feature_frame(cohorts.train, candidates)
    assert list(frame.columns) == list(candidates)
    assert not set(frame.columns) & set(NON_FEATURE_COLUMNS)
    assert not set(frame.columns) & {
        "risk_disposition",
        "matched_rule_ids",
        "reason_codes",
        "policy_version",
    }
    assert isinstance(cohorts.train[0]["amount"], Decimal)
    model = fit_baseline(cohorts.train, candidates)
    preprocessing = model.named_steps["preprocessing"]
    encoder = preprocessing.named_transformers_["categorical"]
    scaler = preprocessing.named_transformers_["numeric"].named_steps["scaler"]
    assert list(encoder.categories_[0]) == ["USD"]
    assert encoder.handle_unknown == "ignore"
    assert scaler.mean_[0] == pytest.approx(sum(range(101, 125)) / 24)
    assert float(cast(Decimal, cohorts.validation[0]["amount"])) > max(range(101, 125))
    assert len(probability_estimates(model, cohorts.validation, candidates)) == 12
    assert build_pipeline().named_steps["classifier"].class_weight == "balanced"


def test_evaluation_transforms_do_not_fit_on_test_rows(snapshot: Path) -> None:
    rows, manifest = snapshot_rows(snapshot)
    cohorts = partition_rows(rows, manifest, config())
    candidates = tuple(CANDIDATE_FEATURE_COLUMNS)
    model = fit_baseline(cohorts.train, candidates)
    preprocessing = model.named_steps["preprocessing"]
    encoder = preprocessing.named_transformers_["categorical"]
    scaler = preprocessing.named_transformers_["numeric"].named_steps["scaler"]
    classifier = model.named_steps["classifier"]
    original_categories = [list(values) for values in encoder.categories_]
    original_mean = list(scaler.mean_)
    original_coefficients = classifier.coef_.copy()
    probability_estimates(model, cohorts.validation, candidates)
    probability_estimates(model, cohorts.test, candidates)
    assert [list(values) for values in encoder.categories_] == original_categories
    assert list(scaler.mean_) == original_mean
    assert (classifier.coef_ == original_coefficients).all()
    assert "EUR" not in encoder.categories_[0]
    assert "JPY" not in encoder.categories_[0]


def test_precision_is_unavailable_without_positive_predictions(snapshot: Path) -> None:
    rows, manifest = snapshot_rows(snapshot)
    validation = partition_rows(rows, manifest, config()).validation
    metrics = evaluate(validation, [0.0] * len(validation))
    assert metrics["precision_at_0_5"] is None
    assert metrics["recall_at_0_5"] == 0.0
    assert metrics["confusion_matrix"] == {"tn": 6, "fp": 0, "fn": 6, "tp": 0}


def test_training_run_metrics_predictions_and_provenance(snapshot: Path, tmp_path: Path) -> None:
    run = train_baseline(snapshot, config(), tmp_path / "runs")
    manifest = run.manifest
    metrics = json.loads((run.path / "metrics.json").read_text())
    predictions = pq.read_table(run.path / "predictions.parquet").to_pylist()
    dataset_manifest = json.loads((snapshot / "manifest.json").read_text())
    assert manifest["dataset_snapshot_id"] == dataset_manifest["snapshot_id"]
    assert (
        manifest["dataset_logical_fingerprint"] == dataset_manifest["logical_dataset_fingerprint"]
    )
    assert manifest["source_delta_version"] == dataset_manifest["source_delta_version"]
    assert manifest["source_schema_fingerprint"] == dataset_manifest["source_schema_fingerprint"]
    assert manifest["dataset_as_of_time"] == dataset_manifest["as_of_time"]
    assert manifest["cohort_support"] == {
        "training": {"rows": 24, "positives": 12, "negatives": 12},
        "validation": {"rows": 12, "positives": 6, "negatives": 6},
        "test": {"rows": 12, "positives": 6, "negatives": 6},
    }
    assert manifest["excluded_from_fit_due_to_label_delay"] == 1
    assert [row["split"] for row in predictions] == ["validation"] * 12 + ["test"] * 12
    assert all(0 <= row["fraud_probability"] <= 1 for row in predictions)
    assert all(
        row["predicted_label_at_0_5"] == int(row["fraud_probability"] >= 0.5) for row in predictions
    )
    assert set(metrics) == {"validation", "test"}
    for split in ("validation", "test"):
        values = metrics[split]
        assert values["rows"] == 12 and values["positives"] == 6 and values["negatives"] == 6
        assert 0 <= values["average_precision"] <= 1
        assert 0 <= values["roc_auc"] <= 1
        assert sum(values["confusion_matrix"].values()) == 12
    assert inspect_run(run.path) == manifest


def test_identical_run_is_reproducible_and_threshold_is_not_tuned(
    snapshot: Path, tmp_path: Path
) -> None:
    first = train_baseline(snapshot, config(), tmp_path / "runs")
    original = {item.name: item.read_bytes() for item in first.path.iterdir()}
    repeat = train_baseline(snapshot, config(), tmp_path / "runs")
    other_root = train_baseline(snapshot, config(), tmp_path / "other-runs")
    assert first.path == repeat.path
    assert first.manifest == repeat.manifest == other_root.manifest
    assert original == {item.name: item.read_bytes() for item in first.path.iterdir()}
    left = pq.read_table(first.path / "predictions.parquet")["fraud_probability"].to_pylist()
    right = pq.read_table(other_root.path / "predictions.parquet")["fraud_probability"].to_pylist()
    assert left == pytest.approx(right, rel=1e-12, abs=1e-12)
    assert json.loads((first.path / "metrics.json").read_text()) == json.loads(
        (other_root.path / "metrics.json").read_text()
    )
    assert first.manifest["classification_threshold"] == 0.5
    changed = train_baseline(snapshot, config(min_train_rows=21), tmp_path / "runs")
    assert changed.manifest["training_run_id"] != first.manifest["training_run_id"]


def test_corrupt_snapshot_and_run_artifacts_fail_closed(snapshot: Path, tmp_path: Path) -> None:
    copied = tmp_path / "snapshot"
    shutil.copytree(snapshot, copied)
    (copied / "dataset.parquet").write_bytes(b"corrupt")
    with pytest.raises(SnapshotConflictError):
        train_baseline(copied, config(), tmp_path / "runs")
    assert not (tmp_path / "runs").exists()
    run = train_baseline(snapshot, config(), tmp_path / "runs")
    (run.path / "metrics.json").write_bytes(b"corrupt")
    with pytest.raises(TrainingContractError):
        train_baseline(snapshot, config(), tmp_path / "runs")


def test_tiny_snapshot_is_rejected_without_creating_run(tmp_path: Path) -> None:
    snapshot = make_snapshot(tmp_path / "sufficient")
    # Deliberately use a valid Phase 12 snapshot with only four labeled rows.
    rows, _ = snapshot_rows(snapshot)
    tiny_root = tmp_path / "tiny"
    tiny_source = tiny_root / "transaction_statistical_features_v2"
    tiny_root.mkdir()
    selected = rows[:4]
    source_rows = [{field.name: row[field.name] for field in SOURCE_SCHEMA} for row in selected]
    write_deltalake(tiny_source, pa.Table.from_pylist(source_rows, schema=SOURCE_SCHEMA))
    labels = tiny_root / "labels.jsonl"
    labels.write_text(
        "".join(
            json.dumps(
                {
                    "event_id": row["event_id"],
                    "outcome": "FRAUD" if row["fraud_label"] else "LEGIT",
                    "label_observed_at": moment(2).isoformat().replace("+00:00", "Z"),
                    "label_source": "SYNTHETIC_FIXTURE",
                    "label_revision": 1,
                }
            )
            + "\n"
            for row in selected
        ),
        encoding="utf-8",
    )
    tiny = build_dataset(tiny_source, labels, moment(10), tiny_root / "snapshots")
    with pytest.raises(InsufficientTrainingDataError, match="requires at least"):
        train_baseline(tiny.path, config(), tmp_path / "runs")
    assert not (tmp_path / "runs").exists()


def test_cli_train_and_inspect(
    snapshot: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    arguments = [
        "train",
        "--dataset-snapshot",
        str(snapshot),
        "--train-end",
        "2030-01-03T00:00:00Z",
        "--validation-end",
        "2030-01-05T00:00:00Z",
        "--test-end",
        "2030-01-07T00:00:00Z",
        "--output-root",
        str(tmp_path / "runs"),
    ]
    assert main(arguments) == 0
    run = next((tmp_path / "runs").iterdir())
    assert main(["inspect", "--run", str(run)]) == 0
    assert '"training_contract_version": "fraud-logistic-baseline-v1"' in capsys.readouterr().out
    assert main([*arguments, "--train-end", "2030-01-03T00:00:00"]) == 1
    assert "Training contract failure" in capsys.readouterr().err
