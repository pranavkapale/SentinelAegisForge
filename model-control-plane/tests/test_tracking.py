"""MLflow is a verified index of immutable baseline runs, not a trainer."""

import hashlib
import json
import shutil
from pathlib import Path

import pytest
from mlflow.entities import Run, ViewType
from mlflow.tracking import MlflowClient
from test_training import config, make_snapshot, moment

from sentinelaegisforge_control_plane.datasets.dataset_builder import build_dataset
from sentinelaegisforge_control_plane.tracking.run import (
    TrackingConfig,
    TrackingError,
    _metrics,
    compare_tracked_runs,
    inspect_tracked_run,
    track_run,
)
from sentinelaegisforge_control_plane.training.errors import TrainingContractError
from sentinelaegisforge_control_plane.training.run import train_baseline


@pytest.fixture(scope="module")
def training_run(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("mlflow-training")
    snapshot = make_snapshot(root)
    return train_baseline(snapshot, config(), root / "runs").path


def tracking_config(root: Path) -> TrackingConfig:
    return TrackingConfig(root / "tracking.db", root / "artifacts", "test-synthetic-baseline")


def matching_runs(client: MlflowClient, experiment_id: str, training_id: str) -> list[Run]:
    return list(
        client.search_runs(
            [experiment_id],
            filter_string=f"tags.training_run_id = '{training_id}'",
            run_view_type=ViewType.ALL,
        )
    )


def test_track_readback_repeat_and_read_only_inspection(
    training_run: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = tracking_config(tmp_path)
    source_hashes = {
        name: hashlib.sha256((training_run / name).read_bytes()).hexdigest()
        for name in ("manifest.json", "metrics.json", "predictions.parquet")
    }
    first = track_run(training_run, settings)
    assert first["reused"] == "false"
    assert first["training_run_id"] == training_run.name
    client = MlflowClient(tracking_uri=settings.tracking_uri)
    stored = client.get_run(first["mlflow_run_id"])
    assert stored.info.status == "FINISHED"
    assert stored.data.tags["data_origin"] == "synthetic"
    assert stored.data.tags["production_fraud_quality"] == "unproven"
    assert stored.data.params["estimator.solver"] == "lbfgs"
    assert stored.data.metrics["validation.average_precision"] == pytest.approx(0.5)
    assert len(matching_runs(client, first["experiment_id"], training_run.name)) == 1

    def cannot_create(*args: object, **kwargs: object) -> None:
        raise AssertionError("Read-only operations must not create an MLflow run")

    monkeypatch.setattr(MlflowClient, "create_run", cannot_create)
    second = track_run(training_run, settings)
    assert second["reused"] == "true"
    assert second["mlflow_run_id"] == first["mlflow_run_id"]
    assert source_hashes == {
        name: hashlib.sha256((training_run / name).read_bytes()).hexdigest()
        for name in source_hashes
    }
    inspected = inspect_tracked_run(settings, first["mlflow_run_id"])
    assert inspected["training_run_id"] == training_run.name
    assert inspected["model_family"] == "logistic_regression"
    assert "Synthetic" in str(inspected["warning"])
    comparison = compare_tracked_runs(settings)
    assert isinstance(comparison["runs"], list)
    assert len(comparison["runs"]) == 1
    assert comparison["warning"] is None


def test_conflicting_metric_and_dataset_identity_fail_closed(
    training_run: Path, tmp_path: Path
) -> None:
    settings = tracking_config(tmp_path)
    result = track_run(training_run, settings)
    client = MlflowClient(tracking_uri=settings.tracking_uri)
    client.log_metric(result["mlflow_run_id"], "test.average_precision", 0.999)
    with pytest.raises(TrackingError, match="metrics differ"):
        track_run(training_run, settings)
    assert len(matching_runs(client, result["experiment_id"], training_run.name)) == 1
    client.log_metric(result["mlflow_run_id"], "test.average_precision", 0.6111111111111112)
    client.set_tag(result["mlflow_run_id"], "dataset_snapshot_id", "wrong-snapshot")
    with pytest.raises(TrackingError, match="tag differs"):
        track_run(training_run, settings)
    assert len(matching_runs(client, result["experiment_id"], training_run.name)) == 1


def test_corrupt_training_artifact_is_rejected_before_tracking(
    training_run: Path, tmp_path: Path
) -> None:
    source = tmp_path / training_run.name
    shutil.copytree(training_run, source)
    (source / "metrics.json").write_text("{}", encoding="utf-8")
    settings = tracking_config(tmp_path / "backend")
    with pytest.raises(TrainingContractError, match="artifact hash"):
        track_run(source, settings)
    assert not settings.database.exists()


def test_duplicate_claims_and_incomplete_run_fail_closed(
    training_run: Path, tmp_path: Path
) -> None:
    settings = tracking_config(tmp_path)
    result = track_run(training_run, settings)
    client = MlflowClient(tracking_uri=settings.tracking_uri)
    second = client.create_run(result["experiment_id"], tags={"training_run_id": training_run.name})
    with pytest.raises(TrackingError, match="Multiple MLflow runs"):
        track_run(training_run, settings)
    client.set_terminated(second.info.run_id, status="FAILED")
    assert len(matching_runs(client, result["experiment_id"], training_run.name)) == 2


def test_partial_logging_failure_remains_visible_and_retry_is_not_repaired(
    training_run: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = tracking_config(tmp_path)

    def fail_upload(*args: object, **kwargs: object) -> None:
        raise OSError("simulated artifact upload failure")

    with monkeypatch.context() as scoped:
        scoped.setattr(MlflowClient, "log_artifact", fail_upload)
        with pytest.raises(OSError, match="simulated artifact"):
            track_run(training_run, settings)
    client = MlflowClient(tracking_uri=settings.tracking_uri)
    experiment = client.get_experiment_by_name(settings.experiment_name)
    assert experiment is not None
    runs = matching_runs(client, experiment.experiment_id, training_run.name)
    assert len(runs) == 1
    assert runs[0].info.status == "FAILED"
    with pytest.raises(TrackingError, match="not complete"):
        track_run(training_run, settings)
    assert len(matching_runs(client, experiment.experiment_id, training_run.name)) == 1


def test_multiple_run_comparison_warns_on_different_periods(
    training_run: Path, tmp_path: Path
) -> None:
    settings = tracking_config(tmp_path / "backend")
    first = track_run(training_run, settings)
    manifest = json.loads((training_run / "manifest.json").read_text(encoding="utf-8"))
    second_source = train_baseline(
        Path(manifest["dataset_snapshot_path"]),
        config(train_end=moment(3, 1)),
        tmp_path / "second-runs",
    ).path
    second = track_run(second_source, settings)
    assert first["mlflow_run_id"] != second["mlflow_run_id"]
    comparison = compare_tracked_runs(settings)
    assert isinstance(comparison["runs"], list) and len(comparison["runs"]) == 2
    assert "not directly comparable" in str(comparison["warning"])


def test_missing_metric_is_not_replaced_with_zero(training_run: Path) -> None:
    metrics = json.loads((training_run / "metrics.json").read_text(encoding="utf-8"))
    metrics["validation"]["precision_at_0_5"] = None
    flattened = _metrics(metrics)
    assert "validation.precision_at_0_5" not in flattened
    assert flattened["validation.average_precision"] == metrics["validation"]["average_precision"]


def test_inspection_of_missing_backend_is_read_only(tmp_path: Path) -> None:
    settings = tracking_config(tmp_path)
    with pytest.raises(TrackingError, match="does not exist"):
        compare_tracked_runs(settings)
    assert not settings.database.exists()


def test_non_synthetic_labels_cannot_be_misrepresented_as_synthetic(
    training_run: Path, tmp_path: Path
) -> None:
    training_manifest = json.loads((training_run / "manifest.json").read_text(encoding="utf-8"))
    snapshot_path = Path(training_manifest["dataset_snapshot_path"])
    snapshot_manifest = json.loads((snapshot_path / "manifest.json").read_text(encoding="utf-8"))
    original_labels = Path(snapshot_manifest["label_source_file"])
    replacement = tmp_path / "external-labels.jsonl"
    replacement.write_text(
        original_labels.read_text(encoding="utf-8").replace("SYNTHETIC_FIXTURE", "EXTERNAL"),
        encoding="utf-8",
    )
    external_snapshot = build_dataset(
        Path(snapshot_manifest["source_delta_path"]),
        replacement,
        moment(10),
        tmp_path / "snapshots",
    ).path
    external_run = train_baseline(external_snapshot, config(), tmp_path / "runs").path
    settings = tracking_config(tmp_path / "backend")
    with pytest.raises(TrackingError, match="synthetic label provenance"):
        track_run(external_run, settings)
    assert not settings.database.exists()
