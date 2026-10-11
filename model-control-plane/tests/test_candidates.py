"""Phase 16 candidate verification uses isolated local snapshots and artifact roots."""

import hashlib
import json
import shutil
from decimal import Decimal
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq
import pytest
from mlflow.tracking import MlflowClient
from sklearn.linear_model import LogisticRegression
from test_training import config, make_snapshot, moment

from sentinelaegisforge_control_plane.candidates.cli import main as candidate_cli
from sentinelaegisforge_control_plane.candidates.contract import (
    FEATURES,
    INPUT_CONTRACT,
    INPUT_FINGERPRINT,
    CandidateError,
    validate_input,
)
from sentinelaegisforge_control_plane.candidates.run import (
    _classes,
    _cohorts,
    _compare,
    _validated_source,
    build_candidate,
    inspect_candidate,
    score_candidate,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig, track_run
from sentinelaegisforge_control_plane.training.baseline import (
    feature_frame,
    fit_baseline,
    probability_estimates,
)
from sentinelaegisforge_control_plane.training.run import train_baseline


@pytest.fixture(scope="module")
def source(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("candidate-source")
    snapshot = make_snapshot(root)
    run = train_baseline(snapshot, config(), root / "runs")
    return run.path, snapshot


@pytest.fixture(scope="module")
def candidate(tmp_path_factory: pytest.TempPathFactory, source: tuple[Path, Path]) -> Path:
    return build_candidate(source[0], tmp_path_factory.mktemp("candidate-output"))


def _row(snapshot: Path) -> dict[str, object]:
    record = cast(dict[str, object], pq.read_table(snapshot / "dataset.parquet").to_pylist()[0])
    return {name: record[name] for name in FEATURES}


def test_exact_input_contract_and_decimal_projection(source: tuple[Path, Path]) -> None:
    row = _row(source[1])
    assert tuple(cast(list[str], INPUT_CONTRACT["ordered_features"])) == FEATURES
    assert len(FEATURES) == 11
    assert INPUT_FINGERPRINT == hashlib.sha256(canonical_json(INPUT_CONTRACT)).hexdigest()
    assert isinstance(validate_input(row)["amount"], Decimal)
    projected = feature_frame((validate_input(row),), FEATURES)
    assert isinstance(projected.loc[0, "amount"], float)
    assert isinstance(row["amount"], Decimal)
    assert validate_input({**row, "currency": "GBP"})["currency"] == "GBP"


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": -1},
        {"amount": "10.0000"},
        {"amount": Decimal("0.0000")},
        {"amount": Decimal("1.00001")},
        {"prior_amount_sum_10m": float("inf")},
        {"prior_transaction_count_5m": -1},
        {"prior_amount_observation_count": True},
        {"currency": 1},
        {"currency": "ÅBC"},
        {"statistical_feature_status": "UNKNOWN"},
        {"prior_amount_stddev": float("nan")},
    ],
)
def test_invalid_input_rejected(source: tuple[Path, Path], changes: dict[str, object]) -> None:
    with pytest.raises(CandidateError):
        validate_input({**_row(source[1]), **changes})


def test_missing_extra_and_null_status_contract(source: tuple[Path, Path]) -> None:
    row = _row(source[1])
    with pytest.raises(CandidateError, match="Input features differ"):
        validate_input({key: value for key, value in row.items() if key != "currency"})
    with pytest.raises(CandidateError, match="Input features differ"):
        validate_input({**row, "fraud_label": 1})
    with pytest.raises(CandidateError, match="column names"):
        validate_input({**row, 4: "unexpected"})  # type: ignore[dict-item]
    no_history = {
        **row,
        "prior_amount_observation_count": 0,
        "prior_amount_mean": None,
        "prior_amount_stddev": None,
        "amount_zscore": None,
        "statistical_feature_status": "NO_HISTORY",
    }
    validate_input(no_history)
    with pytest.raises(CandidateError, match="status/null"):
        validate_input({**no_history, "amount_zscore": 1.0})


def test_reconstruction_matches_archived_probabilities_and_training_only_fit(
    source: tuple[Path, Path],
) -> None:
    run, snapshot, path = _validated_source(source[0])
    cohorts = _cohorts(run, snapshot, path)
    assert len(cohorts.train) == 24
    assert cohorts.excluded_from_fit_due_to_label_delay == 1
    model = fit_baseline(cohorts.train, FEATURES)
    encoder = model.named_steps["preprocessing"].named_transformers_["categorical"]
    assert list(encoder.categories_[0]) == ["USD"]
    assert "EUR" not in encoder.categories_[0]
    assert "JPY" not in encoder.categories_[0]
    result = _compare(model, cohorts, source[0])
    assert result["compared_predictions"] == 24
    assert result["mismatches"] == 0
    assert result["positive_class"] == 1
    assert probability_estimates(model, cohorts.validation, FEATURES)
    classifier = cast(LogisticRegression, model.named_steps["classifier"])
    classifier.classes_ = classifier.classes_[::-1]
    with pytest.raises(CandidateError, match="class order"):
        _classes(model)


def test_modified_prediction_and_estimator_are_rejected(
    source: tuple[Path, Path], tmp_path: Path
) -> None:
    copied = tmp_path / source[0].name
    shutil.copytree(source[0], copied)
    table = pq.read_table(copied / "predictions.parquet")
    rows = table.to_pylist()
    rows[0]["fraud_probability"] = 0.5 if rows[0]["fraud_probability"] != 0.5 else 0.4
    import pyarrow as pa

    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), copied / "predictions.parquet")
    manifest = json.loads((copied / "manifest.json").read_bytes())
    manifest["predictions_parquet_sha256"] = hashlib.sha256(
        (copied / "predictions.parquet").read_bytes()
    ).hexdigest()
    (copied / "manifest.json").write_bytes(canonical_json(manifest))
    with pytest.raises(CandidateError, match="Archived prediction mismatch"):
        build_candidate(copied, tmp_path / "outputs")
    manifest["estimator_parameters"]["C"] = 2.0
    (copied / "manifest.json").write_bytes(canonical_json(manifest))
    with pytest.raises(ValueError):
        build_candidate(copied, tmp_path / "outputs")


def test_immutable_rebuild_and_offline_scoring(candidate: Path, source: tuple[Path, Path]) -> None:
    first = inspect_candidate(candidate)
    before = (candidate / "candidate_manifest.json").read_bytes()
    again = build_candidate(source[0], candidate.parent)
    assert again == candidate
    assert (candidate / "candidate_manifest.json").read_bytes() == before
    assert first["validation_status"] == "TECHNICALLY_VERIFIED"
    probabilities = score_candidate(candidate, [_row(source[1])])
    assert len(probabilities) == 1 and 0 <= probabilities[0] <= 1


def test_corrupt_conflicting_and_incomplete_candidate_fail_closed(
    candidate: Path, source: tuple[Path, Path], tmp_path: Path
) -> None:
    corrupt_root = tmp_path / "corrupt"
    corrupt_root.mkdir()
    corrupt = corrupt_root / candidate.name
    shutil.copytree(candidate, corrupt)
    model_file = corrupt / "model" / "model.skops"
    model_file.write_bytes(model_file.read_bytes() + b"tampered")
    with pytest.raises(CandidateError, match="hash differs"):
        inspect_candidate(corrupt)
    with pytest.raises(CandidateError, match="hash differs"):
        build_candidate(source[0], corrupt_root)

    conflict_root = tmp_path / "conflict"
    conflict_root.mkdir()
    conflict = conflict_root / candidate.name
    shutil.copytree(candidate, conflict)
    manifest = json.loads((conflict / "candidate_manifest.json").read_bytes())
    manifest["runtime_versions"]["scikit_learn"] = "0.0.0"
    (conflict / "candidate_manifest.json").write_bytes(canonical_json(manifest))
    with pytest.raises(CandidateError, match="runtime differs"):
        build_candidate(source[0], conflict_root)

    incomplete_root = tmp_path / "incomplete"
    incomplete = incomplete_root / candidate.name
    incomplete.mkdir(parents=True)
    with pytest.raises(CandidateError, match="Incomplete"):
        build_candidate(source[0], incomplete_root)


def test_candidate_id_changes_with_source_and_not_output_root(
    source: tuple[Path, Path], candidate: Path, tmp_path: Path
) -> None:
    second_root = tmp_path / "independent-output"
    same = build_candidate(source[0], second_root)
    assert same.name == candidate.name
    alternative = train_baseline(source[1], config(train_end=moment(3, 1)), tmp_path / "runs")
    other = build_candidate(alternative.path, tmp_path / "other-output")
    assert other.name != candidate.name


def test_tracking_reference_is_read_only_and_conflicts_fail(
    source: tuple[Path, Path], tmp_path: Path
) -> None:
    tracking = TrackingConfig(
        tmp_path / "tracking.db", tmp_path / "artifacts", "isolated-candidate-experiment"
    )
    tracked = track_run(source[0], tracking)
    output = build_candidate(
        source[0],
        tmp_path / "candidates",
        tracking_run_id=tracked["mlflow_run_id"],
        tracking=tracking,
    )
    assert (
        inspect_candidate(output, tracking=tracking)["source_mlflow_run_id"]
        == tracked["mlflow_run_id"]
    )
    client = MlflowClient(tracking_uri=tracking.tracking_uri)
    client.set_tag(tracked["mlflow_run_id"], "dataset_snapshot_id", "wrong-snapshot")
    with pytest.raises(CandidateError, match="Tracked source tag differs"):
        inspect_candidate(output, tracking=tracking)
    with pytest.raises(CandidateError, match="Tracked source tag differs"):
        build_candidate(
            source[0],
            output.parent,
            tracking_run_id=tracked["mlflow_run_id"],
            tracking=tracking,
        )


def test_unsupported_serialization_is_rejected_before_load(candidate: Path, tmp_path: Path) -> None:
    copied = tmp_path / candidate.name
    shutil.copytree(candidate, copied)
    model_metadata = copied / "model" / "MLmodel"
    model_metadata.write_text(
        model_metadata.read_text().replace(
            "serialization_format: skops", "serialization_format: pickle"
        )
    )
    manifest = json.loads((copied / "candidate_manifest.json").read_bytes())
    files = {
        path.relative_to(copied / "model").as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((copied / "model").rglob("*"))
        if path.is_file()
    }
    manifest["model_files"] = files
    manifest["model_tree_sha256"] = hashlib.sha256(canonical_json(files)).hexdigest()
    (copied / "candidate_manifest.json").write_bytes(canonical_json(manifest))
    with pytest.raises(CandidateError, match="Untrusted/unsupported"):
        inspect_candidate(copied)


def test_offline_cli_scores_only_explicit_decimal_strings(
    candidate: Path, source: tuple[Path, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    row = _row(source[1])
    for name in ("amount", "prior_amount_sum_10m"):
        row[name] = str(row[name])
    input_path = tmp_path / "one-row.json"
    input_path.write_text(json.dumps(row))
    assert (
        candidate_cli(["score", "--candidate", str(candidate), "--input-json", str(input_path)])
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert 0 <= output["fraud_probability_estimate"] <= 1
    assert output["positive_class"] == "FRAUD"
    row["amount"] = 10.0
    input_path.write_text(json.dumps(row))
    assert (
        candidate_cli(["score", "--candidate", str(candidate), "--input-json", str(input_path)])
        == 1
    )
    assert "decimal string" in capsys.readouterr().err
