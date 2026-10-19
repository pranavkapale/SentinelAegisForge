"""Phase 23 local shadow lifecycle, request idempotence, failure isolation and parity."""

import json
import shutil
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake
from sklearn.pipeline import Pipeline
from test_training import config, make_snapshot, moment

from sentinelaegisforge_control_plane.candidates.run import build_candidate, load_verified_candidate
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.monitoring.contract import row_fingerprint
from sentinelaegisforge_control_plane.monitoring.ledger import build_ledger, inspect_ledger
from sentinelaegisforge_control_plane.registry.run import (
    RegistryConfig,
    _source_path,
    inspect_registered_candidate,
    register_candidate,
)
from sentinelaegisforge_control_plane.shadow.contract import RequestInferenceError, ShadowError
from sentinelaegisforge_control_plane.shadow.execution import (
    execute_plan,
    inspect_execution,
    run_deliveries,
)
from sentinelaegisforge_control_plane.shadow.parity import compare_rows, verify_parity
from sentinelaegisforge_control_plane.shadow.plan import _requests, build_plan, inspect_plan
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig, track_run
from sentinelaegisforge_control_plane.training.run import train_baseline


@pytest.fixture(scope="module")
def shadow_replay(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, RegistryConfig, Path, Path, Path, Path]:
    root = tmp_path_factory.mktemp("shadow-replay")
    snapshot = make_snapshot(root)
    training = train_baseline(snapshot, config(), root / "runs")
    tracking = TrackingConfig(root / "tracking.db", root / "artifacts", "shadow-test")
    tracked = track_run(training.path, tracking)
    candidate = build_candidate(
        training.path,
        root / "candidates",
        tracking_run_id=tracked["mlflow_run_id"],
        tracking=tracking,
    )
    registry = RegistryConfig(tracking, "shadow-test-model", root / "receipts")
    register_candidate(candidate, registry)
    source = root / "transaction_statistical_features_v2"
    ledger = build_ledger(registry, "1", source, 0, moment(3), moment(7), root / "ledgers")
    plan = build_plan(registry, "1", source, 0, moment(3), moment(7), root / "shadow-plans")
    execution = execute_plan(plan, registry, root / "shadow-executions")
    return root, registry, source, ledger, plan, execution


def test_deterministic_plan_and_fixed_source(
    shadow_replay: tuple[Path, RegistryConfig, Path, Path, Path, Path],
) -> None:
    root, registry, source, _, plan, _ = shadow_replay
    manifest, requests = inspect_plan(plan, registry)
    assert manifest["plan_contract_version"] == "shadow-replay-plan-v1"
    assert manifest["request_count"] == len(requests) == 24
    assert [row["replay_sequence"] for row in requests] == list(range(24))
    assert len({row["request_id"] for row in requests}) == 24
    source_rows = StatisticalFeatureSource(source, 0).read().table.to_pylist()
    ordered = sorted(
        (row for row in source_rows if moment(3) <= row["ingestion_time"] < moment(7)),
        key=lambda row: (row["ingestion_time"], row["event_id"]),
    )
    assert [row["event_id"] for row in requests] == [row["event_id"] for row in ordered]
    with pytest.raises(ShadowError, match="Duplicate logical"):
        _requests("plan", [ordered[0], ordered[0]], tuple(source_rows[0]))
    assert {
        "fraud_label",
        "risk_disposition",
        "customer_id",
        "fraud_probability_estimate",
    }.isdisjoint(requests[0])
    assert build_plan(registry, "1", source, 0, moment(3), moment(7), root / "shadow-plans") == plan
    changed_window = build_plan(
        registry, "1", source, 0, moment(3), moment(6), root / "other-plans"
    )
    assert changed_window.name != plan.name
    with pytest.raises(ValueError, match="numeric"):
        build_plan(registry, "latest", source, 0, moment(3), moment(7), root / "invalid")
    with pytest.raises(ShadowError, match="limit"):
        build_plan(registry, "1", source, 0, moment(3), moment(7), root / "invalid", maximum=2)


def test_execution_scores_every_request_and_repeats_immutably(
    shadow_replay: tuple[Path, RegistryConfig, Path, Path, Path, Path],
) -> None:
    root, registry, _, _, plan, execution = shadow_replay
    manifest = inspect_execution(execution, plan, registry)
    assert (
        manifest["planned_requests"],
        manifest["scored"],
        manifest["input_rejected"],
        manifest["execution_failed"],
    ) == (24, 24, 0, 0)
    assert manifest["unresolved_requests"] == manifest["duplicate_deliveries"] == 0
    assert manifest["execution_scope"] == "LOCAL_SYNTHETIC_SHADOW_REPLAY"
    assert manifest["feature_availability_evidence"] == "NOT_ESTABLISHED"
    rows = pq.read_table(execution / "results.parquet").to_pylist()
    assert len(rows) == 24
    assert {"fraud_label", "risk_disposition", "customer_id", "device_id", "ip_address"}.isdisjoint(
        rows[0]
    )
    artifact_hashes = cast(dict[str, str], manifest["artifact_sha256"])
    before = {name: (execution / name).read_bytes() for name in artifact_hashes}
    assert execute_plan(plan, registry, root / "shadow-executions") == execution
    assert before == {name: (execution / name).read_bytes() for name in artifact_hashes}


def test_failure_isolation_duplicate_delivery_and_conflict(
    shadow_replay: tuple[Path, RegistryConfig, Path, Path, Path, Path],
) -> None:
    _, registry, source, _, plan, _ = shadow_replay
    _, requests = inspect_plan(plan, registry)
    snapshot = StatisticalFeatureSource(source, 0).read()
    rows = {cast(str, row["event_id"]): row for row in snapshot.table.to_pylist()}
    columns = tuple(snapshot.table.column_names)
    calls = 0

    def scorer(row: dict[str, object]) -> float:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RequestInferenceError("private event-specific detail")
        return 0.25

    chosen = requests[:3]
    result, audit, duplicates = run_deliveries(
        [*chosen, chosen[0]], rows, "candidate", "1", scorer, columns=columns
    )
    assert calls == 3
    assert duplicates == 1
    assert [item["execution_status"] for item in result] == ["SCORED", "EXECUTION_FAILED", "SCORED"]
    assert result[1]["failure_code"] == "MODEL_INFERENCE_ERROR"
    assert "private event-specific detail" not in str(result) + str(audit)
    assert any(item["event"] == "DUPLICATE_DELIVERY" for item in audit)
    conflict = {**chosen[0], "event_id": "conflicting-event"}
    with pytest.raises(ShadowError, match="Conflicting duplicate"):
        run_deliveries([chosen[0], conflict], rows, "candidate", "1", scorer, columns=columns)
    invalid = dict(rows[cast(str, chosen[0]["event_id"])])
    invalid["prior_transaction_count_5m"] = -1
    request = {**chosen[0], "source_feature_row_fingerprint": row_fingerprint(invalid, columns)}
    rejected, _, _ = run_deliveries(
        [request],
        {cast(str, request["event_id"]): invalid},
        "candidate",
        "1",
        scorer,
        columns=columns,
    )
    assert rejected[0]["execution_status"] == "INPUT_REJECTED"
    assert rejected[0]["fraud_probability_estimate"] is None


def test_full_parity_and_negative_controls(
    shadow_replay: tuple[Path, RegistryConfig, Path, Path, Path, Path],
) -> None:
    root, registry, _, ledger, plan, execution = shadow_replay
    evidence = verify_parity(execution, plan, ledger, registry, root / "parity")
    manifest = json.loads((evidence / "manifest.json").read_text())
    comparison = manifest["comparison"]
    assert comparison["compared_event_count"] == 24
    assert comparison["mismatch_count"] == 0
    assert verify_parity(execution, plan, ledger, registry, root / "parity") == evidence
    results = cast(
        list[dict[str, object]], pq.read_table(execution / "results.parquet").to_pylist()
    )
    reference = cast(
        list[dict[str, object]], pq.read_table(ledger / "predictions.parquet").to_pylist()
    )
    candidate = cast(str, results[0]["candidate_id"])
    with pytest.raises(ShadowError, match="Probability parity"):
        compare_rows(
            [{**results[0], "fraud_probability_estimate": 0.0}, *results[1:]], reference, candidate
        )
    with pytest.raises(ShadowError, match="missing"):
        compare_rows(results[:-1], reference, candidate)
    with pytest.raises(ShadowError, match="unexpected"):
        compare_rows([*results, {**results[0], "event_id": "new-id"}], reference, candidate)
    with pytest.raises(ShadowError, match="Duplicate"):
        compare_rows([*results, results[0]], reference, candidate)
    with pytest.raises(ShadowError, match="Candidate/source"):
        compare_rows(
            [{**results[0], "source_feature_row_fingerprint": "bad"}, *results[1:]],
            reference,
            candidate,
        )
    with pytest.raises(ShadowError, match="exact-candidate"):
        compare_rows([{**results[0], "candidate_id": "bad"}, *results[1:]], reference, candidate)
    with pytest.raises(ShadowError, match="Invalid parity tolerance"):
        compare_rows(results, reference, candidate, relative_tolerance=-1)


def test_global_source_and_artifact_fail_closed(
    shadow_replay: tuple[Path, RegistryConfig, Path, Path, Path, Path], tmp_path: Path
) -> None:
    _, registry, source, ledger, plan, execution = shadow_replay
    other = tmp_path / "other-delta"
    write_deltalake(other, DeltaTable(source).to_pyarrow_table())
    with pytest.raises(ShadowError, match="lineage"):
        build_plan(registry, "1", other, 0, moment(3), moment(7), tmp_path / "plans")
    incomplete = tmp_path / "incomplete"
    incomplete.mkdir()
    with pytest.raises(ShadowError, match="Incomplete"):
        inspect_execution(incomplete, plan, registry)
    corrupted = tmp_path / execution.name
    shutil.copytree(execution, corrupted)
    with (corrupted / "results.parquet").open("ab") as stream:
        stream.write(b"corrupt")
    with pytest.raises(ShadowError, match="hash differs"):
        inspect_execution(corrupted, plan, registry)
    altered_ledger = tmp_path / ledger.name
    shutil.copytree(ledger, altered_ledger)
    with (altered_ledger / "predictions.parquet").open("ab") as stream:
        stream.write(b"corrupt")
    with pytest.raises(ValueError, match="hash differs"):
        verify_parity(execution, plan, altered_ledger, registry, tmp_path / "parity")


def test_wrong_positive_class_reference_is_rejected(
    shadow_replay: tuple[Path, RegistryConfig, Path, Path, Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sentinelaegisforge_control_plane.shadow.parity as parity_module

    _, registry, _, ledger, plan, execution = shadow_replay

    def wrong_class(path: Path) -> dict[str, object]:
        return {**inspect_ledger(path), "positive_class": {"label": 0, "meaning": "LEGIT"}}

    monkeypatch.setattr(parity_module, "inspect_ledger", wrong_class)
    with pytest.raises(ShadowError, match="positive-class"):
        verify_parity(execution, plan, ledger, registry, tmp_path / "wrong-class")
    assert not (tmp_path / "wrong-class").exists()


def test_changed_source_version_or_model_identity_changes_plan_or_execution_identity(
    shadow_replay: tuple[Path, RegistryConfig, Path, Path, Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sentinelaegisforge_control_plane.shadow.plan as plan_module
    from sentinelaegisforge_control_plane.shadow.contract import digest
    from sentinelaegisforge_control_plane.shadow.execution import _identity

    _, registry, source, _, plan, _ = shadow_replay
    manifest, _ = inspect_plan(plan, registry)
    assert _identity({**manifest, "candidate_id": "another-candidate"}, 5000) != _identity(
        manifest, 5000
    )
    assert _identity({**manifest, "source_delta_version": 99}, 5000) != _identity(manifest, 5000)
    assert digest({**manifest, "source_delta_version": 99}) != digest(manifest)
    original_source = plan_module._source

    def changed_source(
        *args: object,
    ) -> tuple[dict[str, object], list[dict[str, object]], tuple[str, ...]]:
        fields, rows, columns = original_source(registry, "1", source, 0)
        return {**fields, "source_delta_version": 99}, rows, columns

    monkeypatch.setattr(plan_module, "_source", changed_source)
    different, _, _ = plan_module._plan(registry, "1", source, 0, moment(3), moment(7), 5000)
    assert different["plan_id"] != manifest["plan_id"]


def test_model_loads_once_and_corrupt_global_model_never_publishes(
    shadow_replay: tuple[Path, RegistryConfig, Path, Path, Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sentinelaegisforge_control_plane.candidates.run as candidate_module
    import sentinelaegisforge_control_plane.shadow.execution as execution_module

    _, registry, _, _, plan, _ = shadow_replay
    calls = 0

    def counted(
        path: Path, *, tracking: TrackingConfig | None = None
    ) -> tuple[dict[str, object], Pipeline]:
        nonlocal calls
        calls += 1
        return load_verified_candidate(path, tracking=tracking)

    monkeypatch.setattr(execution_module, "load_verified_candidate", counted)
    output = execute_plan(plan, registry, tmp_path / "fresh-executions")
    assert output.is_dir() and calls == 1
    registration = inspect_registered_candidate(registry, "1")
    original = _source_path(cast(str, registration["source_uri"])).parent
    altered = tmp_path / original.name
    shutil.copytree(original, altered)
    with (altered / "model" / "model.skops").open("ab") as stream:
        stream.write(b"corrupt")
    monkeypatch.setattr(execution_module, "_source_path", lambda _uri: altered / "model")
    monkeypatch.setattr(
        candidate_module, "_load", lambda _path: pytest.fail("corrupt model loaded")
    )
    with pytest.raises(ValueError, match="hash differs"):
        execute_plan(plan, registry, tmp_path / "corrupt-executions")
    assert not (tmp_path / "corrupt-executions").exists()
