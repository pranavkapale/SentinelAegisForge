"""Phase 20 descriptive metrics and immutable, label-free Delta/ledger drift boundary."""

import inspect
import json
import math
import shutil
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from test_training import config, make_snapshot, moment

from sentinelaegisforge_control_plane.candidates.run import build_candidate
from sentinelaegisforge_control_plane.datasets.dataset_contract import SOURCE_SCHEMA
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.monitoring import drift
from sentinelaegisforge_control_plane.monitoring.contract import MonitoringError
from sentinelaegisforge_control_plane.monitoring.distributions import (
    categorical_comparison,
    ks_distance,
    numeric_comparison,
    numeric_summary,
    psi,
)
from sentinelaegisforge_control_plane.monitoring.drift import (
    build_drift_snapshot,
    compare_drift_snapshots,
    inspect_drift_snapshot,
)
from sentinelaegisforge_control_plane.monitoring.ledger import build_ledger
from sentinelaegisforge_control_plane.registry.run import RegistryConfig, register_candidate
from sentinelaegisforge_control_plane.tracking.run import TrackingConfig, track_run
from sentinelaegisforge_control_plane.training.run import train_baseline


@pytest.fixture(scope="module")
def replay(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, RegistryConfig, Path, Path]:
    root = tmp_path_factory.mktemp("drift-ledger")
    snapshot = make_snapshot(root)
    training = train_baseline(snapshot, config(), root / "runs")
    tracking = TrackingConfig(root / "tracking.db", root / "artifacts", "drift-test")
    tracked = track_run(training.path, tracking)
    candidate = build_candidate(
        training.path,
        root / "candidates",
        tracking_run_id=tracked["mlflow_run_id"],
        tracking=tracking,
    )
    registry = RegistryConfig(tracking, "drift-test-model", root / "receipts")
    register_candidate(candidate, registry)
    source = root / "transaction_statistical_features_v2"
    ledger = build_ledger(
        registry, "1", source, DeltaTable(source).version(), moment(3), moment(7), root / "ledgers"
    )
    return root, registry, source, ledger


def test_numeric_summary_type7_missingness_and_permutation() -> None:
    expected = numeric_summary([None, 1, 2, 3, 4, 5])
    assert expected == numeric_summary([5, 4, None, 1, 3, 2])
    assert expected["rows"] == 6
    assert expected["null"] == 1
    assert expected["missing_fraction"] == pytest.approx(1 / 6)
    assert expected["mean"] == expected["median"] == 3
    assert expected["p05"] == pytest.approx(1.2)
    assert expected["p95"] == pytest.approx(4.8)
    assert numeric_summary([None])["mean"] is None


@pytest.mark.parametrize(
    ("reference", "comparison", "expected"),
    [
        ([1, 2, 2, 3], [2, 3, 1, 2], 0.0),
        ([1, 1, 2], [8, 9], 1.0),
        ([1, 2], [1, 1, 1, 2], 0.25),
        ([3, 3, 3], [3, 3], 0.0),
    ],
)
def test_ks_empirical_cdf_with_ties_and_unequal_sizes(
    reference: list[int], comparison: list[int], expected: float
) -> None:
    assert ks_distance(cast(list[object], reference), cast(list[object], comparison))[
        "value"
    ] == pytest.approx(expected)


def test_numeric_empty_nonfinite_and_support() -> None:
    assert ks_distance([None], [1])["reason"] == "EMPTY_NON_NULL_COHORT"
    assert psi([1], [None])["reason"] == "EMPTY_NON_NULL_COHORT"
    with pytest.raises(MonitoringError, match="nonfinite"):
        numeric_summary([math.inf])
    with pytest.raises(MonitoringError, match="nonfinite"):
        psi([1, 2], [math.nan])
    compared = numeric_comparison(
        [None, 1], [2, 3], minimum_rows=3, minimum_non_null=1, bins=2, smoothing=0.5
    )
    assert compared["missing_fraction_difference"] == -0.5
    assert cast(dict[str, object], compared["ks_d"])["reason"] == "INSUFFICIENT_COHORT_ROWS"


def test_psi_reference_bins_degenerate_and_sparse_smoothing() -> None:
    assert psi([7, 7, 7], [7, 8])["reason"] == "DEGENERATE_REFERENCE"
    assert psi([7, 7, 7], [7, 7])["value"] is None
    a = psi([0, 0, 0, 0, 10, 10, 10, 10], [0, 0, 10, 10], bins=4)
    b = psi([10, 0, 10, 0, 0, 10, 0, 10], [10, 0, 10, 0], bins=4)
    assert a == b
    assert a["boundaries"] == [5.0]
    assert a["effective_bin_count"] == 2
    assert a["reference_counts"] == [4, 4]
    sparse = psi([0, 0, 10, 10], [20, 20, 20, 20], bins=2, smoothing=0.5)
    assert sparse["status"] == "AVAILABLE"
    assert cast(float, sparse["value"]) > 0
    assert math.isfinite(cast(float, sparse["value"]))
    assert psi([0, 1, 2, 3], [0, 1, 2, 3], bins=2)["value"] == pytest.approx(0)


def test_categorical_tvd_js_new_disappeared_and_null_policy() -> None:
    same = categorical_comparison(["USD", "EUR"], ["EUR", "USD"], minimum_rows=1)
    assert same["tvd"] == same["js_divergence_base2"] == 0
    disjoint = categorical_comparison(["USD", "USD"], ["EUR", "EUR"], minimum_rows=1)
    assert disjoint["tvd"] == disjoint["js_divergence_base2"] == 1
    assert disjoint["new_categories"] == [{"name": "EUR", "count": 2, "share": 1}]
    assert disjoint["disappeared_categories"] == ["USD"]
    altered = categorical_comparison(["USD", "EUR"], ["USD"] * 3, minimum_rows=1)
    reversed_result = categorical_comparison(["USD"] * 3, ["EUR", "USD"], minimum_rows=1)
    assert altered["tvd"] == pytest.approx(0.5)
    assert altered["js_divergence_base2"] == pytest.approx(reversed_result["js_divergence_base2"])
    missing = categorical_comparison([None, "USD"], [None, None], minimum_rows=1)
    assert missing["comparison_counts"] == {"<MISSING>": 2, "USD": 0}
    assert categorical_comparison(["USD"], ["EUR"], minimum_rows=2)["tvd"] is None


@pytest.fixture
def fixture_snapshot(replay: tuple[Path, RegistryConfig, Path, Path], tmp_path: Path) -> Path:
    return build_drift_snapshot(
        replay[3],
        moment(3),
        moment(5),
        moment(5),
        moment(7),
        tmp_path / "drift",
        minimum_rows=2,
        minimum_non_null=2,
    )


def test_fixed_ledger_delta_cohorts_and_full_feature_contract(
    fixture_snapshot: Path,
) -> None:
    manifest = inspect_drift_snapshot(fixture_snapshot)
    metrics = cast(
        dict[str, object], json.loads((fixture_snapshot / "drift_metrics.json").read_text())
    )
    assert manifest["reference_rows"] == manifest["comparison_rows"] == 12
    assert set(cast(dict[str, object], metrics["numeric"])) == set(drift.NUMERIC)
    assert set(cast(dict[str, object], metrics["categorical"])) == set(drift.CATEGORICAL)
    assert cast(dict[str, object], metrics["prediction"])["reference"] is not None
    currency = cast(dict[str, dict[str, object]], metrics["categorical"])["currency"]
    assert currency["tvd"] == 1
    assert currency["new_categories"] == [{"name": "JPY", "count": 12, "share": 1.0}]
    assert cast(dict[str, object], metrics["currency_strata"])["EUR"] is not None
    assert "FRAUD" not in (fixture_snapshot / "drift_report.md").read_text()


def test_windows_utc_overlap_outside_and_half_open(
    replay: tuple[Path, RegistryConfig, Path, Path], tmp_path: Path
) -> None:
    ledger = replay[3]
    for windows in (
        (moment(3), moment(6), moment(5), moment(7)),
        (moment(2), moment(5), moment(5), moment(7)),
        (moment(3), moment(5), moment(5), moment(8)),
        (moment(5), moment(3), moment(5), moment(7)),
    ):
        with pytest.raises(MonitoringError, match="windows"):
            build_drift_snapshot(ledger, *windows, tmp_path / "invalid")
    with pytest.raises(ValueError, match="UTC"):
        build_drift_snapshot(ledger, datetime(2030, 1, 3), moment(5), moment(5), moment(7))
    with pytest.raises(MonitoringError, match="at least one"):
        build_drift_snapshot(
            ledger, moment(3), moment(3, 1), moment(6), moment(7), tmp_path / "empty"
        )


def test_repeat_identity_changed_boundaries_configuration_and_compare(
    replay: tuple[Path, RegistryConfig, Path, Path], fixture_snapshot: Path, tmp_path: Path
) -> None:
    contents = {name: (fixture_snapshot / name).read_bytes() for name in drift.DRIFT_FILES}
    assert (
        build_drift_snapshot(
            replay[3],
            moment(3),
            moment(5),
            moment(5),
            moment(7),
            fixture_snapshot.parent,
            minimum_rows=2,
            minimum_non_null=2,
        )
        == fixture_snapshot
    )
    assert contents == {name: (fixture_snapshot / name).read_bytes() for name in drift.DRIFT_FILES}
    changed = build_drift_snapshot(
        replay[3],
        moment(3),
        moment(5),
        moment(5),
        moment(7),
        tmp_path / "changed",
        minimum_rows=3,
        minimum_non_null=2,
    )
    assert changed.name != fixture_snapshot.name
    assert compare_drift_snapshots(fixture_snapshot, changed)["warnings"] == [
        "metric_configuration"
    ]
    shifted = build_drift_snapshot(
        replay[3],
        moment(3),
        moment(5, 1),
        moment(5, 1),
        moment(7),
        tmp_path / "shifted",
        minimum_rows=2,
        minimum_non_null=2,
    )
    assert shifted.name != fixture_snapshot.name
    assert "reference_population" in cast(
        list[str], compare_drift_snapshots(fixture_snapshot, shifted)["warnings"]
    )


def test_corrupt_incomplete_and_source_fingerprint_fail_closed(
    replay: tuple[Path, RegistryConfig, Path, Path], fixture_snapshot: Path, tmp_path: Path
) -> None:
    empty = tmp_path / "incomplete"
    empty.mkdir()
    with pytest.raises(MonitoringError, match="Incomplete"):
        inspect_drift_snapshot(empty)
    copied = tmp_path / fixture_snapshot.name
    shutil.copytree(fixture_snapshot, copied)
    (copied / "drift_metrics.json").write_text("{}")
    with pytest.raises(MonitoringError, match="hash differs"):
        inspect_drift_snapshot(copied)
    (copied / "drift_metrics.json").write_bytes(
        (fixture_snapshot / "drift_metrics.json").read_bytes()
    )
    (copied / "drift_report.md").write_text("changed report")
    with pytest.raises(MonitoringError, match="hash differs"):
        inspect_drift_snapshot(copied)
    (copied / "drift_report.md").write_bytes((fixture_snapshot / "drift_report.md").read_bytes())
    (copied / "drift_manifest.json").write_text("{}")
    with pytest.raises(MonitoringError):
        inspect_drift_snapshot(copied)
    forged = cast(
        dict[str, object], json.loads((fixture_snapshot / "drift_manifest.json").read_text())
    )
    forged["feature_columns"] = ["currency"]
    (copied / "drift_manifest.json").write_bytes(canonical_json(forged))
    with pytest.raises(MonitoringError, match="source identity differs"):
        inspect_drift_snapshot(copied)
    copied_ledger = tmp_path / "ledger" / replay[3].name
    copied_ledger.parent.mkdir()
    shutil.copytree(replay[3], copied_ledger)
    manifest = cast(dict[str, object], json.loads((copied_ledger / "manifest.json").read_text()))
    manifest["scored_input_fingerprint"] = "0" * 64
    (copied_ledger / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(MonitoringError):
        build_drift_snapshot(
            copied_ledger,
            moment(3),
            moment(5),
            moment(5),
            moment(7),
            tmp_path / "source-fail",
            minimum_rows=2,
        )


def test_no_model_scoring_or_labels_in_drift_builder(
    replay: tuple[Path, RegistryConfig, Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sentinelaegisforge_control_plane.monitoring.ledger as ledger_module
    import sentinelaegisforge_control_plane.monitoring.outcomes as outcomes_module

    monkeypatch.setattr(ledger_module, "score_candidate", lambda *_a, **_k: pytest.fail("rescored"))
    monkeypatch.setattr(
        outcomes_module, "resolve_labels", lambda *_a, **_k: pytest.fail("labels read")
    )
    assert not {"labels", "corpus", "model", "registry"} & set(
        inspect.signature(build_drift_snapshot).parameters
    )
    assert build_drift_snapshot(
        replay[3],
        moment(3),
        moment(5),
        moment(5),
        moment(7),
        tmp_path / "no-labels",
        minimum_rows=2,
    ).is_dir()


@pytest.mark.parametrize(
    "mutation", ["table_id", "schema", "missing_row", "changed_amount", "changed_currency"]
)
def test_drift_source_reconciliation_rejects_mutation(
    replay: tuple[Path, RegistryConfig, Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    snapshot = StatisticalFeatureSource(replay[2], 0).read()
    if mutation == "table_id":
        altered = replace(snapshot, delta_table_id="wrong-table")
    elif mutation == "schema":
        altered = replace(snapshot, source_schema_fingerprint="wrong-schema")
    else:
        rows = snapshot.table.to_pylist()
        if mutation == "missing_row":
            del rows[25]
        elif mutation == "changed_amount":
            rows[25]["amount"] += 1
        else:
            rows[25]["currency"] = "GBP"
        altered = replace(snapshot, table=pa.Table.from_pylist(rows, schema=SOURCE_SCHEMA))
    monkeypatch.setattr(
        drift, "StatisticalFeatureSource", lambda *_a, **_k: SimpleNamespace(read=lambda: altered)
    )
    with pytest.raises(MonitoringError, match="source|Source|fingerprint|segment"):
        build_drift_snapshot(
            replay[3],
            moment(3),
            moment(5),
            moment(5),
            moment(7),
            tmp_path / "bad-source",
            minimum_rows=2,
        )
