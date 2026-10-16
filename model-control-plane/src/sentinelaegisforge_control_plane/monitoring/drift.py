"""Immutable, label-free distribution snapshots over one verified prediction ledger."""

import json
import math
from datetime import datetime
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.candidates.contract import (
    FEATURES,
    INPUT_FINGERPRINT,
    validate_input,
)
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.labels import (
    parse_utc_timestamp,
    require_utc,
    utc_text,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

from .contract import (
    MonitoringError,
    artifact_directory,
    identity,
    object_value,
    row_fingerprint,
    sha256,
)
from .distributions import (
    ALGORITHM_VERSION,
    categorical_comparison,
    numeric_comparison,
)
from .ledger import ROOT, inspect_ledger

DRIFT_VERSION = "offline-distribution-monitoring-v1"
REPORT_VERSION = "offline-distribution-report-v2"
DEFAULT_DRIFT_ROOT = ROOT / ".local/ml/drift-snapshots"
DRIFT_FILES = {"drift_manifest.json", "drift_metrics.json", "drift_report.md"}
CATEGORICAL = ("currency", "country", "transaction_type", "statistical_feature_status")
NUMERIC = tuple(name for name in FEATURES if name not in CATEGORICAL)
MONETARY = (
    "amount",
    "prior_amount_sum_10m",
    "prior_amount_mean",
    "prior_amount_stddev",
    "amount_zscore",
)
PREDICTION = "fraud_probability_estimate"
IDENTITY_FIELDS = (
    "drift_contract_version",
    "algorithm_version",
    "ledger_id",
    "ledger_manifest_sha256",
    "ledger_predictions_sha256",
    "candidate_id",
    "input_contract_fingerprint",
    "source_delta_table_id",
    "source_delta_version",
    "source_schema_fingerprint",
    "source_input_fingerprint",
    "reference_window",
    "comparison_window",
    "reference_membership_fingerprint",
    "comparison_membership_fingerprint",
    "metric_configuration",
    "report_contract_version",
)


def _configuration(
    minimum_rows: int, minimum_non_null: int, bins: int, smoothing: float
) -> dict[str, object]:
    if (
        type(minimum_rows) is not int
        or minimum_rows < 1
        or type(minimum_non_null) is not int
        or minimum_non_null < 1
        or type(bins) is not int
        or bins < 2
        or type(smoothing) is not float
        or not math.isfinite(smoothing)
        or smoothing <= 0
    ):
        raise MonitoringError("Invalid distribution support/bin/smoothing configuration")
    return {
        "minimum_rows": minimum_rows,
        "minimum_non_null": minimum_non_null,
        "target_psi_bins": bins,
        "psi_pseudocount_per_bin": smoothing,
    }


def _windows(
    ledger: dict[str, object],
    reference_start: datetime,
    reference_end: datetime,
    comparison_start: datetime,
    comparison_end: datetime,
) -> tuple[datetime, datetime, datetime, datetime]:
    a, b, c, d = map(
        require_utc, (reference_start, reference_end, comparison_start, comparison_end)
    )
    ledger_start = parse_utc_timestamp(cast(str, ledger["score_start"]))
    ledger_end = parse_utc_timestamp(cast(str, ledger["score_end"]))
    if not (ledger_start <= a < b <= c < d <= ledger_end):
        raise MonitoringError("Drift windows must be ordered, disjoint and inside the ledger")
    return a, b, c, d


def _cohort(
    predictions: list[dict[str, object]],
    source_by_id: dict[str, dict[str, object]],
    start: datetime,
    end: datetime,
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    for prediction in predictions:
        when = cast(datetime, prediction["ingestion_time"])
        if not start <= when < end:
            continue
        event_id = cast(str, prediction["event_id"])
        row = source_by_id.get(event_id)
        if row is None or row["ingestion_time"] != when:
            raise MonitoringError(f"Missing or mismatched source row: {event_id}")
        # Recheck the entire selected source row; the ledger inspector also checks all rows.
        if prediction["source_feature_row_fingerprint"] != row_fingerprint(row, tuple(row)):
            raise MonitoringError(f"Source row fingerprint differs: {event_id}")
        if any(
            prediction[name] != row[name] for name in ("currency", "country", "transaction_type")
        ):
            raise MonitoringError(f"Ledger segment and feature source differ: {event_id}")
        validate_input({name: row[name] for name in FEATURES})
        selected.append(
            {
                **{name: row[name] for name in FEATURES},
                "event_id": event_id,
                "ingestion_time": when,
                PREDICTION: prediction[PREDICTION],
                "source_feature_row_fingerprint": prediction["source_feature_row_fingerprint"],
            }
        )
    ids = [cast(str, row["event_id"]) for row in selected]
    if len(ids) != len(set(ids)):
        raise MonitoringError("Duplicate event ID in drift cohort")
    return selected


def _values(rows: list[dict[str, object]], name: str) -> list[object]:
    return [row[name] for row in rows]


def _membership(rows: list[dict[str, object]]) -> str:
    return identity([[row["event_id"], row["source_feature_row_fingerprint"]] for row in rows])


def _metrics(
    reference: list[dict[str, object]],
    comparison: list[dict[str, object]],
    configuration: dict[str, object],
) -> dict[str, object]:
    minimum_rows = cast(int, configuration["minimum_rows"])
    minimum_non_null = cast(int, configuration["minimum_non_null"])
    bins = cast(int, configuration["target_psi_bins"])
    smoothing = cast(float, configuration["psi_pseudocount_per_bin"])
    numeric = {
        name: numeric_comparison(
            _values(reference, name),
            _values(comparison, name),
            minimum_rows=minimum_rows,
            minimum_non_null=minimum_non_null,
            bins=bins,
            smoothing=smoothing,
        )
        for name in NUMERIC
    }
    categorical = {
        name: categorical_comparison(
            cast(list[str | None], _values(reference, name)),
            cast(list[str | None], _values(comparison, name)),
            minimum_rows=minimum_rows,
        )
        for name in CATEGORICAL
    }
    probability = numeric_comparison(
        _values(reference, PREDICTION),
        _values(comparison, PREDICTION),
        minimum_rows=minimum_rows,
        minimum_non_null=minimum_non_null,
        bins=bins,
        smoothing=smoothing,
    )
    probability["fraction_at_or_above_0_5"] = {
        "reference": sum(cast(float, row[PREDICTION]) >= 0.5 for row in reference) / len(reference),
        "comparison": sum(cast(float, row[PREDICTION]) >= 0.5 for row in comparison)
        / len(comparison),
        "meaning": "Descriptive fixed-score threshold rate, not observed fraud prevalence",
    }
    currencies = sorted({cast(str, row["currency"]) for row in reference + comparison})
    stratified: dict[str, object] = {}
    for currency in currencies:
        left = [row for row in reference if row["currency"] == currency]
        right = [row for row in comparison if row["currency"] == currency]
        stratified[currency] = {
            "reference_rows": len(left),
            "comparison_rows": len(right),
            "features": {
                name: numeric_comparison(
                    _values(left, name),
                    _values(right, name),
                    minimum_rows=minimum_rows,
                    minimum_non_null=minimum_non_null,
                    bins=bins,
                    smoothing=smoothing,
                )
                for name in MONETARY
            },
        }
    return {
        "reference_rows": len(reference),
        "comparison_rows": len(comparison),
        "reference_observed_ingestion_range": [
            utc_text(cast(datetime, reference[0]["ingestion_time"])),
            utc_text(cast(datetime, reference[-1]["ingestion_time"])),
        ],
        "comparison_observed_ingestion_range": [
            utc_text(cast(datetime, comparison[0]["ingestion_time"])),
            utc_text(cast(datetime, comparison[-1]["ingestion_time"])),
        ],
        "numeric": numeric,
        "categorical": categorical,
        "prediction": probability,
        "currency_strata": stratified,
        "scope": (
            "Descriptive synthetic offline replay; no alert, causal or model-quality conclusion"
        ),
    }


def _report(manifest: dict[str, object], metrics: dict[str, object]) -> bytes:
    lines = [
        "# Offline feature and frozen-prediction distribution comparison",
        "",
        f"Ledger `{manifest['ledger_id']}`; candidate `{manifest['candidate_id']}`; "
        f"Registry `{manifest['registered_model_name']}` v{manifest['registered_model_version']}; "
        f"Delta `{manifest['source_delta_table_id']}` version {manifest['source_delta_version']}.",
        f"Reference `{manifest['reference_window']}`: {metrics['reference_rows']} rows; "
        f"comparison `{manifest['comparison_window']}`: {metrics['comparison_rows']} rows.",
        "",
        "| Numeric distribution | Reference mean | Comparison mean | Missing Δ | KS D | PSI |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    numeric = cast(dict[str, dict[str, object]], metrics["numeric"])
    for name in (*NUMERIC, PREDICTION):
        item = cast(
            dict[str, object], metrics["prediction"] if name == PREDICTION else numeric[name]
        )
        left = cast(dict[str, object], item["reference"])
        right = cast(dict[str, object], item["comparison"])
        ks = cast(dict[str, object], item["ks_d"])
        psi = cast(dict[str, object], item["psi"])
        lines.append(
            f"| {name} | {left['mean']} | {right['mean']} | "
            f"{item['missing_fraction_difference']} | "
            f"{ks['value'] if ks['value'] is not None else ks['reason']} | "
            f"{psi['value'] if psi['value'] is not None else psi['reason']} |"
        )
    lines.extend(
        [
            "",
            "| Category | TVD | JS divergence (base 2) | New | Disappeared |",
            "| --- | ---: | ---: | --- | --- |",
        ]
    )
    categories = cast(dict[str, dict[str, object]], metrics["categorical"])
    for name in CATEGORICAL:
        item = categories[name]
        lines.append(
            f"| {name} | {item['tvd']} | {item['js_divergence_base2']} | "
            f"{json.dumps(item['new_categories'], sort_keys=True)} | "
            f"{item['disappeared_categories']} |"
        )
    lines.extend(["", "Categorical composition:"])
    for name in CATEGORICAL:
        item = categories[name]
        lines.append(
            f"- {name} counts: reference {item['reference_counts']}; "
            f"comparison {item['comparison_counts']}."
        )
    prediction = cast(dict[str, object], metrics["prediction"])
    score_a = cast(dict[str, object], prediction["reference"])
    score_b = cast(dict[str, object], prediction["comparison"])
    rate = cast(dict[str, object], prediction["fraction_at_or_above_0_5"])
    lines.extend(
        [
            "",
            "Frozen score summary (reference → comparison): "
            f"min {score_a['minimum']} → {score_b['minimum']}; "
            f"median {score_a['median']} → {score_b['median']}; "
            f"p05 {score_a['p05']} → {score_b['p05']}; "
            f"p95 {score_a['p95']} → {score_b['p95']}; "
            f"max {score_a['maximum']} → {score_b['maximum']}; "
            f"score ≥ 0.5 share {rate['reference']} → {rate['comparison']}.",
            "",
            "Nullable statistical-feature missing fractions (reference → comparison):",
        ]
    )
    for name in ("prior_amount_mean", "prior_amount_stddev", "amount_zscore"):
        item = numeric[name]
        lines.append(
            f"- {name}: {cast(dict[str, object], item['reference'])['missing_fraction']} → "
            f"{cast(dict[str, object], item['comparison'])['missing_fraction']} "
            f"(Δ {item['missing_fraction_difference']})."
        )
    lines.extend(["", "Same-currency amount magnitudes (reference → comparison):"])
    for currency, group in cast(dict[str, dict[str, object]], metrics["currency_strata"]).items():
        amount = cast(dict[str, dict[str, object]], group["features"])["amount"]
        lines.append(
            f"- {currency}: {group['reference_rows']} → {group['comparison_rows']} rows; "
            f"mean amount {cast(dict[str, object], amount['reference'])['mean']} → "
            f"{cast(dict[str, object], amount['comparison'])['mean']}; "
            f"KS {cast(dict[str, object], amount['ks_d'])['value']}."
        )
    lines.extend(
        [
            "",
            "All 11 candidate inputs and the frozen prediction distribution are measured. "
            "Exact decimal source values are unchanged; distribution magnitudes use finite "
            "float conversion.",
            "Numeric p05/median/p95 use (n−1) linear interpolation. KS is a descriptive "
            "empirical-CDF D statistic without a p-value. PSI uses reference-fitted quantile "
            "boundaries and a fixed per-bin pseudocount; unavailable metrics carry reasons. "
            "Null statistical values remain missing.",
            "Raw amount distributions across currencies are not currency-normalized financial "
            "totals; see the currency-specific diagnostics in drift_metrics.json. "
            "No FX conversion is made.",
            "A new category is descriptive; the existing one-hot encoder ignores unknowns. "
            "A threshold-0.5 score rate is not observed fraud prevalence.",
            "This is an earlier post-training synthetic historical-replay reference, not an "
            "established production baseline. Distribution differences do not prove "
            "model-quality degradation or "
            "concept drift. No alert, retraining, promotion or deployment action follows.",
            "",
        ]
    )
    return "\n".join(lines).encode("utf-8")


def build_drift_snapshot(
    ledger_path: Path,
    reference_start: datetime,
    reference_end: datetime,
    comparison_start: datetime,
    comparison_end: datetime,
    output_root: Path = DEFAULT_DRIFT_ROOT,
    *,
    minimum_rows: int = 30,
    minimum_non_null: int = 5,
    bins: int = 10,
    smoothing: float = 0.5,
) -> Path:
    """Measure two disjoint populations; never load a model or outcome label."""
    configuration = _configuration(minimum_rows, minimum_non_null, bins, smoothing)
    ledger_path = ledger_path.resolve()
    ledger = inspect_ledger(ledger_path)
    a, b, c, d = _windows(ledger, reference_start, reference_end, comparison_start, comparison_end)
    source = StatisticalFeatureSource(
        Path(cast(str, ledger["source_delta_path"])), cast(int, ledger["source_delta_version"])
    ).read()
    if (
        source.delta_table_id != ledger["source_delta_table_id"]
        or source.delta_version != ledger["source_delta_version"]
        or source.source_schema_fingerprint != ledger["source_schema_fingerprint"]
        or ledger["input_contract_fingerprint"] != INPUT_FINGERPRINT
    ):
        raise MonitoringError("Pinned source or candidate input contract differs")
    source_rows = cast(list[dict[str, object]], source.table.to_pylist())
    source_by_id = {cast(str, row["event_id"]): row for row in source_rows}
    if len(source_by_id) != len(source_rows):
        raise MonitoringError("Duplicate source event identity")
    predictions = cast(
        list[dict[str, object]], pq.read_table(ledger_path / "predictions.parquet").to_pylist()
    )
    reference = _cohort(predictions, source_by_id, a, b)
    comparison = _cohort(predictions, source_by_id, c, d)
    if not reference or not comparison:
        raise MonitoringError("Both drift cohorts require at least one ledger event")
    basis: dict[str, object] = {
        "drift_contract_version": DRIFT_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "ledger_id": ledger["ledger_id"],
        "ledger_manifest_sha256": sha256((ledger_path / "manifest.json").read_bytes()),
        "ledger_predictions_sha256": ledger["predictions_parquet_sha256"],
        "candidate_id": ledger["candidate_id"],
        "input_contract_fingerprint": INPUT_FINGERPRINT,
        "source_delta_table_id": source.delta_table_id,
        "source_delta_version": source.delta_version,
        "source_schema_fingerprint": source.source_schema_fingerprint,
        "source_input_fingerprint": ledger["scored_input_fingerprint"],
        "reference_window": [utc_text(a), utc_text(b)],
        "comparison_window": [utc_text(c), utc_text(d)],
        "reference_membership_fingerprint": _membership(reference),
        "comparison_membership_fingerprint": _membership(comparison),
        "metric_configuration": configuration,
        "report_contract_version": REPORT_VERSION,
    }
    metrics = _metrics(reference, comparison, configuration)
    snapshot_id = identity(basis)
    contents = {"drift_metrics.json": canonical_json(metrics)}
    manifest: dict[str, object] = {
        **basis,
        "drift_snapshot_id": snapshot_id,
        "ledger_path": str(ledger_path),
        "registered_model_name": ledger["registered_model_name"],
        "registered_model_version": ledger["registered_model_version"],
        "feature_columns": list(FEATURES),
        "prediction_column": PREDICTION,
        "reference_rows": len(reference),
        "comparison_rows": len(comparison),
        "metrics_fingerprint": identity(metrics),
        "scope": "Synthetic, label-free offline distribution measurement; no automatic action",
    }
    contents["drift_report.md"] = _report(manifest, metrics)
    manifest["artifact_sha256"] = {name: sha256(content) for name, content in contents.items()}
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / snapshot_id
    if target.exists():
        existing = inspect_drift_snapshot(target)
        if existing != manifest:
            raise MonitoringError("Existing drift snapshot conflicts with current evidence")
        if any((target / name).read_bytes() != content for name, content in contents.items()):
            raise MonitoringError("Existing drift artifact content differs")
        return target
    target.mkdir()  # Exclusive claim; incomplete results remain visible and invalid.
    for name, content in contents.items():
        with (target / name).open("xb") as stream:
            stream.write(content)
    with (target / "drift_manifest.json").open("xb") as stream:
        stream.write(canonical_json(manifest))
    inspect_drift_snapshot(target)
    return target


def inspect_drift_snapshot(path: Path) -> dict[str, object]:
    artifact_directory(path, DRIFT_FILES)
    try:
        raw = (path / "drift_manifest.json").read_bytes()
        manifest = object_value(json.loads(raw), "drift manifest")
        if raw != canonical_json(manifest) or manifest["drift_contract_version"] != DRIFT_VERSION:
            raise MonitoringError("Noncanonical or unsupported drift manifest")
        if (
            manifest["algorithm_version"] != ALGORITHM_VERSION
            or manifest["report_contract_version"] != REPORT_VERSION
            or manifest["drift_snapshot_id"] != path.name
        ):
            raise MonitoringError("Drift algorithm or snapshot identity differs")
        if identity({key: manifest[key] for key in IDENTITY_FIELDS}) != path.name:
            raise MonitoringError("Drift snapshot identity differs")
        hashes = object_value(manifest["artifact_sha256"], "drift artifact hashes")
        if set(hashes) != DRIFT_FILES - {"drift_manifest.json"}:
            raise MonitoringError("Drift artifact inventory differs")
        for name, expected in hashes.items():
            if sha256((path / name).read_bytes()) != expected:
                raise MonitoringError(f"Drift artifact hash differs: {name}")
        metrics_raw = (path / "drift_metrics.json").read_bytes()
        metrics = object_value(json.loads(metrics_raw), "drift metrics")
        if (
            metrics_raw != canonical_json(metrics)
            or identity(metrics) != manifest["metrics_fingerprint"]
        ):
            raise MonitoringError("Drift logical metrics differ")
        if (
            metrics["reference_rows"] != manifest["reference_rows"]
            or metrics["comparison_rows"] != manifest["comparison_rows"]
        ):
            raise MonitoringError("Drift cohort counts differ")
        if (path / "drift_report.md").read_bytes() != _report(manifest, metrics):
            raise MonitoringError("Drift report differs from measured metrics")
        ledger_path = Path(cast(str, manifest["ledger_path"]))
        ledger = inspect_ledger(ledger_path)
        if (
            ledger["ledger_id"] != manifest["ledger_id"]
            or manifest["registered_model_name"] != ledger["registered_model_name"]
            or manifest["registered_model_version"] != ledger["registered_model_version"]
            or manifest["feature_columns"] != list(FEATURES)
            or manifest["prediction_column"] != PREDICTION
            or manifest["input_contract_fingerprint"] != INPUT_FINGERPRINT
            or sha256((ledger_path / "manifest.json").read_bytes())
            != manifest["ledger_manifest_sha256"]
            or ledger["predictions_parquet_sha256"] != manifest["ledger_predictions_sha256"]
            or ledger["source_delta_table_id"] != manifest["source_delta_table_id"]
            or ledger["source_delta_version"] != manifest["source_delta_version"]
            or ledger["source_schema_fingerprint"] != manifest["source_schema_fingerprint"]
            or ledger["scored_input_fingerprint"] != manifest["source_input_fingerprint"]
            or ledger["candidate_id"] != manifest["candidate_id"]
            or ledger["input_contract_fingerprint"] != INPUT_FINGERPRINT
        ):
            raise MonitoringError("Drift source identity differs")
        bounds = manifest["reference_window"], manifest["comparison_window"]
        if any(not isinstance(window, list) or len(window) != 2 for window in bounds):
            raise MonitoringError("Drift window representation differs")
        reference_window = cast(list[str], bounds[0])
        comparison_window = cast(list[str], bounds[1])
        _windows(
            ledger,
            parse_utc_timestamp(reference_window[0]),
            parse_utc_timestamp(reference_window[1]),
            parse_utc_timestamp(comparison_window[0]),
            parse_utc_timestamp(comparison_window[1]),
        )
        return manifest
    except MonitoringError:
        raise
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise MonitoringError(f"Cannot inspect drift snapshot: {error}") from error


def compare_drift_snapshots(first: Path, second: Path) -> dict[str, object]:
    a, b = inspect_drift_snapshot(first), inspect_drift_snapshot(second)
    differences = {
        "candidate_identity": a["candidate_id"] != b["candidate_id"],
        "reference_population": (
            a["reference_window"] != b["reference_window"]
            or a["reference_membership_fingerprint"] != b["reference_membership_fingerprint"]
        ),
        "algorithm_version": a["algorithm_version"] != b["algorithm_version"],
        "feature_contract": a["input_contract_fingerprint"] != b["input_contract_fingerprint"],
        "metric_configuration": a["metric_configuration"] != b["metric_configuration"],
        "delta_source": (
            a["source_delta_table_id"] != b["source_delta_table_id"]
            or a["source_delta_version"] != b["source_delta_version"]
        ),
    }
    return {
        "first_snapshot_id": a["drift_snapshot_id"],
        "second_snapshot_id": b["drift_snapshot_id"],
        "warnings": [name for name, changed in differences.items() if changed],
        "comparison_window_changed": a["comparison_window"] != b["comparison_window"],
        "metric_configuration_changed": a["metric_configuration"] != b["metric_configuration"],
        "first_metrics": str(first.resolve() / "drift_metrics.json"),
        "second_metrics": str(second.resolve() / "drift_metrics.json"),
        "scope": "Read-only descriptive snapshot comparison; no ranking or action",
    }
