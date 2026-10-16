"""Pure, deterministic descriptive two-cohort distribution calculations."""

import bisect
import math
import statistics
from collections import Counter
from decimal import Decimal
from typing import cast

from .contract import MonitoringError

ALGORITHM_VERSION = "offline-distribution-metrics-v2"
MISSING_CATEGORY = "<MISSING>"


def _finite(values: list[object]) -> list[float]:
    result: list[float] = []
    for value in values:
        if value is None:
            continue
        if type(value) not in (int, float, Decimal) or isinstance(value, bool):
            raise MonitoringError(f"Numeric distribution contains nonnumeric value: {value!r}")
        number = float(cast(int | float | Decimal, value))
        if not math.isfinite(number):
            raise MonitoringError("Numeric distribution contains nonfinite value")
        result.append(number)
    return sorted(result)


def _percentile(sorted_values: list[float], fraction: float) -> float:
    """Type-7 linear interpolation at (n-1)*fraction on finite sorted values."""
    position = (len(sorted_values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (position - lower)


def numeric_summary(values: list[object]) -> dict[str, object]:
    ordered = _finite(values)
    count = len(values)
    present = len(ordered)
    return {
        "rows": count,
        "non_null": present,
        "null": count - present,
        "missing_fraction": (count - present) / count if count else None,
        "minimum": ordered[0] if ordered else None,
        "maximum": ordered[-1] if ordered else None,
        "mean": statistics.fmean(ordered) if ordered else None,
        "median": _percentile(ordered, 0.5) if ordered else None,
        "p05": _percentile(ordered, 0.05) if ordered else None,
        "p95": _percentile(ordered, 0.95) if ordered else None,
    }


def ks_distance(reference: list[object], comparison: list[object]) -> dict[str, object]:
    left, right = _finite(reference), _finite(comparison)
    if not left or not right:
        return {"status": "UNAVAILABLE", "reason": "EMPTY_NON_NULL_COHORT", "value": None}
    i = j = 0
    distance = 0.0
    for value in sorted(set(left) | set(right)):
        while i < len(left) and left[i] <= value:
            i += 1
        while j < len(right) and right[j] <= value:
            j += 1
        distance = max(distance, abs(i / len(left) - j / len(right)))
    return {"status": "AVAILABLE", "reason": None, "value": distance}


def psi(
    reference: list[object], comparison: list[object], *, bins: int = 10, smoothing: float = 0.5
) -> dict[str, object]:
    if type(bins) is not int or bins < 2 or not math.isfinite(smoothing) or smoothing <= 0:
        raise MonitoringError("PSI requires at least two bins and positive finite smoothing")
    left, right = _finite(reference), _finite(comparison)
    base: dict[str, object] = {
        "target_bins": bins,
        "smoothing_pseudocount_per_bin": smoothing,
        "boundaries": [],
        "effective_bin_count": 0,
        "reference_counts": [],
        "comparison_counts": [],
    }
    if not left or not right:
        return {**base, "status": "UNAVAILABLE", "reason": "EMPTY_NON_NULL_COHORT", "value": None}
    boundaries = sorted(
        edge
        for edge in {_percentile(left, index / bins) for index in range(1, bins)}
        if left[0] < edge < left[-1]
    )
    if len(set(left)) < 2:
        return {**base, "status": "UNAVAILABLE", "reason": "DEGENERATE_REFERENCE", "value": None}
    if not boundaries:
        return {
            **base,
            "status": "UNAVAILABLE",
            "reason": "INSUFFICIENT_EFFECTIVE_BINS",
            "value": None,
        }
    counts_left = [0] * (len(boundaries) + 1)
    counts_right = [0] * (len(boundaries) + 1)
    for value in left:
        counts_left[bisect.bisect_right(boundaries, value)] += 1
    for value in right:
        counts_right[bisect.bisect_right(boundaries, value)] += 1
    count = len(counts_left)
    result = 0.0
    for a, b in zip(counts_left, counts_right, strict=True):
        p = (a + smoothing) / (len(left) + smoothing * count)
        q = (b + smoothing) / (len(right) + smoothing * count)
        result += (q - p) * math.log(q / p)
    return {
        **base,
        "status": "AVAILABLE",
        "reason": None,
        "value": result,
        "boundaries": boundaries,
        "effective_bin_count": count,
        "reference_counts": counts_left,
        "comparison_counts": counts_right,
    }


def numeric_comparison(
    reference: list[object],
    comparison: list[object],
    *,
    minimum_rows: int,
    minimum_non_null: int,
    bins: int,
    smoothing: float,
) -> dict[str, object]:
    a, b = numeric_summary(reference), numeric_summary(comparison)
    reason = (
        "INSUFFICIENT_COHORT_ROWS"
        if min(len(reference), len(comparison)) < minimum_rows
        else "INSUFFICIENT_NON_NULL_ROWS"
        if min(cast(int, a["non_null"]), cast(int, b["non_null"])) < minimum_non_null
        else None
    )
    unavailable = {"status": "UNAVAILABLE", "reason": reason, "value": None}
    return {
        "reference": a,
        "comparison": b,
        "missing_fraction_difference": (
            cast(float, b["missing_fraction"]) - cast(float, a["missing_fraction"])
            if a["missing_fraction"] is not None and b["missing_fraction"] is not None
            else None
        ),
        "ks_d": unavailable if reason else ks_distance(reference, comparison),
        "psi": (
            {
                **unavailable,
                "target_bins": bins,
                "smoothing_pseudocount_per_bin": smoothing,
                "boundaries": [],
                "effective_bin_count": 0,
                "reference_counts": [],
                "comparison_counts": [],
            }
            if reason
            else psi(reference, comparison, bins=bins, smoothing=smoothing)
        ),
    }


def categorical_comparison(
    reference: list[str | None], comparison: list[str | None], *, minimum_rows: int
) -> dict[str, object]:
    if any(value == MISSING_CATEGORY for value in reference + comparison):
        raise MonitoringError("Literal reserved missing-category token is not supported")
    if any(value is not None and not isinstance(value, str) for value in reference + comparison):
        raise MonitoringError("Categorical distribution contains non-string value")
    a = Counter(MISSING_CATEGORY if value is None else value for value in reference)
    b = Counter(MISSING_CATEGORY if value is None else value for value in comparison)
    names = sorted(set(a) | set(b))
    counts_a = {name: a[name] for name in names}
    counts_b = {name: b[name] for name in names}
    proportions_a = {name: a[name] / len(reference) if reference else None for name in names}
    proportions_b = {name: b[name] / len(comparison) if comparison else None for name in names}
    result: dict[str, object] = {
        "reference_counts": counts_a,
        "comparison_counts": counts_b,
        "reference_proportions": proportions_a,
        "comparison_proportions": proportions_b,
        "new_categories": [
            {"name": name, "count": b[name], "share": b[name] / len(comparison)}
            for name in names
            if not a[name] and b[name]
        ],
        "disappeared_categories": [name for name in names if a[name] and not b[name]],
        "null_policy": f"null is counted as {MISSING_CATEGORY}",
    }
    if min(len(reference), len(comparison)) < minimum_rows:
        return {
            **result,
            "status": "UNAVAILABLE",
            "reason": "INSUFFICIENT_COHORT_ROWS",
            "tvd": None,
            "js_divergence_base2": None,
        }
    tvd = 0.5 * sum(
        abs(cast(float, proportions_a[n]) - cast(float, proportions_b[n])) for n in names
    )
    js = 0.0
    for name in names:
        p, q = cast(float, proportions_a[name]), cast(float, proportions_b[name])
        midpoint = (p + q) / 2
        if p:
            js += 0.5 * p * math.log2(p / midpoint)
        if q:
            js += 0.5 * q * math.log2(q / midpoint)
    return {**result, "status": "AVAILABLE", "reason": None, "tvd": tvd, "js_divergence_base2": js}
