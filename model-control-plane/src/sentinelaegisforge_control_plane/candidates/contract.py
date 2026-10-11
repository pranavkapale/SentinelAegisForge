"""Strict versioned input boundary for the Phase 13 logistic pipeline."""

import hashlib
import math
from decimal import Decimal
from typing import cast

from sentinelaegisforge_control_plane.datasets.dataset_contract import (
    CANDIDATE_FEATURE_COLUMNS,
    ZSCORE_ABS_TOL,
    ZSCORE_REL_TOL,
)
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.training.temporal import (
    CATEGORICAL_FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
)

CONTRACT_VERSION = "offline-logistic-candidate-v1"
INPUT_CONTRACT_VERSION = "offline-logistic-input-v1"
SERIALIZATION_FORMAT = "skops"
SKOPS_TRUSTED_TYPES = ["numpy.dtype"]  # Required by this pinned sklearn Pipeline only.
FEATURES = tuple(CANDIDATE_FEATURE_COLUMNS)
NULLABLE_STATISTICS = ("prior_amount_mean", "prior_amount_stddev", "amount_zscore")
LOGICAL_TYPES = {
    "amount": "decimal(18,4)",
    "currency": "string",
    "country": "string",
    "transaction_type": "string",
    "prior_transaction_count_5m": "int64",
    "prior_amount_sum_10m": "decimal(38,4)",
    "prior_amount_observation_count": "int64",
    "prior_amount_mean": "float64",
    "prior_amount_stddev": "float64",
    "amount_zscore": "float64",
    "statistical_feature_status": "string",
}
INPUT_CONTRACT: dict[str, object] = {
    "version": INPUT_CONTRACT_VERSION,
    "ordered_features": list(FEATURES),
    "fields": [
        {
            "name": name,
            "logical_type": LOGICAL_TYPES[name],
            "nullable": name in NULLABLE_STATISTICS,
        }
        for name in FEATURES
    ],
    "categorical": list(CATEGORICAL_FEATURE_COLUMNS),
    "numeric": list(NUMERIC_FEATURE_COLUMNS),
    "nullable": list(NULLABLE_STATISTICS),
    "money": {"amount": "decimal(18,4)", "prior_amount_sum_10m": "decimal(38,4)"},
    "positive_class": {"label": 1, "meaning": "FRAUD"},
    "unknown_categories": "OneHotEncoder(handle_unknown=ignore)",
    "model_float_projection": ["amount", "prior_amount_sum_10m"],
    "output": "fraud_probability_estimate in [0,1]; uncalibrated, offline only",
}
INPUT_FINGERPRINT = hashlib.sha256(canonical_json(INPUT_CONTRACT)).hexdigest()


class CandidateError(ValueError):
    """Candidate provenance, input, serialization or integrity is invalid."""


def _decimal(value: object, name: str, precision: int) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise CandidateError(f"{name} must be a finite Decimal")
    exponent = value.as_tuple().exponent
    if (
        not isinstance(exponent, int)
        or exponent < -4
        or len(value.as_tuple().digits) + max(exponent, 0) > precision
    ):
        raise CandidateError(f"{name} exceeds decimal({precision},4)")
    if value < 0 or (name == "amount" and value == 0):
        raise CandidateError(f"{name} must be {'positive' if name == 'amount' else 'nonnegative'}")
    return value


def validate_input(row: dict[str, object]) -> dict[str, object]:
    """Reject coercion and non-feature leakage; preserve exact Decimals until projection."""
    if any(not isinstance(key, str) for key in row):
        raise CandidateError("Candidate input column names must be strings")
    if set(row) != set(FEATURES):
        missing = sorted(set(FEATURES) - set(row))
        extra = sorted(set(row) - set(FEATURES))
        raise CandidateError(f"Input features differ; missing={missing}, extra={extra}")
    for name in CATEGORICAL_FEATURE_COLUMNS:
        value = row[name]
        if not isinstance(value, str) or not value.strip():
            raise CandidateError(f"{name} must be a nonblank string")
    if (
        not isinstance(row["currency"], str)
        or len(row["currency"]) != 3
        or not row["currency"].isascii()
        or not row["currency"].isalpha()
        or not row["currency"].isupper()
    ):
        raise CandidateError("currency must have three uppercase letters")
    if (
        not isinstance(row["country"], str)
        or len(row["country"]) != 2
        or not row["country"].isascii()
        or not row["country"].isalpha()
        or not row["country"].isupper()
    ):
        raise CandidateError("country must have two uppercase letters")
    _decimal(row["amount"], "amount", 18)
    _decimal(row["prior_amount_sum_10m"], "prior_amount_sum_10m", 38)
    for name in ("prior_transaction_count_5m", "prior_amount_observation_count"):
        value = row[name]
        if type(value) is not int or value < 0:
            raise CandidateError(f"{name} must be a nonnegative integer")
    for name in ("prior_amount_mean", "prior_amount_stddev", "amount_zscore"):
        value = row[name]
        if value is not None and (type(value) is not float or not math.isfinite(value)):
            raise CandidateError(f"{name} must be a finite float or null")
    count = cast(int, row["prior_amount_observation_count"])
    mean = row["prior_amount_mean"]
    stddev = row["prior_amount_stddev"]
    zscore = row["amount_zscore"]
    status = row["statistical_feature_status"]
    if count > 0 and (not isinstance(mean, float) or mean <= 0):
        raise CandidateError("Positive history requires positive prior_amount_mean")
    valid = (
        (
            status == "NO_HISTORY"
            and count == 0
            and mean is None
            and stddev is None
            and zscore is None
        )
        or (
            status == "INSUFFICIENT_VARIANCE_HISTORY"
            and count == 1
            and mean is not None
            and stddev is None
            and zscore is None
        )
        or (
            status == "ZERO_VARIANCE"
            and count >= 2
            and mean is not None
            and isinstance(stddev, float)
            and 0 <= stddev <= 1e-12
            and zscore is None
        )
        or (
            status == "READY"
            and count >= 2
            and mean is not None
            and isinstance(stddev, float)
            and stddev > 1e-12
            and isinstance(zscore, float)
        )
    )
    if not valid:
        raise CandidateError("Invalid statistical status/null combination")
    if status == "READY":
        expected = (float(cast(Decimal, row["amount"])) - cast(float, mean)) / cast(float, stddev)
        if not math.isfinite(expected) or not math.isclose(
            cast(float, zscore), expected, rel_tol=ZSCORE_REL_TOL, abs_tol=ZSCORE_ABS_TOL
        ):
            raise CandidateError("READY amount_zscore is inconsistent with monetary context")
    return {name: row[name] for name in FEATURES}
