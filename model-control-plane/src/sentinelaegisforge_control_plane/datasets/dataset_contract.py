"""Exact corrected v2 feature boundary and declared future model candidates."""

import hashlib
import json
import math
from datetime import datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

import pyarrow as pa

from .errors import FeatureDataError, SourceSchemaError
from .labels import require_utc

DATASET_CONTRACT_VERSION = "transaction-fraud-v1"
# Phase 10.1 stores Double approximations. Permit ordinary Double roundoff in
# recomputed z-scores, including values near zero, without accepting material drift.
ZSCORE_REL_TOL = 1e-9
ZSCORE_ABS_TOL = 1e-9
CANDIDATE_FEATURE_COLUMNS = (
    "amount",
    "currency",
    "country",
    "transaction_type",
    "prior_transaction_count_5m",
    "prior_amount_sum_10m",
    "prior_amount_observation_count",
    "prior_amount_mean",
    "prior_amount_stddev",
    "amount_zscore",
    "statistical_feature_status",
)
DEFERRED_IDENTIFIER_COLUMNS = ("merchant_id", "device_id", "ip_address")
NON_FEATURE_COLUMNS = (
    "event_id",
    "transaction_id",
    "customer_id",
    *DEFERRED_IDENTIFIER_COLUMNS,
    "event_time",
    "ingestion_time",
    "schema_version",
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "fraud_label",
    "label_observed_at",
    "label_source",
    "label_revision",
)
FORBIDDEN_RISK_COLUMNS = (
    "risk_disposition",
    "matched_rule_ids",
    "reason_codes",
    "policy_version",
    "policy_fingerprint",
    "matched_rule_count",
)
SOURCE_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("transaction_id", pa.string()),
        ("customer_id", pa.string()),
        ("merchant_id", pa.string()),
        ("event_time", pa.timestamp("us", tz="UTC")),
        ("ingestion_time", pa.timestamp("us", tz="UTC")),
        ("amount", pa.decimal128(18, 4)),
        ("currency", pa.string()),
        ("country", pa.string()),
        ("device_id", pa.string()),
        ("ip_address", pa.string()),
        ("transaction_type", pa.string()),
        ("schema_version", pa.int32()),
        ("kafka_key", pa.string()),
        ("kafka_topic", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("kafka_timestamp", pa.timestamp("us", tz="UTC")),
        ("prior_transaction_count_5m", pa.int64()),
        ("prior_amount_sum_10m", pa.decimal128(38, 4)),
        ("prior_amount_observation_count", pa.int64()),
        ("prior_amount_mean", pa.float64()),
        ("prior_amount_stddev", pa.float64()),
        ("amount_zscore", pa.float64()),
        ("statistical_feature_status", pa.string()),
    ]
)


def schema_fingerprint(schema: pa.Schema) -> str:
    # Ordered logical names/types, not Arrow metadata or library-dependent serialized bytes.
    fields = [{"name": field.name, "logical_type": str(field.type)} for field in schema]
    canonical = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def validate_schema(schema: pa.Schema) -> None:
    actual = [(field.name, field.type) for field in schema]
    expected = [(field.name, field.type) for field in SOURCE_SCHEMA]
    if actual != expected:
        raise SourceSchemaError(
            f"Expected the full ordered 25-column statistical v2 schema. Actual: {schema}"
        )


def feature_rows(table: pa.Table) -> list[dict[str, object]]:
    validate_schema(table.schema)
    rows = cast(list[dict[str, object]], table.to_pylist())
    seen: set[str] = set()
    optional = {"prior_amount_mean", "prior_amount_stddev", "amount_zscore"}
    for row in rows:
        event_id = row["event_id"]
        if not isinstance(event_id, str):
            raise FeatureDataError("event_id must be a canonical UUID string")
        try:
            if str(UUID(event_id)) != event_id:
                raise ValueError("noncanonical UUID")
        except ValueError as error:
            raise FeatureDataError(f"Malformed feature event_id: {event_id}") from error
        if event_id in seen:
            raise FeatureDataError(
                f"Duplicate source event_id: {event_id}; replay policy is not defined"
            )
        seen.add(event_id)
        for field, value in row.items():
            if value is None and field not in optional:
                raise FeatureDataError(f"{event_id}: missing required source value {field}")
            if isinstance(value, str) and not value.strip():
                raise FeatureDataError(f"{event_id}: blank source value {field}")
        for name in ("event_time", "ingestion_time", "kafka_timestamp"):
            time = row[name]
            if not isinstance(time, datetime):
                raise FeatureDataError(f"{event_id}: invalid {name}")
            require_utc(time)
        for name in ("amount", "prior_amount_sum_10m"):
            amount = row[name]
            if not isinstance(amount, Decimal) or not amount.is_finite() or amount < 0:
                raise FeatureDataError(f"{event_id}: invalid decimal money {name}")
            if name == "amount" and amount == 0:
                raise FeatureDataError(f"{event_id}: transaction amount must be positive")
        for name in ("prior_transaction_count_5m", "prior_amount_observation_count"):
            count = row[name]
            if type(count) is not int or count < 0:
                raise FeatureDataError(f"{event_id}: invalid nonnegative count {name}")
        _validate_statistics(row, event_id)
    return rows


def _validate_statistics(row: dict[str, object], event_id: str) -> None:
    count = cast(int, row["prior_amount_observation_count"])
    mean, stddev, z = (
        row[name] for name in ("prior_amount_mean", "prior_amount_stddev", "amount_zscore")
    )
    for value in (mean, stddev, z):
        if value is not None and (not isinstance(value, float) or not math.isfinite(value)):
            raise FeatureDataError(f"{event_id}: statistical values must be finite doubles or null")
    if count > 0 and (not isinstance(mean, float) or mean <= 0):
        raise FeatureDataError(f"{event_id}: prior_amount_mean must be positive with history")
    status = row["statistical_feature_status"]
    consistent = False
    if status == "NO_HISTORY":
        consistent = count == 0 and mean is None and stddev is None and z is None
    elif status == "INSUFFICIENT_VARIANCE_HISTORY":
        consistent = count == 1 and mean is not None and stddev is None and z is None
    elif status == "ZERO_VARIANCE":
        consistent = (
            count >= 2
            and mean is not None
            and isinstance(stddev, float)
            and 0 <= stddev <= 1e-12
            and z is None
        )
    elif status == "READY":
        consistent = (
            count >= 2
            and mean is not None
            and isinstance(stddev, float)
            and stddev > 1e-12
            and z is not None
        )
    if not consistent:
        raise FeatureDataError(f"{event_id}: inconsistent statistical context {status}")
    if status == "READY":
        # Mirror Scala's decimal-to-Double scoring calculation for validation only.
        expected = (float(cast(Decimal, row["amount"])) - cast(float, mean)) / cast(float, stddev)
        if not math.isfinite(expected) or not math.isclose(
            cast(float, z), expected, rel_tol=ZSCORE_REL_TOL, abs_tol=ZSCORE_ABS_TOL
        ):
            raise FeatureDataError(f"{event_id}: amount_zscore is inconsistent with prior state")
