"""Observed retained-state and timer-expiry examples from real statistical output."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from math import isclose, sqrt
from pathlib import Path
from typing import cast

import pyarrow.fs as pa_fs
from deltalake import DeltaTable

from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import Plan

from .contract import ReliabilityError


def _timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise ReliabilityError("Missing event-time watermark or timestamp")


def examples(
    statistical_path: Path,
    plan: Plan,
    wave_zero_progress: dict[str, object],
) -> dict[str, object]:
    batches = cast(list[dict[str, object]], wave_zero_progress["batches"])
    watermarks = [
        cast(dict[str, object], batch["eventTime"]).get("watermark")
        for batch in batches
        if isinstance(batch.get("eventTime"), dict)
    ]
    if not watermarks:
        raise ReliabilityError("No observed statistical watermark for state examples")
    watermark = _timestamp(watermarks[-1])
    removed = sum(
        cast(int, state.get("numRowsRemoved", 0))
        for batch in batches
        for state in cast(list[dict[str, object]], batch.get("stateOperators", []))
    )
    by_id = {row["event_id"]: int(row["wave"]) for row in plan.rows}
    rows = (
        DeltaTable(statistical_path)
        .to_pyarrow_table(
            filesystem=pa_fs.SubTreeFileSystem(
                str(statistical_path.resolve()), pa_fs.LocalFileSystem()
            ),
            columns=[
                "event_id",
                "customer_id",
                "currency",
                "event_time",
                "amount",
                "kafka_offset",
                "prior_amount_observation_count",
                "prior_amount_mean",
                "prior_amount_stddev",
                "statistical_feature_status",
            ],
        )
        .to_pylist()
    )
    by_key: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_key[(cast(str, row["customer_id"]), cast(str, row["currency"]))].append(row)
    retained: dict[str, object] | None = None
    expired: dict[str, object] | None = None
    for (customer, currency), values in sorted(by_key.items()):
        previous = [row for row in values if by_id[cast(str, row["event_id"])] == 0]
        later = [row for row in values if by_id[cast(str, row["event_id"])] == 1]
        if not previous or not later:
            continue
        last = max(previous, key=lambda row: _timestamp(row["event_time"]))
        first = min(later, key=lambda row: cast(int, row["kafka_offset"]))
        deadline = _timestamp(last["event_time"]) + timedelta(hours=24)
        common = {
            "customer_id": customer,
            "currency": currency,
            "prior_event_id": last["event_id"],
            "later_event_id": first["event_id"],
            "prior_max_event_time": _timestamp(last["event_time"]).isoformat(),
            "later_event_time": _timestamp(first["event_time"]).isoformat(),
            "expiry_deadline": deadline.isoformat(),
            "observed_wave_zero_watermark": watermark.isoformat(),
            "prior_wave_observations": len(previous),
            "later_prior_count": first["prior_amount_observation_count"],
            "later_status": first["statistical_feature_status"],
        }
        if (
            retained is None
            and deadline > watermark
            and cast(int, first["prior_amount_observation_count"]) == len(previous)
            and first["prior_amount_mean"] is not None
        ):
            expected = float(
                sum((cast(Decimal, row["amount"]) for row in previous), Decimal(0))
                / Decimal(len(previous))
            )
            actual = cast(float, first["prior_amount_mean"])
            expected_stddev = (
                sqrt(
                    sum((float(cast(Decimal, row["amount"])) - expected) ** 2 for row in previous)
                    / (len(previous) - 1)
                )
                if len(previous) >= 2
                else None
            )
            observed_stddev = cast(float | None, first["prior_amount_stddev"])
            stddev_matches = (expected_stddev is None and observed_stddev is None) or (
                expected_stddev is not None
                and expected_stddev > 1e-12
                and observed_stddev is not None
                and isclose(observed_stddev, expected_stddev, rel_tol=1e-9, abs_tol=1e-9)
            )
            if isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9) and stddev_matches:
                retained = {
                    **common,
                    "expected_prior_mean": expected,
                    "observed_prior_mean": actual,
                    "expected_prior_stddev": expected_stddev,
                    "observed_prior_stddev": observed_stddev,
                }
        if (
            expired is None
            and deadline < watermark
            and removed > 0
            and first["prior_amount_observation_count"] == 0
            and first["statistical_feature_status"] == "NO_HISTORY"
        ):
            expired = {**common, "observed_state_rows_removed": removed}
    return {"retained": retained, "expired_and_reactivated": expired}
