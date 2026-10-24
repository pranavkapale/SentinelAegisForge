"""Explicit local Delta fixture commits; not a claim of Spark feature computation."""

import time
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from sentinelaegisforge_control_plane.availability.contract import AvailabilityError
from sentinelaegisforge_control_plane.datasets.dataset_contract import SOURCE_SCHEMA, feature_rows

MAX_WAVE = 200


def initialize_source(path: Path) -> dict[str, object]:
    if path.exists():
        raise AvailabilityError("Isolated fixture source already exists")
    write_deltalake(path, pa.Table.from_pylist([], schema=SOURCE_SCHEMA), mode="error")
    table = DeltaTable(path)
    return {
        "fixture_kind": "LOCAL_INCREMENTAL_FEATURE_FIXTURE",
        "delta_table_id": str(table.metadata().id),
        "delta_version": table.version(),
    }


def fixture_rows(
    seed: int, first_index: int, count: int, base_time: datetime
) -> list[dict[str, object]]:
    if (
        type(seed) is not int
        or type(first_index) is not int
        or first_index < 0
        or type(count) is not int
        or not 1 <= count <= MAX_WAVE
        or base_time.tzinfo is None
        or base_time.utcoffset() != timedelta(0)
    ):
        raise AvailabilityError("Fixture requires bounded indices and explicit UTC base time")
    rows: list[dict[str, object]] = []
    for index in range(first_index, first_index + count):
        instant = base_time + timedelta(seconds=index)
        event_id = str(uuid5(NAMESPACE_URL, f"sentinel-phase27:{seed}:{index}"))
        amount = Decimal(10000 + ((seed + index * 31) % 40000)).scaleb(-4)
        rows.append(
            {
                "event_id": event_id,
                "transaction_id": f"fixture-transaction-{seed}-{index}",
                "customer_id": f"fixture-customer-{seed}-{index}",
                "merchant_id": f"fixture-merchant-{index % 7}",
                "event_time": instant,
                "ingestion_time": instant + timedelta(seconds=1),
                "amount": amount,
                "currency": "USD" if index % 2 == 0 else "EUR",
                "country": "US" if index % 2 == 0 else "DE",
                "device_id": f"fixture-device-{index}",
                "ip_address": f"192.0.2.{1 + index % 200}",
                "transaction_type": "CARD_PAYMENT",
                "schema_version": 1,
                "kafka_key": f"fixture-customer-{seed}-{index}",
                "kafka_topic": "transactions.raw",
                "kafka_partition": index % 3,
                "kafka_offset": index,
                "kafka_timestamp": instant,
                "prior_transaction_count_5m": 0,
                "prior_amount_sum_10m": Decimal("0.0000"),
                "prior_amount_observation_count": 0,
                "prior_amount_mean": None,
                "prior_amount_stddev": None,
                "amount_zscore": None,
                "statistical_feature_status": "NO_HISTORY",
            }
        )
    feature_rows(pa.Table.from_pylist(rows, schema=SOURCE_SCHEMA))
    return rows


def append_wave(
    path: Path, seed: int, first_index: int, count: int, base_time: datetime
) -> dict[str, object]:
    """Return only observed post-commit facts and local write-operation timing."""
    before = DeltaTable(path)
    rows = fixture_rows(seed, first_index, count, base_time)
    committed_ids = {row["event_id"] for row in before.to_pyarrow_table().to_pylist()}
    if committed_ids & {row["event_id"] for row in rows}:
        raise AvailabilityError("Fixture wave would duplicate an already committed event")
    started = time.perf_counter()
    write_deltalake(path, pa.Table.from_pylist(rows, schema=SOURCE_SCHEMA), mode="append")
    elapsed = time.perf_counter() - started
    after = DeltaTable(path)
    if after.version() != before.version() + 1 or after.metadata().id != before.metadata().id:
        raise AvailabilityError("Fixture Delta commit version/table identity differs")
    return {
        "fixture_kind": "LOCAL_INCREMENTAL_FEATURE_FIXTURE",
        "delta_table_id": str(after.metadata().id),
        "delta_version": after.version(),
        "committed_row_count": len(rows),
        "first_index": first_index,
        "delta_write_seconds": elapsed,
    }
