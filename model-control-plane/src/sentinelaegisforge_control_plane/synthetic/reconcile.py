"""Event-ID, currency, and label-availability checks over the real Delta lineage."""

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from deltalake import DeltaTable

from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.labels import require_utc, resolve_labels

from .corpus import PlannedEvent, ScenarioCorpus, SyntheticCorpusError


@dataclass(frozen=True)
class DeltaBoundary:
    version: int
    rows: int
    event_ids: frozenset[str]
    records: tuple[dict[str, object], ...]


def _boundary(path: Path, columns: list[str]) -> DeltaBoundary:
    try:
        table = DeltaTable(path)
        version = table.version()
        records = tuple(
            cast(list[dict[str, object]], table.to_pyarrow_table(columns=columns).to_pylist())
        )
    except Exception as error:
        raise SyntheticCorpusError(f"Cannot read Delta boundary {path}: {error}") from error
    ids = [cast(str, row["event_id"]) for row in records]
    if len(ids) != len(set(ids)):
        raise SyntheticCorpusError(f"Duplicate materialized event IDs at {path}")
    return DeltaBoundary(version, len(ids), frozenset(ids), records)


def _publication(corpus: ScenarioCorpus) -> tuple[dict[str, object], frozenset[tuple[int, int]]]:
    offsets: set[tuple[int, int]] = set()
    for wave in range(3):
        report = corpus.path / f"publish-report-wave-{wave}.tsv"
        attempt = corpus.path / f"publish-attempt-wave-{wave}.tsv"
        if not report.is_file() or not attempt.is_file():
            raise SyntheticCorpusError(f"Missing publish report/attempt for wave {wave}")
        lines = [line.split("\t") for line in report.read_text(encoding="utf-8").splitlines()]
        expected = sum(row.wave == wave for row in corpus.plan)
        if lines[:3] != [
            ["requested", str(expected)],
            ["acknowledged", str(expected)],
            ["failed", "0"],
        ]:
            raise SyntheticCorpusError(f"Publish acknowledgement count differs in wave {wave}")
        if len(lines[3:]) != expected or any(
            len(line) != 4 or line[:2] != ["ack", "transactions.raw"] for line in lines[3:]
        ):
            raise SyntheticCorpusError(f"Publish acknowledgement details differ in wave {wave}")
        for line in lines[3:]:
            coordinate = (int(line[2]), int(line[3]))
            if coordinate in offsets:
                raise SyntheticCorpusError("Repeated Kafka acknowledgement coordinate")
            offsets.add(coordinate)
    return (
        {"acknowledged": len(offsets), "unique_kafka_coordinates": len(offsets)},
        frozenset(offsets),
    )


def reconcile(
    corpus: ScenarioCorpus,
    validated_path: Path,
    deduplicated_path: Path,
    rolling_path: Path,
    statistical_path: Path,
) -> dict[str, object]:
    publication, acknowledged_coordinates = _publication(corpus)
    expected = frozenset(event.event_id for event in corpus.plan)
    boundaries: dict[str, DeltaBoundary] = {}
    for name, path, columns in (
        ("validated", validated_path, ["event_id", "kafka_partition", "kafka_offset"]),
        ("deduplicated", deduplicated_path, ["event_id"]),
        (
            "rolling",
            rolling_path,
            [
                "event_id",
                "customer_id",
                "currency",
                "prior_transaction_count_5m",
                "prior_amount_sum_10m",
            ],
        ),
        (
            "statistical",
            statistical_path,
            [
                "event_id",
                "customer_id",
                "currency",
                "prior_amount_observation_count",
                "prior_amount_mean",
                "statistical_feature_status",
            ],
        ),
    ):
        boundary = _boundary(path, columns)
        if boundary.event_ids != expected or boundary.rows != len(expected):
            raise SyntheticCorpusError(
                f"{name} event-ID mismatch: missing={len(expected - boundary.event_ids)} "
                f"unexpected={len(boundary.event_ids - expected)} rows={boundary.rows} "
                f"expected={len(expected)}"
            )
        boundaries[name] = boundary
        if name == "validated":
            materialized_coordinates = [
                (cast(int, row["kafka_partition"]), cast(int, row["kafka_offset"]))
                for row in boundary.records
            ]
            if (
                len(set(materialized_coordinates)) != len(materialized_coordinates)
                or frozenset(materialized_coordinates) != acknowledged_coordinates
            ):
                raise SyntheticCorpusError(
                    "Validated Kafka coordinates differ from publish acknowledgements"
                )
    # Phase 12's strict source validator also checks all 25 statistical columns,
    # exact decimals, nullable status structure, and z-score consistency.
    StatisticalFeatureSource(statistical_path, boundaries["statistical"].version).read()
    _verify_rolling(corpus.plan, boundaries["rolling"])
    _verify_mixed_currency(corpus.plan, boundaries["rolling"], boundaries["statistical"])
    return {
        "corpus_id": corpus.manifest["corpus_id"],
        "generated": len(corpus.plan),
        **publication,
        "boundaries": {
            name: {"delta_version": value.version, "rows": value.rows, "missing_ids": 0}
            for name, value in boundaries.items()
        },
        "currency_safe_rolling_rows_checked": len(corpus.plan),
        "mixed_currency_statistical_example": "first USD/EUR/USD customer uses USD-only prior mean",
    }


def _verify_rolling(plan: tuple[PlannedEvent, ...], actual: DeltaBoundary) -> None:
    by_id = {cast(str, row["event_id"]): row for row in actual.records}
    previously_observed: dict[str, list[PlannedEvent]] = defaultdict(list)
    # Phase 9 orders each customer's rows by event time within a source wave.
    # Publication order may deliberately include eligible out-of-order events.
    for event in sorted(plan, key=lambda row: (row.wave, row.event_time, row.event_id)):
        prior = previously_observed[event.customer_id]
        five_minutes = [
            old for old in prior if 0 < (event.event_time - old.event_time).total_seconds() <= 300
        ]
        ten_minutes_same_currency = [
            old
            for old in prior
            if old.currency == event.currency
            and 0 < (event.event_time - old.event_time).total_seconds() <= 600
        ]
        row = by_id[event.event_id]
        if (
            row["customer_id"] != event.customer_id
            or row["currency"] != event.currency
            or row["prior_transaction_count_5m"] != len(five_minutes)
            or row["prior_amount_sum_10m"]
            != sum((old.amount for old in ten_minutes_same_currency), Decimal("0"))
        ):
            raise SyntheticCorpusError(f"Currency-safe rolling feature differs: {event.event_id}")
        prior.append(event)


def _verify_mixed_currency(
    plan: tuple[PlannedEvent, ...], rolling: DeltaBoundary, statistical: DeltaBoundary
) -> None:
    first = plan[:3]
    if len(first) != 3 or [event.currency for event in first] != ["USD", "EUR", "USD"]:
        raise SyntheticCorpusError("Expected controlled USD/EUR/USD scenario prefix")
    if len({event.customer_id for event in first}) != 1:
        raise SyntheticCorpusError("Expected one customer in mixed-currency scenario prefix")
    rolled = {cast(str, row["event_id"]): row for row in rolling.records}
    stats = {cast(str, row["event_id"]): row for row in statistical.records}
    if (
        rolled[first[2].event_id]["prior_transaction_count_5m"] != 2
        or rolled[first[2].event_id]["prior_amount_sum_10m"] != first[0].amount
        or stats[first[1].event_id]["prior_amount_observation_count"] != 0
        or stats[first[2].event_id]["prior_amount_observation_count"] != 1
        or stats[first[2].event_id]["prior_amount_mean"] != float(first[0].amount)
    ):
        raise SyntheticCorpusError("Mixed-currency feature semantics differ")


def temporal_quality(
    corpus: ScenarioCorpus,
    train_end: datetime,
    validation_end: datetime,
    test_end: datetime,
    as_of: datetime,
) -> dict[str, object]:
    train_end, validation_end, test_end, as_of = (
        require_utc(value) for value in (train_end, validation_end, test_end, as_of)
    )
    if not train_end < validation_end < test_end <= as_of:
        raise SyntheticCorpusError("Invalid temporal quality-report boundaries")
    selected = resolve_labels(corpus.labels, as_of)
    previous_customers: set[str] = set()
    report: dict[str, object] = {}
    for name, lower, upper in (
        ("training", None, train_end),
        ("validation", train_end, validation_end),
        ("test", validation_end, test_end),
    ):
        cohort = [
            row
            for row in corpus.plan
            if (lower is None or row.ingestion_time >= lower) and row.ingestion_time < upper
        ]
        available = [row for row in cohort if row.event_id in selected]
        known_for_fit = [
            row
            for row in available
            if name != "training" or selected[row.event_id].label_observed_at <= train_end
        ]
        currencies = Counter(row.currency for row in cohort)
        outcomes = Counter(selected[row.event_id].outcome.value for row in available)
        customers = {row.customer_id for row in cohort}
        delays = Counter(
            "short_<=24h"
            if (selected[row.event_id].label_observed_at - row.ingestion_time).total_seconds()
            <= 86400
            else "long_>24h"
            for row in available
        )
        report[name] = {
            "events": len(cohort),
            "customers": len(customers),
            "new_customers": len(customers - previous_customers),
            "currencies": dict(sorted(currencies.items())),
            "labeled": len(available),
            "unlabeled": len(cohort) - len(available),
            "outcomes": dict(sorted(outcomes.items())),
            "label_delay_distribution": dict(sorted(delays.items())),
            "eligible_for_fit_or_evaluation": len(known_for_fit),
            "delayed_or_revised_fit_label_exclusions": len(available) - len(known_for_fit),
        }
        previous_customers |= customers
    return report
