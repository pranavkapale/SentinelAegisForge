"""Explicit ingestion-time cohorts and causal training-label eligibility."""

from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from sentinelaegisforge_control_plane.datasets.dataset_contract import CANDIDATE_FEATURE_COLUMNS
from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp, require_utc

from .errors import InsufficientTrainingDataError, TrainingContractError

TRAINING_CONTRACT_VERSION = "fraud-logistic-baseline-v1"
CLASSIFICATION_THRESHOLD = 0.5
CATEGORICAL_FEATURE_COLUMNS = (
    "currency",
    "country",
    "transaction_type",
    "statistical_feature_status",
)
NUMERIC_FEATURE_COLUMNS = (
    "amount",
    "prior_transaction_count_5m",
    "prior_amount_sum_10m",
    "prior_amount_observation_count",
    "prior_amount_mean",
    "prior_amount_stddev",
    "amount_zscore",
)
assert set(CATEGORICAL_FEATURE_COLUMNS) | set(NUMERIC_FEATURE_COLUMNS) == set(
    CANDIDATE_FEATURE_COLUMNS
)
assert not set(CATEGORICAL_FEATURE_COLUMNS) & set(NUMERIC_FEATURE_COLUMNS)


@dataclass(frozen=True)
class TemporalConfig:
    train_end: datetime
    validation_end: datetime
    test_end: datetime
    min_train_rows: int = 20
    min_validation_rows: int = 10
    min_test_rows: int = 10

    def __post_init__(self) -> None:
        for name in ("train_end", "validation_end", "test_end"):
            try:
                object.__setattr__(self, name, require_utc(getattr(self, name)))
            except (AttributeError, TypeError, ValueError) as error:
                raise TrainingContractError(f"{name} must be timezone-aware UTC") from error
        if not self.train_end < self.validation_end < self.test_end:
            raise TrainingContractError("Require train_end < validation_end < test_end")
        for name in ("min_train_rows", "min_validation_rows", "min_test_rows"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise TrainingContractError(f"{name} must be a positive integer")


@dataclass(frozen=True)
class TemporalCohorts:
    train: tuple[dict[str, object], ...]
    validation: tuple[dict[str, object], ...]
    test: tuple[dict[str, object], ...]
    train_period_rows: int
    excluded_from_fit_due_to_label_delay: int
    outside_test_window_rows: int


def partition_rows(
    rows: list[dict[str, object]], manifest: dict[str, object], config: TemporalConfig
) -> TemporalCohorts:
    if manifest.get("candidate_feature_columns") != list(CANDIDATE_FEATURE_COLUMNS):
        raise TrainingContractError("Snapshot candidate feature contract differs from Phase 12")
    try:
        as_of = parse_utc_timestamp(cast(str, manifest["as_of_time"]))
    except (KeyError, TypeError, ValueError) as error:
        raise TrainingContractError("Snapshot AS_OF_TIME is invalid") from error
    if config.test_end > as_of:
        raise TrainingContractError("test_end must not exceed snapshot AS_OF_TIME")

    train: list[dict[str, object]] = []
    validation: list[dict[str, object]] = []
    test: list[dict[str, object]] = []
    seen: set[str] = set()
    train_period = delayed = outside = 0
    for row in rows:
        event_id = row.get("event_id")
        try:
            if not isinstance(event_id, str) or str(UUID(event_id)) != event_id:
                raise ValueError("noncanonical event_id")
            if event_id in seen:
                raise TrainingContractError(f"Duplicate event_id in snapshot: {event_id}")
            seen.add(event_id)
            ingestion = require_utc(cast(datetime, row["ingestion_time"]))
            observed = require_utc(cast(datetime, row["label_observed_at"]))
            outcome = row["fraud_label"]
            if type(outcome) is not int or outcome not in (0, 1):
                raise TrainingContractError(f"Invalid fraud_label for {event_id}")
            if not ingestion <= observed <= as_of:
                raise TrainingContractError(f"Invalid label availability for {event_id}")
        except (KeyError, AttributeError, TypeError, ValueError) as error:
            raise TrainingContractError(f"Invalid snapshot chronology/identity: {error}") from error
        if ingestion < config.train_end:
            train_period += 1
            if observed <= config.train_end:
                train.append(row)
            else:
                delayed += 1
        elif ingestion < config.validation_end:
            validation.append(row)
        elif ingestion < config.test_end:
            test.append(row)
        else:
            outside += 1

    def ordered(items: list[dict[str, object]]) -> tuple[dict[str, object], ...]:
        return tuple(
            sorted(
                items,
                key=lambda item: (
                    cast(datetime, item["ingestion_time"]),
                    cast(str, item["event_id"]),
                ),
            )
        )

    cohorts = TemporalCohorts(
        ordered(train), ordered(validation), ordered(test), train_period, delayed, outside
    )
    failures: list[str] = []
    for name, items, minimum in (
        ("training", cohorts.train, config.min_train_rows),
        ("validation", cohorts.validation, config.min_validation_rows),
        ("test", cohorts.test, config.min_test_rows),
    ):
        if len(items) < minimum:
            failures.append(f"{name} has {len(items)} rows, requires at least {minimum}")
        classes = {cast(int, item["fraud_label"]) for item in items}
        if classes != {0, 1}:
            failures.append(f"{name} requires both FRAUD and LEGIT (observed {sorted(classes)})")
    if failures:
        raise InsufficientTrainingDataError("; ".join(failures))
    return cohorts
