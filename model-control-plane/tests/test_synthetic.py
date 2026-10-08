"""Docker-independent checks for the private scenario/feature reconciliation boundary."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from sentinelaegisforge_control_plane.synthetic.corpus import (
    PLAN_COLUMNS,
    TRUTH_COLUMNS,
    PlannedEvent,
    SyntheticCorpusError,
    inspect_corpus,
)
from sentinelaegisforge_control_plane.synthetic.reconcile import (
    DeltaBoundary,
    _verify_rolling,
    reconcile,
    temporal_quality,
)


def _hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical(fields: dict[str, str]) -> bytes:
    return "".join(f"{key}\t{value}\n" for key, value in sorted(fields.items())).encode()


def _event(number: int) -> str:
    return str(UUID(int=number))


def fixture_corpus(
    root: Path,
    *,
    duplicate: bool = False,
    label_before_ingestion: bool = False,
    missing_revision: bool = False,
    unknown_label: bool = False,
) -> Path:
    config = {
        "scenario_contract_version": "synthetic-fraud-scenario-v1",
        "seed": "41",
        "base_time": "2030-01-01T00:00:00Z",
        "transaction_count": "3",
        "customer_count": "2",
        "time_horizon_days": "9",
        "usd_weight_percent": "65",
        "behavior_profile": "mixed-v1",
        "label_delay_profile": "short-long-v1",
    }
    ids = [_event(1), _event(1 if duplicate else 2), _event(3)]
    ingestions = [
        "2030-01-01T00:00:02Z",
        "2030-01-04T00:00:02Z",
        "2030-01-07T00:00:02Z",
    ]
    plan_rows = []
    truth_rows = []
    labels = []
    for index, event_id in enumerate(ids):
        plan_rows.append(
            [
                event_id,
                f"txn-{index}",
                "customer-0000",
                "merchant-0000",
                ingestions[index].replace(":02Z", ":00Z"),
                ingestions[index],
                "100.0000",
                "USD" if index != 1 else "EUR",
                "US",
                "device-0000",
                "192.0.2.1",
                "CARD_PAYMENT",
                "1",
                str(index),
            ]
        )
        first_observed = (
            "2029-12-31T00:00:00Z"
            if index == 0 and label_before_ingestion
            else ["2030-01-02T00:00:00Z", "2030-01-05T00:00:00Z", "2030-01-08T00:00:00Z"][index]
        )
        truth_rows.append(
            [
                event_id,
                "REGULAR",
                "FRAUD" if index == 0 else "LEGIT",
                "LEGIT",
                first_observed,
                "2030-01-03T00:00:00Z" if index == 0 else "",
                "41",
                "synthetic-fraud-scenario-v1",
            ]
        )
        labels.append(
            {
                "event_id": _event(4) if unknown_label and index == 2 else event_id,
                "outcome": "LEGIT",
                "label_observed_at": first_observed,
                "label_source": "SYNTHETIC_SCENARIO_V1",
                "label_revision": 1,
            }
        )
        if index == 0:
            labels.append(
                {
                    "event_id": event_id,
                    "outcome": "FRAUD",
                    "label_observed_at": "2030-01-03T00:00:00Z",
                    "label_source": "SYNTHETIC_SCENARIO_V1",
                    "label_revision": 2,
                }
            )
    if missing_revision:
        labels = [
            item
            for item in labels
            if not (item["event_id"] == _event(1) and item["label_revision"] == 1)
        ]
    plan = (
        "\t".join(PLAN_COLUMNS) + "\n" + "".join("\t".join(row) + "\n" for row in plan_rows)
    ).encode()
    truth = (
        "\t".join(TRUTH_COLUMNS) + "\n" + "".join("\t".join(row) + "\n" for row in truth_rows)
    ).encode()
    label_bytes = "".join(json.dumps(item) + "\n" for item in labels).encode()
    config_hash = _hash(_canonical(config))
    identity = {
        "scenario_contract_version": "synthetic-fraud-scenario-v1",
        "generator_config_sha256": config_hash,
        "plan_sha256": _hash(plan),
        "truth_sha256": _hash(truth),
    }
    corpus_id = _hash(_canonical(identity))
    manifest = {
        **config,
        **identity,
        "corpus_id": corpus_id,
        "labels_sha256": _hash(label_bytes),
        "generated_transaction_count": "3",
        "label_record_count": str(len(labels)),
        "fraud_count": "1",
        "first_label_available_by_as_of": "3",
        "label_as_of_time": "2030-01-20T00:00:00Z",
    }
    target = root / corpus_id
    target.mkdir(parents=True)
    (target / "manifest.tsv").write_bytes(_canonical(manifest))
    (target / "plan.tsv").write_bytes(plan)
    (target / "truth.tsv").write_bytes(truth)
    (target / "labels.jsonl").write_bytes(label_bytes)
    return target


def test_private_truth_and_temporal_availability(tmp_path: Path) -> None:
    corpus = inspect_corpus(fixture_corpus(tmp_path))
    assert [row.currency for row in corpus.plan] == ["USD", "EUR", "USD"]
    report = temporal_quality(
        corpus,
        datetime(2030, 1, 4, tzinfo=UTC),
        datetime(2030, 1, 7, tzinfo=UTC),
        datetime(2030, 1, 10, tzinfo=UTC),
        datetime(2030, 1, 20, tzinfo=UTC),
    )
    assert [
        cast(dict[str, object], report[name])["events"]
        for name in ("training", "validation", "test")
    ] == [1, 1, 1]
    training = cast(dict[str, object], report["training"])
    assert training["delayed_or_revised_fit_label_exclusions"] == 0
    assert training["outcomes"] == {"FRAUD": 1}


@pytest.mark.parametrize(
    "problem,expected",
    [
        ({"duplicate": True}, "Duplicate scenario event_id"),
        ({"label_before_ingestion": True}, "Label chronology invalid"),
        ({"missing_revision": True}, "revisions must be"),
        ({"unknown_label": True}, "Label event IDs differ"),
    ],
)
def test_broken_scenario_truth_is_rejected(
    tmp_path: Path, problem: dict[str, bool], expected: str
) -> None:
    with pytest.raises(SyntheticCorpusError, match=expected):
        inspect_corpus(fixture_corpus(tmp_path, **problem))


def test_event_id_reconciliation_rejects_missing_feature(tmp_path: Path) -> None:
    corpus = inspect_corpus(fixture_corpus(tmp_path / "corpus"))
    for wave in range(3):
        (corpus.path / f"publish-attempt-wave-{wave}.tsv").write_text(
            f"corpus_id\t{corpus.manifest['corpus_id']}\nwave\t{wave}\nexpected\t1\n"
        )
        (corpus.path / f"publish-report-wave-{wave}.tsv").write_text(
            f"requested\t1\nacknowledged\t1\nfailed\t0\nack\ttransactions.raw\t0\t{wave}\n"
        )
    paths = {
        name: tmp_path / name for name in ("validated", "deduplicated", "rolling", "statistical")
    }
    for name, path in paths.items():
        ids = [_event(1), _event(2), _event(3)] if name != "validated" else [_event(1), _event(3)]
        data: dict[str, list[object]] = {"event_id": [cast(object, item) for item in ids]}
        if name == "validated":
            data["kafka_partition"] = [0, 0]
            data["kafka_offset"] = [0, 2]
        write_deltalake(path, pa.table(data))
    with pytest.raises(SyntheticCorpusError, match="validated event-ID mismatch: missing=1"):
        reconcile(
            corpus,
            paths["validated"],
            paths["deduplicated"],
            paths["rolling"],
            paths["statistical"],
        )


def test_event_id_reconciliation_rejects_unacknowledged_kafka_coordinate(tmp_path: Path) -> None:
    corpus = inspect_corpus(fixture_corpus(tmp_path / "corpus"))
    for wave in range(3):
        (corpus.path / f"publish-attempt-wave-{wave}.tsv").write_text(
            f"corpus_id\t{corpus.manifest['corpus_id']}\nwave\t{wave}\nexpected\t1\n"
        )
        (corpus.path / f"publish-report-wave-{wave}.tsv").write_text(
            f"requested\t1\nacknowledged\t1\nfailed\t0\nack\ttransactions.raw\t0\t{wave}\n"
        )
    paths = {
        name: tmp_path / name for name in ("validated", "deduplicated", "rolling", "statistical")
    }
    for name, path in paths.items():
        data: dict[str, list[object]] = {"event_id": [_event(1), _event(2), _event(3)]}
        if name == "validated":
            data["kafka_partition"] = [0, 0, 0]
            data["kafka_offset"] = [0, 1, 4]
        write_deltalake(path, pa.table(data))
    with pytest.raises(SyntheticCorpusError, match="coordinates differ"):
        reconcile(
            corpus,
            paths["validated"],
            paths["deduplicated"],
            paths["rolling"],
            paths["statistical"],
        )


def test_rolling_reconciliation_uses_phase9_event_time_order_within_wave() -> None:
    base = datetime(2030, 1, 1, tzinfo=UTC)
    plan = (
        PlannedEvent(_event(1), "customer-1", base, base, Decimal("100"), "USD", 0),
        PlannedEvent(
            _event(2),
            "customer-1",
            base + timedelta(seconds=15),
            base + timedelta(seconds=17),
            Decimal("200"),
            "EUR",
            0,
        ),
        PlannedEvent(
            _event(3),
            "customer-1",
            base + timedelta(seconds=10),
            base + timedelta(seconds=19),
            Decimal("300"),
            "USD",
            0,
        ),
    )
    records: tuple[dict[str, object], ...] = (
        {
            "event_id": _event(1),
            "customer_id": "customer-1",
            "currency": "USD",
            "prior_transaction_count_5m": 0,
            "prior_amount_sum_10m": Decimal("0"),
        },
        {
            "event_id": _event(2),
            "customer_id": "customer-1",
            "currency": "EUR",
            "prior_transaction_count_5m": 2,
            "prior_amount_sum_10m": Decimal("0"),
        },
        {
            "event_id": _event(3),
            "customer_id": "customer-1",
            "currency": "USD",
            "prior_transaction_count_5m": 1,
            "prior_amount_sum_10m": Decimal("100"),
        },
    )
    _verify_rolling(plan, DeltaBoundary(0, 3, frozenset(item.event_id for item in plan), records))
