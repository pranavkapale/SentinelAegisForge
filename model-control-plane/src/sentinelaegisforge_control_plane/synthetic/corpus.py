"""Inspect the Scala-generated plan and independent synthetic outcome truth."""

import csv
import hashlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from sentinelaegisforge_control_plane.datasets.labels import (
    LabelInput,
    load_labels,
    parse_utc_timestamp,
    resolve_labels,
)

VERSION = "synthetic-fraud-scenario-v1"
LABEL_SOURCE = "SYNTHETIC_SCENARIO_V1"
PLAN_COLUMNS = (
    "event_id",
    "transaction_id",
    "customer_id",
    "merchant_id",
    "event_time",
    "ingestion_time",
    "amount",
    "currency",
    "country",
    "device_id",
    "ip_address",
    "transaction_type",
    "schema_version",
    "wave",
)
TRUTH_COLUMNS = (
    "event_id",
    "latent_profile",
    "outcome",
    "initial_outcome",
    "first_observed_at",
    "correction_observed_at",
    "seed",
    "scenario_contract_version",
)
CONFIG_KEYS = (
    "scenario_contract_version",
    "seed",
    "base_time",
    "transaction_count",
    "customer_count",
    "time_horizon_days",
    "usd_weight_percent",
    "behavior_profile",
    "label_delay_profile",
)


class SyntheticCorpusError(ValueError):
    """The private scenario or its derived labels are inconsistent."""


@dataclass(frozen=True)
class PlannedEvent:
    event_id: str
    customer_id: str
    event_time: datetime
    ingestion_time: datetime
    amount: Decimal
    currency: str
    wave: int


@dataclass(frozen=True)
class ScenarioTruth:
    event_id: str
    latent_profile: str
    outcome: str
    initial_outcome: str
    first_observed_at: datetime
    correction_observed_at: datetime | None


@dataclass(frozen=True)
class ScenarioCorpus:
    path: Path
    manifest: dict[str, str]
    plan: tuple[PlannedEvent, ...]
    truth: tuple[ScenarioTruth, ...]
    labels: LabelInput


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _canonical(fields: dict[str, str]) -> bytes:
    return "".join(f"{key}\t{value}\n" for key, value in sorted(fields.items())).encode("utf-8")


def _tsv(path: Path, columns: tuple[str, ...]) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        if tuple(reader.fieldnames or ()) != columns:
            raise SyntheticCorpusError(f"Unexpected scenario columns: {path}")
        rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise SyntheticCorpusError(f"Malformed scenario row: {path}")
    return rows


def _manifest(path: Path) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t", 1)
        if len(parts) != 2 or parts[0] in fields:
            raise SyntheticCorpusError("Malformed or duplicate scenario manifest field")
        fields[parts[0]] = parts[1]
    return fields


def inspect_corpus(path: Path) -> ScenarioCorpus:
    if path.is_symlink() or not path.is_dir():
        raise SyntheticCorpusError(f"Not a scenario corpus: {path}")
    required = {"manifest.tsv", "plan.tsv", "truth.tsv", "labels.jsonl"}
    if not required <= {item.name for item in path.iterdir()}:
        raise SyntheticCorpusError("Scenario corpus artifacts are incomplete")
    if any((path / name).is_symlink() for name in required):
        raise SyntheticCorpusError("Scenario corpus artifacts must not be symlinks")
    try:
        manifest = _manifest(path / "manifest.tsv")
        if manifest["scenario_contract_version"] != VERSION:
            raise SyntheticCorpusError("Unsupported scenario contract")
        if manifest["corpus_id"] != path.name:
            raise SyntheticCorpusError("Scenario directory identity differs")
        if (
            _digest(_canonical({key: manifest[key] for key in CONFIG_KEYS}))
            != manifest["generator_config_sha256"]
        ):
            raise SyntheticCorpusError("Scenario configuration hash differs")
        identity = {
            key: manifest[key]
            for key in (
                "scenario_contract_version",
                "generator_config_sha256",
                "plan_sha256",
                "truth_sha256",
            )
        }
        if _digest(_canonical(identity)) != manifest["corpus_id"]:
            raise SyntheticCorpusError("Scenario corpus identity differs")
        for name, field in (
            ("plan.tsv", "plan_sha256"),
            ("truth.tsv", "truth_sha256"),
            ("labels.jsonl", "labels_sha256"),
        ):
            if _digest((path / name).read_bytes()) != manifest[field]:
                raise SyntheticCorpusError(f"Scenario artifact hash differs: {name}")
        raw_plan = _tsv(path / "plan.tsv", PLAN_COLUMNS)
        raw_truth = _tsv(path / "truth.tsv", TRUTH_COLUMNS)
        if len(raw_plan) != len(raw_truth) or len(raw_plan) != int(
            manifest["generated_transaction_count"]
        ):
            raise SyntheticCorpusError("Scenario row counts differ")
        plan: list[PlannedEvent] = []
        truth: list[ScenarioTruth] = []
        for left, right in zip(raw_plan, raw_truth, strict=True):
            event_id = left["event_id"]
            if str(UUID(event_id)) != event_id or right["event_id"] != event_id:
                raise SyntheticCorpusError("Scenario event identity differs")
            event_time = parse_utc_timestamp(left["event_time"])
            ingestion = parse_utc_timestamp(left["ingestion_time"])
            if ingestion < event_time:
                raise SyntheticCorpusError(f"Ingestion precedes event time: {event_id}")
            amount = Decimal(left["amount"])
            if amount <= 0 or left["currency"] not in {"USD", "EUR"}:
                raise SyntheticCorpusError(f"Invalid scenario monetary input: {event_id}")
            wave = int(left["wave"])
            if wave not in (0, 1, 2):
                raise SyntheticCorpusError("Invalid scenario wave")
            plan.append(
                PlannedEvent(
                    event_id,
                    left["customer_id"],
                    event_time,
                    ingestion,
                    amount,
                    left["currency"],
                    wave,
                )
            )
            if right["seed"] != manifest["seed"] or right["scenario_contract_version"] != VERSION:
                raise SyntheticCorpusError("Private truth has wrong scenario identity")
            first = parse_utc_timestamp(right["first_observed_at"])
            revision = (
                parse_utc_timestamp(right["correction_observed_at"])
                if right["correction_observed_at"]
                else None
            )
            if first < ingestion or (revision is not None and revision < first):
                raise SyntheticCorpusError(f"Label chronology invalid: {event_id}")
            if right["outcome"] not in {"FRAUD", "LEGIT"} or right["initial_outcome"] not in {
                "FRAUD",
                "LEGIT",
            }:
                raise SyntheticCorpusError(f"Invalid synthetic outcome: {event_id}")
            truth.append(
                ScenarioTruth(
                    event_id,
                    right["latent_profile"],
                    right["outcome"],
                    right["initial_outcome"],
                    first,
                    revision,
                )
            )
        if len({item.event_id for item in plan}) != len(plan):
            raise SyntheticCorpusError("Duplicate scenario event_id")
        if [item.ingestion_time for item in plan] != sorted(item.ingestion_time for item in plan):
            raise SyntheticCorpusError("Scenario ingestion order differs")
        if any(item.wave != index * 3 // len(plan) for index, item in enumerate(plan)):
            raise SyntheticCorpusError("Scenario wave assignments differ")
        labels = load_labels(path / "labels.jsonl")
        by_id: dict[str, list[tuple[int, str, datetime]]] = {}
        for label in labels.records:
            if label.label_source != LABEL_SOURCE:
                raise SyntheticCorpusError("Label source is not synthetic scenario provenance")
            by_id.setdefault(str(label.event_id), []).append(
                (label.label_revision, label.outcome.value, label.label_observed_at)
            )
        if set(by_id) != {item.event_id for item in plan}:
            raise SyntheticCorpusError("Label event IDs differ from the transaction plan")
        for item in truth:
            expected = [(1, item.initial_outcome, item.first_observed_at)]
            if item.correction_observed_at is not None:
                expected.append((2, item.outcome, item.correction_observed_at))
            if sorted(by_id[item.event_id]) != expected:
                raise SyntheticCorpusError(f"Labels differ from private truth: {item.event_id}")
        if len(labels.records) != int(manifest["label_record_count"]):
            raise SyntheticCorpusError("Label record count differs")
        if sum(item.outcome == "FRAUD" for item in truth) != int(manifest["fraud_count"]):
            raise SyntheticCorpusError("Latent outcome count differs")
        as_of = parse_utc_timestamp(manifest["label_as_of_time"])
        if len(resolve_labels(labels, as_of)) != int(manifest["first_label_available_by_as_of"]):
            raise SyntheticCorpusError("Eligible label count differs")
        return ScenarioCorpus(path.resolve(), manifest, tuple(plan), tuple(truth), labels)
    except (KeyError, TypeError, OSError, ValueError) as error:
        if isinstance(error, SyntheticCorpusError):
            raise
        raise SyntheticCorpusError(f"Cannot inspect scenario corpus: {error}") from error
