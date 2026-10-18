"""Small deterministic provider-like fixture over existing frozen prediction identities."""

import json
from datetime import timedelta
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq

from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp, utc_text
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.monitoring.contract import identity, sha256
from sentinelaegisforge_control_plane.monitoring.ledger import ROOT, inspect_ledger

from .contract import FeedbackError, parse_jsonl

FIXTURE_VERSION = "local-provider-feedback-fixture-v1"
DEFAULT_FIXTURE_ROOT = ROOT / ".local/ml/feedback-fixtures"


def _notice(
    event_id: str,
    case: int,
    revision: int,
    status: str,
    updated: str,
    received: str,
) -> dict[str, object]:
    return {
        "provider_id": "LOCAL_PROVIDER_FIXTURE",
        "provider_record_id": f"NOTICE-{case:03d}-{revision}",
        "provider_case_id": f"CASE-{case:03d}",
        "source_revision": revision,
        "event_id": event_id,
        "status": status,
        "source_updated_at": updated,
        "received_at": received,
    }


def build_local_fixture(prediction_ledger: Path, output_root: Path = DEFAULT_FIXTURE_ROOT) -> Path:
    """Outcomes are hand-assigned fixture statuses, never model/risk/Phase 14 truth."""
    manifest = inspect_ledger(prediction_ledger)
    rows = pq.read_table(
        prediction_ledger / "predictions.parquet", columns=["event_id"]
    ).to_pylist()
    if len(rows) < 9:
        raise FeedbackError("Local provider fixture needs nine existing scored event IDs")
    ids = [cast(str, row["event_id"]) for row in rows[:9]]
    score_end = parse_utc_timestamp(cast(str, manifest["score_end"]))
    t1, t2, t3 = (score_end + timedelta(days=offset) for offset in (1, 2, 3))
    early = utc_text(t1 - timedelta(hours=1))
    middle = utc_text(t2 - timedelta(hours=1))
    late = utc_text(t3 - timedelta(hours=1))
    first = [
        _notice(ids[0], 1, 1, "CONFIRMED_FRAUD", early, early),
        _notice(ids[1], 2, 1, "PENDING", early, early),
        _notice(ids[2], 3, 1, "CONFIRMED_LEGIT", early, early),
        _notice(ids[3], 4, 1, "CONFIRMED_FRAUD", early, early),
        _notice(ids[4], 5, 1, "CONFIRMED_LEGIT", early, early),
        _notice(ids[5], 6, 1, "PENDING", early, early),
        _notice(ids[6], 7, 1, "CONFIRMED_LEGIT", early, early),
        _notice(ids[7], 8, 1, "CONFIRMED_FRAUD", early, early),
    ]
    first.append(first[-1].copy())  # Idempotent delivery replay, not another case revision.
    second = [
        _notice(ids[0], 1, 2, "RETRACTED", middle, middle),
        _notice(ids[0], 1, 3, "CONFIRMED_LEGIT", late, late),
        _notice(ids[1], 2, 2, "CONFIRMED_FRAUD", middle, middle),
        _notice(ids[2], 3, 2, "CONFIRMED_FRAUD", middle, middle),
        _notice(ids[5], 6, 2, "CONFIRMED_LEGIT", late, late),
        _notice(ids[6], 7, 3, "CONFIRMED_FRAUD", utc_text(t1), middle),
        _notice(ids[6], 7, 2, "PENDING", utc_text(t1), late),
        _notice(ids[8], 9, 1, "CONFIRMED_FRAUD", late, late),
        first[-1].copy(),
    ]
    basis = {
        "fixture_contract_version": FIXTURE_VERSION,
        "prediction_ledger_id": manifest["ledger_id"],
        "selected_event_ids": ids,
        "t1": utc_text(t1),
        "t2": utc_text(t2),
        "t3": utc_text(t3),
    }
    fixture_id = identity(basis)
    contents = {
        "F1.jsonl": b"".join(canonical_json(item) for item in first),
        "F2.jsonl": b"".join(canonical_json(item) for item in second),
    }
    for raw in contents.values():
        parse_jsonl(raw)
    artifact_manifest = {
        **basis,
        "fixture_id": fixture_id,
        "fixture_source": (
            "LOCAL_PROVIDER_FIXTURE; hand-assigned synthetic states, not Phase 14 truth"
        ),
        "artifact_sha256": {name: sha256(raw) for name, raw in contents.items()},
    }
    root = output_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / fixture_id
    if target.exists():
        inspect_local_fixture(target)
        if {item.name for item in target.iterdir()} != {"F1.jsonl", "F2.jsonl", "manifest.json"}:
            raise FeedbackError("Existing local fixture is incomplete")
        if any((target / name).read_bytes() != raw for name, raw in contents.items()) or (
            target / "manifest.json"
        ).read_bytes() != canonical_json(artifact_manifest):
            raise FeedbackError("Existing local fixture conflicts with deterministic plan")
        return target
    target.mkdir()
    for name, raw in contents.items():
        with (target / name).open("xb") as stream:
            stream.write(raw)
    with (target / "manifest.json").open("xb") as stream:
        stream.write(canonical_json(artifact_manifest))
    return target


def inspect_local_fixture(path: Path) -> dict[str, object]:
    try:
        if path.is_symlink() or {item.name for item in path.iterdir()} != {
            "F1.jsonl",
            "F2.jsonl",
            "manifest.json",
        }:
            raise FeedbackError("Local provider fixture is incomplete")
        if any((path / name).is_symlink() for name in ("F1.jsonl", "F2.jsonl", "manifest.json")):
            raise FeedbackError("Local provider fixture artifacts must not be symlinks")
        value: object = json.loads((path / "manifest.json").read_bytes())
        if not isinstance(value, dict):
            raise FeedbackError("Fixture manifest must be an object")
        manifest = cast(dict[str, object], value)
        if (
            manifest["fixture_contract_version"] != FIXTURE_VERSION
            or path.name != manifest["fixture_id"]
        ):
            raise FeedbackError("Fixture version/identity differs")
        if (
            identity(
                {
                    key: manifest[key]
                    for key in (
                        "fixture_contract_version",
                        "prediction_ledger_id",
                        "selected_event_ids",
                        "t1",
                        "t2",
                        "t3",
                    )
                }
            )
            != path.name
        ):
            raise FeedbackError("Fixture identity differs")
        hashes = cast(dict[str, str], manifest["artifact_sha256"])
        for name in ("F1.jsonl", "F2.jsonl"):
            raw = (path / name).read_bytes()
            if sha256(raw) != hashes[name]:
                raise FeedbackError(f"Fixture byte hash differs: {name}")
            parse_jsonl(raw)
        return manifest
    except FeedbackError:
        raise
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise FeedbackError(f"Cannot inspect local provider fixture: {error}") from error
