"""Read-only exact-set checks over the Scala Delta stages and research handoff."""

from pathlib import Path
from typing import cast

import pyarrow.compute as pc
from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import read_object
from sentinelaegisforge_control_plane.availability.outbox import inspect_segment
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.incremental_shadow.operations import (
    inspect as inspect_stream,
)
from sentinelaegisforge_control_plane.incremental_shadow.operations import (
    records,
)
from sentinelaegisforge_control_plane.incremental_shadow.progress import (
    progress,
    reconcile,
)

from .contract import STAGES, IntegrationError, inspect_identity, read_plan


def _publication(corpus: Path, wave: int, expected: int) -> set[tuple[int, int]]:
    report = corpus / f"publish-report-wave-{wave}.tsv"
    attempt = corpus / f"publish-attempt-wave-{wave}.tsv"
    if not report.is_file() or not attempt.is_file():
        raise IntegrationError(f"Wave {wave} has no completed publication evidence")
    lines = [line.split("\t") for line in report.read_text(encoding="utf-8").splitlines()]
    header = [["requested", str(expected)], ["acknowledged", str(expected)], ["failed", "0"]]
    if lines[:3] != header:
        raise IntegrationError(f"Wave {wave} acknowledgements are incomplete")
    if len(lines[3:]) != expected:
        raise IntegrationError(f"Wave {wave} acknowledgement detail count differs")
    coordinates: set[tuple[int, int]] = set()
    for row in lines[3:]:
        if len(row) != 4 or row[:2] != ["ack", "transactions.raw"]:
            raise IntegrationError(f"Wave {wave} has invalid Kafka acknowledgement")
        part, offset = int(row[2]), int(row[3])
        if part not in range(3) or offset < 0 or (part, offset) in coordinates:
            raise IntegrationError(f"Wave {wave} has repeated/invalid Kafka coordinate")
        coordinates.add((part, offset))
    return coordinates


def _stage(path: Path, columns: list[str], version: int | None = None) -> dict[str, object]:
    try:
        table = DeltaTable(path, version=version)
        rows = cast(list[dict[str, object]], table.to_pyarrow_table(columns=columns).to_pylist())
        ids = [cast(str, row["event_id"]) for row in rows]
        if len(ids) != len(set(ids)):
            raise IntegrationError(f"Duplicate durable event ID in {path}")
        return {
            "table_id": str(table.metadata().id),
            "version": table.version(),
            "rows": rows,
            "event_ids": set(ids),
        }
    except IntegrationError:
        raise
    except Exception as error:
        raise IntegrationError(f"Cannot inspect committed Delta stage {path}: {error}") from error


def verify_wave(path: Path, wave: int) -> dict[str, object]:
    """Require cumulative exact IDs at each committed boundary after a wave."""
    if wave not in range(3):
        raise IntegrationError("Wave must be 0, 1 or 2")
    identity = inspect_identity(path)
    corpus = Path(cast(str, identity["corpus_path"]))
    plan = read_plan(corpus)
    saved_path = path / "waves" / f"{wave}.json"
    saved = read_object(saved_path) if saved_path.is_file() else None
    expected = frozenset().union(*(plan.wave_ids(index) for index in range(wave + 1)))
    coordinates: set[tuple[int, int]] = set()
    for index in range(wave + 1):
        next_coordinates = _publication(corpus, index, len(plan.wave_ids(index)))
        if coordinates & next_coordinates:
            raise IntegrationError("Kafka coordinates repeated across source waves")
        coordinates.update(next_coordinates)
    stages: dict[str, dict[str, object]] = {}
    for name in STAGES:
        columns = ["event_id"]
        if name == "validated":
            columns += ["kafka_partition", "kafka_offset"]
        if name in {"rolling", "statistical"}:
            columns += ["customer_id", "currency"]
        if name == "rolling":
            columns += ["prior_transaction_count_5m", "prior_amount_sum_10m"]
        if name == "statistical":
            columns += [
                "prior_amount_observation_count",
                "prior_amount_mean",
                "statistical_feature_status",
            ]
        saved_stages = cast(dict[str, dict[str, object]], saved["stages"]) if saved else {}
        pinned = cast(int, saved_stages[name]["version"]) if saved else None
        stage = _stage(Path(cast(dict[str, str], identity["paths"])[name]), columns, pinned)
        if stage["event_ids"] != expected:
            actual_ids = cast(set[str], stage["event_ids"])
            raise IntegrationError(
                f"{name} exact event-ID mismatch: missing={len(expected - actual_ids)} "
                f"unexpected={len(actual_ids - expected)}"
            )
        stages[name] = stage
    observed = {
        (cast(int, row["kafka_partition"]), cast(int, row["kafka_offset"]))
        for row in cast(list[dict[str, object]], stages["validated"]["rows"])
    }
    if observed != coordinates or len(observed) != len(expected):
        raise IntegrationError("Validated Kafka coordinates differ from wave acknowledgements")
    starting = cast(list[int], identity["starting_offsets"])
    if any(offset < starting[part] for part, offset in observed):
        raise IntegrationError("Validated source crossed the transaction starting fence")
    statistical_path = Path(cast(dict[str, str], identity["paths"])["statistical"])
    snapshot = StatisticalFeatureSource(
        statistical_path, cast(int, stages["statistical"]["version"])
    ).read()
    if snapshot.row_count != len(expected):
        raise IntegrationError("Statistical 25-column source failed row reconciliation")
    historical = path / "waves" / f"{wave - 1}.json"
    if wave and historical.is_file():
        prior = read_object(historical)
        prior_stages = cast(dict[str, dict[str, object]], prior["stages"])
        for name in STAGES:
            if prior_stages[name]["table_id"] != stages[name]["table_id"]:
                raise IntegrationError(f"{name} Delta table identity changed across waves")
            if cast(int, prior_stages[name]["version"]) >= cast(int, stages[name]["version"]):
                raise IntegrationError(f"{name} Delta did not advance after wave {wave}")
    return {
        "contract_version": "stateful-shadow-wave-reconciliation-v1",
        "integration_id": identity["integration_id"],
        "wave": wave,
        "wave_event_count": len(plan.wave_ids(wave)),
        "cumulative_event_count": len(expected),
        "source_kafka_coordinates": len(observed),
        "stages": {
            name: {
                "table_id": stage["table_id"],
                "version": stage["version"],
                "rows": len(cast(list[dict[str, object]], stage["rows"])),
            }
            for name, stage in stages.items()
        },
        "statistical_schema_fingerprint": snapshot.source_schema_fingerprint,
    }


def verify_final(path: Path, stream: Path) -> dict[str, object]:
    identity = inspect_identity(path)
    stream_identity = inspect_stream(stream)
    if (
        stream_identity["source_delta_path"]
        != cast(dict[str, str], identity["paths"])["statistical"]
        or stream_identity["group_id"] != identity["group_id"]
        or stream_identity["candidate_id"] != identity["candidate_id"]
    ):
        raise IntegrationError("Final research stream is bound to another source/group/candidate")
    final = verify_wave(path, 2)
    if any(not (path / "waves" / f"{wave}.json").is_file() for wave in range(3)):
        raise IntegrationError("Missing completed wave evidence")
    for wave in range(3):
        if verify_wave(path, wave) != read_object(path / "waves" / f"{wave}.json"):
            raise IntegrationError("Pinned historical wave evidence differs")
    discovered = records(stream)
    ready: dict[str, dict[str, object]] = {}
    for record in discovered:
        _, events = inspect_segment(Path(cast(str, record["segment_path"])))
        for event in events:
            event_id = cast(str, event["event_id"])
            if event_id in ready:
                raise IntegrationError("Event has multiple logical ready identities")
            ready[event_id] = event
    expected = {row["event_id"] for row in read_plan(Path(cast(str, identity["corpus_path"]))).rows}
    if set(ready) != expected:
        raise IntegrationError("Committed feature and ready-event membership differs")
    research = reconcile(stream)
    if research["unique_source_ready_events"] != len(expected):
        raise IntegrationError("Research receipt/result membership differs")
    summary = progress(stream)
    return {
        "contract_version": "stateful-shadow-final-reconciliation-v1",
        "integration_id": identity["integration_id"],
        "corpus_id": identity["corpus_id"],
        "source_event_count": len(expected),
        "stages": final["stages"],
        "ready_event_count": len(ready),
        "research": research,
        "operational_progress": summary,
    }


def trace_event(path: Path, stream: Path, event_id: str) -> dict[str, object]:
    """Inspect one event without opening private synthetic truth or labels."""
    identity = inspect_identity(path)
    plan = read_plan(Path(cast(str, identity["corpus_path"])))
    planned = next((row for row in plan.rows if row["event_id"] == event_id), None)
    if planned is None:
        raise IntegrationError("Event is absent from the scenario transaction plan")
    wave = int(planned["wave"])
    wave_rows = [row for row in plan.rows if row["wave"] == str(wave)]
    index = wave_rows.index(planned)
    report = Path(cast(str, identity["corpus_path"])) / f"publish-report-wave-{wave}.tsv"
    coordinates = [line.split("\t") for line in report.read_text(encoding="utf-8").splitlines()][3:]
    if len(coordinates) != len(wave_rows):
        raise IntegrationError("Publisher acknowledgement report cannot trace event")
    stage_rows: dict[str, dict[str, object]] = {}
    for name in STAGES:
        table = DeltaTable(cast(dict[str, str], identity["paths"])[name])
        source = table.to_pyarrow_table()
        matching = cast(
            list[dict[str, object]],
            source.filter(pc.equal(source["event_id"], event_id)).to_pylist(),
        )
        if len(matching) != 1:
            raise IntegrationError(f"Expected exactly one {name} row for traced event")
        row = matching[0]
        stage_rows[name] = {
            "delta_table_id": str(table.metadata().id),
            "delta_version_inspected": table.version(),
            "event_id": row["event_id"],
            "customer_id": row["customer_id"],
            "currency": row["currency"],
            "kafka_partition": row["kafka_partition"],
            "kafka_offset": row["kafka_offset"],
        }
        if name == "rolling":
            stage_rows[name]["prior_transaction_count_5m"] = row["prior_transaction_count_5m"]
            stage_rows[name]["prior_amount_sum_10m"] = str(row["prior_amount_sum_10m"])
        if name == "statistical":
            for field in (
                "prior_amount_observation_count",
                "prior_amount_mean",
                "prior_amount_stddev",
                "amount_zscore",
                "statistical_feature_status",
            ):
                stage_rows[name][field] = row[field]
    ack = coordinates[index]
    if ack[:2] != ["ack", "transactions.raw"] or (int(ack[2]), int(ack[3])) != (
        stage_rows["validated"]["kafka_partition"],
        stage_rows["validated"]["kafka_offset"],
    ):
        raise IntegrationError("Traced Kafka acknowledgement differs from validated lineage")
    matching_ready: list[tuple[dict[str, object], dict[str, object]]] = []
    for record in records(stream):
        _, events = inspect_segment(Path(cast(str, record["segment_path"])))
        matching_ready.extend((record, ready) for ready in events if ready["event_id"] == event_id)
    if len(matching_ready) != 1:
        raise IntegrationError("Expected exactly one first-visible ready event")
    record, ready = matching_ready[0]
    ready_id = cast(str, ready["ready_event_id"])
    batch_id = cast(str, record["publication_batch_id"])
    ack_path = stream / "journals" / batch_id / "acknowledgements" / f"{ready_id}.json"
    receipt_path = stream / "receipts" / batch_id / "receipts" / f"{ready_id}.json"
    results = list((stream / "results" / cast(str, identity["candidate_id"])).glob("*.json"))
    matched_result = []
    for item in results:
        result = read_object(item)
        if result.get("ready_event_id") == ready_id:
            matched_result.append(result)
    if len(matched_result) != 1:
        raise IntegrationError("Expected exactly one durable inference result")
    return {
        "integration_id": identity["integration_id"],
        "event_id": event_id,
        "wave": wave,
        "plan": {
            key: planned[key]
            for key in ("event_id", "customer_id", "currency", "event_time", "ingestion_time")
        },
        "transaction_kafka": {"topic": ack[1], "partition": int(ack[2]), "offset": int(ack[3])},
        "stages": stage_rows,
        "first_visible_delta_version": ready["first_visible_delta_version"],
        "ready_event_id": ready_id,
        "publication_batch_id": batch_id,
        "ready_kafka_acknowledgement": read_object(ack_path),
        "research_receipt": read_object(receipt_path),
        "inference_result": matched_result[0],
    }
