"""Bounded restartable inference over independently accepted operational batches."""

import os
import tempfile
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from sentinelaegisforge_control_plane.availability.contract import AvailabilityError, read_object
from sentinelaegisforge_control_plane.availability.discovery import discover_versions
from sentinelaegisforge_control_plane.candidates.contract import FEATURES
from sentinelaegisforge_control_plane.candidates.run import load_verified_candidate, score_candidate
from sentinelaegisforge_control_plane.datasets.labels import utc_text
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.kafka_shadow.worker import evaluate_one
from sentinelaegisforge_control_plane.registry.run import (
    RegistryConfig,
    _source_path,
    inspect_registered_candidate,
)
from sentinelaegisforge_control_plane.training.baseline import probability_estimates
from sentinelaegisforge_control_plane.training.run import inspect_run

from .operations import RESULT_CONTRACT, inspect, logical_request_id, records
from .progress import _results, inspect_acceptance, progress

Scorer = Callable[[dict[str, object]], float]


def resolved_work(
    root: Path, maximum_cached_rows: int
) -> list[tuple[dict[str, object], dict[str, object], str]]:
    """Resolve only accepted notifications against exact retained first-visible versions."""
    manifest = inspect(root)
    source = Path(cast(str, manifest["source_delta_path"]))
    fence = read_object(Path(cast(str, manifest["fence_path"])))
    previous = {part: cast(int, fence[f"end_{part}"]) for part in range(3)}
    result: list[tuple[dict[str, object], dict[str, object], str]] = []
    for record in records(root):
        if record["batch_path"] is None:
            continue
        batch_id = cast(str, record["publication_batch_id"])
        if not (root / "acceptance" / f"{batch_id}.json").exists():
            break
        acceptance, events = inspect_acceptance(root, record, manifest, previous)
        previous = {part: cast(int, acceptance[f"end_{part}"]) for part in range(3)}
        version = cast(int, record["delta_version"])
        diff = discover_versions(source, version, version)[0]
        if diff.current.row_count > maximum_cached_rows:
            raise AvailabilityError("Pinned source snapshot exceeds explicit cache bound")
        rows = {
            cast(str, row["event_id"]): row
            for row in cast(list[dict[str, object]], diff.current.table.to_pylist())
        }
        if {event["event_id"] for event in events} != set(diff.new_rows):
            raise AvailabilityError(
                "Accepted notification membership differs from pinned Delta commit"
            )
        for event in events:
            event_id = cast(str, event["event_id"])
            if (
                event["first_visible_delta_version"] != version
                or event["source_feature_row_fingerprint"] != diff.new_rows[event_id]
                or event["source_delta_table_id"] != manifest["source_delta_table_id"]
                or event["source_schema_fingerprint"] != manifest["source_schema_fingerprint"]
            ):
                raise AvailabilityError("Accepted notification/source-row provenance differs")
            result.append((event, rows[event_id], batch_id))
    return sorted(
        result, key=lambda item: (item[0]["first_visible_delta_version"], item[0]["event_id"])
    )


def _claim(path: Path, value: dict[str, object]) -> bool:
    """Never overwrite a completed logical result, including after worker restart."""
    raw = canonical_json(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        from sentinelaegisforge_control_plane.availability.contract import read_object

        if read_object(path) != value:
            raise AvailabilityError("Conflicting durable incremental result")
        return False
    descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            from sentinelaegisforge_control_plane.availability.contract import read_object

            if read_object(path) != value:
                raise AvailabilityError(
                    "Concurrent result conflicts with logical request"
                ) from None
            return False
        return True
    finally:
        os.unlink(temporary)


def process_resolved(
    root: Path,
    resolved: list[tuple[dict[str, object], dict[str, object], str]],
    start: datetime,
    end: datetime,
    scorer: Scorer,
    maximum: int,
    *,
    fail_after_result: int = -1,
) -> dict[str, object]:
    manifest = inspect(root)
    if type(maximum) is not int or not 1 <= maximum <= 5000 or not start < end:
        raise AvailabilityError("Invalid bounded inference tick or scoring window")
    existing = _results(root, manifest)
    expected = {cast(str, event["ready_event_id"]) for event, _, _ in resolved}
    if set(existing) - expected:
        raise AvailabilityError("Durable result not in accepted work queue")
    created = 0
    attempts = 0
    for event, row, batch_id in resolved:
        ready_id = cast(str, event["ready_event_id"])
        request = logical_request_id(
            ready_id,
            cast(str, manifest["candidate_id"]),
            cast(str, manifest["model_tree_sha256"]),
        )
        base: dict[str, object] = {
            "contract_version": RESULT_CONTRACT,
            "stream_id": manifest["stream_id"],
            "request_id": request,
            "ready_event_id": ready_id,
            "event_id": event["event_id"],
            "publication_batch_id": batch_id,
            "candidate_id": manifest["candidate_id"],
            "model_tree_sha256": manifest["model_tree_sha256"],
            "first_visible_delta_version": event["first_visible_delta_version"],
            "source_feature_row_fingerprint": event["source_feature_row_fingerprint"],
            "score_start": utc_text(start),
            "score_end": utc_text(end),
        }
        if ready_id in existing:
            if any(existing[ready_id].get(key) != value for key, value in base.items()):
                raise AvailabilityError("Existing logical result/source identity conflicts")
            continue
        if attempts >= maximum:
            break
        outcome = evaluate_one({}, row, scorer, start, end)
        result = {
            **base,
            "execution_status": outcome["execution_status"],
            "fraud_probability_estimate": outcome["fraud_probability_estimate"],
            "failure_code": outcome["failure_code"],
        }
        path = root / "results" / cast(str, manifest["candidate_id"]) / f"{request}.json"
        if _claim(path, result):
            created += 1
        attempts += 1
        if created == fail_after_result:
            raise AvailabilityError("Injected interruption after durable operational result")
    return {"attempted": attempts, "new_results": created, "progress": progress(root)}


def process(
    root: Path,
    registry: RegistryConfig,
    model_version: str,
    start: datetime,
    end: datetime,
    *,
    maximum: int,
    fail_after_result: int = -1,
) -> dict[str, object]:
    """Verify one exact research candidate, load once and drain a bounded accepted prefix."""
    tick_start = time.perf_counter()
    manifest = inspect(root)
    limits = cast(dict[str, int], manifest["limits"])
    if maximum > limits["maximum_inference_events_per_tick"]:
        raise AvailabilityError("Inference tick exceeds configured work bound")
    resolved = resolved_work(root, limits["maximum_cached_source_rows"])
    registration = inspect_registered_candidate(registry, model_version)
    if (
        registration["candidate_id"] != manifest["candidate_id"]
        or registration["model_tree_sha256"] != manifest["model_tree_sha256"]
        or registration["production_eligible"] is not False
        or registration["governance_state"] != "REGISTERED_RESEARCH_ONLY"
    ):
        raise AvailabilityError("Operational worker may load only pinned research-only candidate")
    candidate_dir = _source_path(cast(str, registration["source_uri"])).parent
    load_start = time.perf_counter()
    candidate, model = load_verified_candidate(candidate_dir, tracking=registry.tracking)
    load_seconds = time.perf_counter() - load_start
    training = inspect_run(Path(cast(str, candidate["source_training_path"])))
    if candidate["candidate_id"] != manifest["candidate_id"] or cast(
        dict[str, object], training["split_boundaries"]
    )["train_end"] != utc_text(start):
        raise AvailabilityError("Candidate identity or original training cutoff differs")

    def scorer(projection: dict[str, object]) -> float:
        return float(probability_estimates(model, (projection,), FEATURES)[0])

    result = process_resolved(
        root, resolved, start, end, scorer, maximum, fail_after_result=fail_after_result
    )
    observation = {
        "observation_contract_version": "incremental-shadow-timing-v1",
        "stage": "inference_tick",
        "stream_id": manifest["stream_id"],
        "model_loads": 1,
        "model_load_seconds": load_seconds,
        "work_tick_seconds": time.perf_counter() - tick_start,
        "attempted": result["attempted"],
        "new_results": result["new_results"],
    }
    observation_path = root / "observations" / f"{uuid4()}.json"
    _claim(observation_path, observation)
    return result


def verify_reference(
    root: Path, candidate_dir: Path, registry: RegistryConfig
) -> dict[str, object]:
    """Compare completed results with separate trusted offline scoring of pinned rows."""
    manifest = inspect(root)
    resolved = resolved_work(
        root, cast(dict[str, int], manifest["limits"])["maximum_cached_source_rows"]
    )
    results = _results(root, manifest)
    scored = [
        (event, row)
        for event, row, _ in resolved
        if results.get(cast(str, event["ready_event_id"]), {}).get("execution_status") == "SCORED"
    ]
    if not scored:
        raise AvailabilityError("No completed scored results for offline comparison")
    probabilities = score_candidate(
        candidate_dir,
        [{name: row[name] for name in FEATURES} for _, row in scored],
        tracking=registry.tracking,
    )
    differences = [
        (
            cast(str, event["event_id"]),
            abs(
                cast(
                    float,
                    results[cast(str, event["ready_event_id"])]["fraud_probability_estimate"],
                )
                - reference
            ),
        )
        for (event, _), reference in zip(scored, probabilities, strict=True)
    ]
    mismatches = [event_id for event_id, difference in differences if difference > 1e-12]
    if mismatches:
        raise AvailabilityError(f"Independent offline scoring differs for {len(mismatches)} events")
    return {
        "compared_scored_events": len(scored),
        "probability_mismatches": 0,
        "maximum_absolute_probability_difference": max(
            (difference for _, difference in differences), default=0.0
        ),
        "comparison_tolerance": 1e-12,
    }
