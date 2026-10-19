"""Immutable request plans over one verified, fixed corrected statistical Delta version."""

import json
from datetime import datetime
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.candidates.contract import INPUT_FINGERPRINT
from sentinelaegisforge_control_plane.datasets.dataset_builder import inspect_snapshot
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource
from sentinelaegisforge_control_plane.datasets.labels import parse_utc_timestamp, utc_text
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.monitoring.contract import (
    row_fingerprint,
    rows_fingerprint,
    utc_window,
)
from sentinelaegisforge_control_plane.registry.run import (
    RegistryConfig,
    _source_path,
    inspect_registered_candidate,
)
from sentinelaegisforge_control_plane.training.run import inspect_run

from .contract import (
    AVAILABILITY,
    PLAN_VERSION,
    SCOPE,
    ShadowError,
    digest,
    exact_directory,
    file_hash,
    read_object,
)

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_PLAN_ROOT = ROOT / ".local/ml/shadow-replay-plans"
PLAN_FILES = {"manifest.json", "requests.jsonl"}
MAX_REQUESTS = 5000


def _source(
    registry: RegistryConfig, version: str, path: Path, source_version: int
) -> tuple[dict[str, object], list[dict[str, object]], tuple[str, ...]]:
    if type(source_version) is not int or source_version < 0:
        raise ShadowError("Explicit nonnegative Delta version is required")
    registration = inspect_registered_candidate(registry, version)
    if (
        registration["production_eligible"] is not False
        or registration["governance_state"] != "REGISTERED_RESEARCH_ONLY"
    ):
        raise ShadowError("Only an explicitly research-only Registry version is permitted")
    candidate = _source_path(cast(str, registration["source_uri"])).parent
    candidate_manifest = read_object(candidate / "candidate_manifest.json")
    run = inspect_run(Path(cast(str, candidate_manifest["source_training_path"])))
    snapshot = inspect_snapshot(Path(cast(str, run["dataset_snapshot_path"])))
    feature_source = StatisticalFeatureSource(path, source_version).read()
    if (
        str(feature_source.delta_path) != snapshot["source_delta_path"]
        or feature_source.delta_table_id != snapshot["source_delta_table_id"]
        or feature_source.delta_version != snapshot["source_delta_version"]
        or feature_source.source_schema_fingerprint != snapshot["source_schema_fingerprint"]
        or candidate_manifest["input_schema_fingerprint"] != INPUT_FINGERPRINT
        or candidate_manifest["candidate_id"] != registration["candidate_id"]
    ):
        raise ShadowError("Fixed feature source/candidate contract differs from training lineage")
    rows = cast(list[dict[str, object]], feature_source.table.to_pylist())
    fields: dict[str, object] = {
        "candidate_id": registration["candidate_id"],
        "registered_model_name": registry.model_name,
        "registered_model_version": version,
        "model_tree_sha256": registration["model_tree_sha256"],
        "input_contract_fingerprint": INPUT_FINGERPRINT,
        "source_delta_path": str(feature_source.delta_path),
        "source_delta_table_id": feature_source.delta_table_id,
        "source_delta_version": feature_source.delta_version,
        "source_schema_fingerprint": feature_source.source_schema_fingerprint,
    }
    return fields, rows, tuple(feature_source.table.column_names)


def _requests(
    plan_id: str, selected: list[dict[str, object]], columns: tuple[str, ...]
) -> list[dict[str, object]]:
    result = [
        {
            "request_id": digest([PLAN_VERSION, plan_id, row["event_id"]]),
            "event_id": row["event_id"],
            "replay_sequence": index,
            "source_feature_row_fingerprint": row_fingerprint(row, columns),
        }
        for index, row in enumerate(selected)
    ]
    if len({row["event_id"] for row in result}) != len(result) or len(
        {row["request_id"] for row in result}
    ) != len(result):
        raise ShadowError("Duplicate logical request identity")
    return result


def _plan(
    registry: RegistryConfig,
    version: str,
    path: Path,
    source_version: int,
    start: datetime,
    end: datetime,
    maximum: int,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    start, end = utc_window(start, end)
    if type(maximum) is not int or not 1 <= maximum <= MAX_REQUESTS:
        raise ShadowError(f"Maximum request count must be in [1,{MAX_REQUESTS}]")
    source_fields, source_rows, columns = _source(registry, version, path, source_version)
    selected = sorted(
        (row for row in source_rows if start <= cast(datetime, row["ingestion_time"]) < end),
        key=lambda row: (cast(datetime, row["ingestion_time"]), cast(str, row["event_id"])),
    )
    if not selected or len(selected) > maximum:
        raise ShadowError("Replay window is empty or exceeds the explicit request limit")
    basis: dict[str, object] = {
        "plan_contract_version": PLAN_VERSION,
        **source_fields,
        "replay_window_start": utc_text(start),
        "replay_window_end": utc_text(end),
        "request_count": len(selected),
        "source_rows_fingerprint": rows_fingerprint(selected, columns),
        "execution_scope": SCOPE,
        "feature_availability_evidence": AVAILABILITY,
    }
    plan_id = digest(basis)
    requests = _requests(plan_id, selected, columns)
    manifest = {
        **basis,
        "plan_id": plan_id,
        "request_plan_fingerprint": digest(requests),
    }
    return manifest, requests, selected


def _lines(requests: list[dict[str, object]]) -> bytes:
    return b"".join(canonical_json(request) for request in requests)


def build_plan(
    registry: RegistryConfig,
    version: str,
    source_delta: Path,
    source_version: int,
    start: datetime,
    end: datetime,
    output_root: Path = DEFAULT_PLAN_ROOT,
    *,
    maximum: int = MAX_REQUESTS,
) -> Path:
    manifest, requests, _ = _plan(
        registry, version, source_delta, source_version, start, end, maximum
    )
    target = output_root.resolve() / cast(str, manifest["plan_id"])
    if target.exists():
        existing, _ = inspect_plan(target, registry)
        if existing != {**manifest, "requests_sha256": file_hash(target / "requests.jsonl")} or (
            target / "requests.jsonl"
        ).read_bytes() != _lines(requests):
            raise ShadowError("Existing replay plan conflicts with requested source")
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir()  # Exclusive claim; interrupted publication stays visibly incomplete.
    raw = _lines(requests)
    (target / "requests.jsonl").write_bytes(raw)
    (target / "manifest.json").write_bytes(
        canonical_json({**manifest, "requests_sha256": file_hash(target / "requests.jsonl")})
    )
    inspect_plan(target, registry)
    return target


def inspect_plan(
    path: Path, registry: RegistryConfig
) -> tuple[dict[str, object], list[dict[str, object]]]:
    exact_directory(path, PLAN_FILES)
    manifest = read_object(path / "manifest.json")
    try:
        if (
            manifest["registered_model_name"] != registry.model_name
            or manifest["plan_contract_version"] != PLAN_VERSION
        ):
            raise ShadowError("Plan Registry/contract identity differs")
        raw = (path / "requests.jsonl").read_bytes()
        requests = [json.loads(line) for line in raw.splitlines()]
        if (
            raw != _lines(requests)
            or file_hash(path / "requests.jsonl") != manifest["requests_sha256"]
        ):
            raise ShadowError("Plan request bytes differ")
        expected, expected_requests, _ = _plan(
            registry,
            cast(str, manifest["registered_model_version"]),
            Path(cast(str, manifest["source_delta_path"])),
            cast(int, manifest["source_delta_version"]),
            parse_utc_timestamp(cast(str, manifest["replay_window_start"])),
            parse_utc_timestamp(cast(str, manifest["replay_window_end"])),
            MAX_REQUESTS,
        )
        if (
            path.name != expected["plan_id"]
            or manifest != {**expected, "requests_sha256": file_hash(path / "requests.jsonl")}
            or requests != expected_requests
        ):
            raise ShadowError("Replay plan no longer matches verified fixed source")
        return manifest, requests
    except ShadowError:
        raise
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise ShadowError(f"Invalid replay plan: {error}") from error
