"""Reconstruct incremental Delta and Kafka progress from immutable evidence."""

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import (
    AvailabilityError,
    digest,
    exact_directory,
    publish,
    read_object,
)
from sentinelaegisforge_control_plane.availability.discovery import discover_versions
from sentinelaegisforge_control_plane.availability.kafka_export import inspect_batch, prepare_batch
from sentinelaegisforge_control_plane.availability.outbox import (
    build_outbox,
    build_segments,
    canonical,
    inspect_outbox,
    inspect_segment,
)
from sentinelaegisforge_control_plane.datasets.feature_source import StatisticalFeatureSource

CONTRACT = "incremental-shadow-operations-v1"
RESULT_CONTRACT = "incremental-shadow-result-v1"
GROUP = "sentinel-feature-ready-incremental-shadow-v1"
FENCE_CONTRACT = "feature-ready-operational-fence-v1"
ACCEPTANCE_CONTRACT = "feature-ready-operational-acceptance-v1"
TOPIC = "features.ready"


def logical_request_id(ready_id: str, candidate_id: str, model_hash: str) -> str:
    return digest([RESULT_CONTRACT, ready_id, candidate_id, model_hash])


@dataclass(frozen=True)
class Limits:
    maximum_undiscovered_versions: int = 8
    maximum_unpublished_ready_events: int = 500
    maximum_accepted_unprocessed_events: int = 200
    maximum_active_publication_batches: int = 1
    maximum_inference_events_per_tick: int = 50
    maximum_cached_source_rows: int = 5000

    def __post_init__(self) -> None:
        for name, value in vars(self).items():
            if type(value) is not int or not 1 <= value <= 5000:
                raise AvailabilityError(f"Invalid local backpressure bound: {name}")
        if self.maximum_active_publication_batches != 1:
            raise AvailabilityError("Only one active publication batch is supported")


def _fence(path: Path) -> dict[str, object]:
    value = read_object(path)
    if (
        value.get("contract_version") != FENCE_CONTRACT
        or value.get("topic") != TOPIC
        or value.get("group_id") != GROUP
        or not all(
            isinstance(value.get(name), str) and value[name] for name in ("cluster_id", "topic_id")
        )
        or set(value)
        != {"contract_version", "group_id", "cluster_id", "topic_id", "topic"}
        | {f"{prefix}_{part}" for prefix in ("beginning", "end") for part in range(3)}
    ):
        raise AvailabilityError("Invalid operational Kafka starting fence")
    for part in range(3):
        beginning = value[f"beginning_{part}"]
        end = value[f"end_{part}"]
        if type(beginning) is not int or type(end) is not int or not 0 <= beginning <= end:
            raise AvailabilityError("Invalid operational Kafka offset fence")
    return value


def initialize(
    source: Path,
    root: Path,
    fence_path: Path,
    first_version: int,
    candidate_id: str,
    model_tree_sha256: str,
    limits: Limits,
) -> dict[str, object]:
    """Bind a fresh source and stable Kafka fence before any incremental discovery."""
    if type(first_version) is not int or first_version < 0:
        raise AvailabilityError("First Delta version must be explicit and nonnegative")
    snapshot = StatisticalFeatureSource(source, first_version).read()
    fence = _fence(fence_path)
    if not all(
        len(value) == 64 and all(c in "0123456789abcdef" for c in value)
        for value in (candidate_id, model_tree_sha256)
    ):
        raise AvailabilityError("Invalid trusted candidate identity")
    basis: dict[str, object] = {
        "contract_version": CONTRACT,
        "source_delta_path": str(source.resolve()),
        "source_delta_table_id": snapshot.delta_table_id,
        "source_schema_fingerprint": snapshot.source_schema_fingerprint,
        "first_delta_version": first_version,
        "topic": TOPIC,
        "cluster_id": fence["cluster_id"],
        "topic_id": fence["topic_id"],
        "group_id": GROUP,
        "candidate_id": candidate_id,
        "model_tree_sha256": model_tree_sha256,
        "logic_version": "bounded-sequential-v1",
    }
    manifest = {
        **basis,
        "stream_id": digest(basis),
        "fence_path": str(fence_path.resolve()),
        "fence_fingerprint": digest(fence),
        "limits": vars(limits),
    }
    target = root.resolve() / cast(str, manifest["stream_id"])
    if target.exists():
        if inspect(target) != manifest:
            raise AvailabilityError("Existing operational stream identity conflicts")
    else:
        publish(target / "identity", {"stream_manifest.json": canonical(manifest)})
    return inspect(target)


def inspect(root: Path) -> dict[str, object]:
    exact_directory(root / "identity", {"stream_manifest.json"})
    manifest = read_object(root / "identity" / "stream_manifest.json")
    basis = {
        key: value
        for key, value in manifest.items()
        if key not in {"stream_id", "fence_path", "fence_fingerprint", "limits"}
    }
    if (
        manifest.get("contract_version") != CONTRACT
        or manifest.get("group_id") != GROUP
        or manifest.get("topic") != TOPIC
        or digest(basis) != manifest.get("stream_id")
        or root.name != manifest["stream_id"]
        or digest(_fence(Path(cast(str, manifest["fence_path"]))))
        != manifest.get("fence_fingerprint")
    ):
        raise AvailabilityError("Operational stream identity or Kafka fence changed")
    Limits(**cast(dict[str, int], manifest["limits"]))
    source = Path(cast(str, manifest["source_delta_path"]))
    first = cast(int, manifest["first_delta_version"])
    snapshot = StatisticalFeatureSource(source, first).read()
    if (
        snapshot.delta_table_id != manifest["source_delta_table_id"]
        or snapshot.source_schema_fingerprint != manifest["source_schema_fingerprint"]
    ):
        raise AvailabilityError("Operational Delta source identity changed")
    return manifest


def records(root: Path) -> list[dict[str, object]]:
    manifest = inspect(root)
    directory = root / "versions"
    paths = (
        sorted(directory.glob("*.json"), key=lambda path: int(path.stem))
        if directory.exists()
        else []
    )
    first = cast(int, manifest["first_delta_version"])
    if [int(path.stem) for path in paths] != list(range(first, first + len(paths))):
        raise AvailabilityError("Incremental discovery record has a version gap")
    result: list[dict[str, object]] = []
    all_ids: set[str] = set()
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise AvailabilityError("Linked or non-file operational discovery record")
        record = read_object(path)
        version = int(path.stem)
        if (
            set(record)
            != {
                "contract_version",
                "delta_version",
                "segment_id",
                "segment_path",
                "ready_event_count",
                "publication_batch_id",
                "batch_path",
            }
            or record.get("contract_version") != CONTRACT
            or record.get("delta_version") != version
        ):
            raise AvailabilityError("Invalid operational discovery record")
        segment_path = Path(cast(str, record["segment_path"]))
        segment, ready = inspect_segment(segment_path)
        if (
            segment["source_delta_table_id"] != manifest["source_delta_table_id"]
            or segment["source_schema_fingerprint"] != manifest["source_schema_fingerprint"]
            or segment["source_delta_version"] != version
            or segment["segment_id"] != record["segment_id"]
            or len(ready) != record["ready_event_count"]
        ):
            raise AvailabilityError("Operational segment differs from pinned source")
        new_ids = {cast(str, event["ready_event_id"]) for event in ready}
        if new_ids & all_ids:
            raise AvailabilityError("Cross-batch duplicate ready-event identity")
        all_ids.update(new_ids)
        if ready:
            batch = inspect_batch(Path(cast(str, record["batch_path"])))
            outbox, outbox_ready = inspect_outbox(Path(cast(str, batch["outbox_path"])))
            if (
                batch["publication_batch_id"] != record["publication_batch_id"]
                or outbox["segment_ids"] != [record["segment_id"]]
                or outbox_ready != ready
            ):
                raise AvailabilityError("Operational outbox/batch membership conflicts")
        elif record["batch_path"] is not None or record["publication_batch_id"] is not None:
            raise AvailabilityError("Empty commit must not create a publication batch")
        result.append(record)
    return result


def discover_next(root: Path) -> dict[str, object] | None:
    manifest = inspect(root)
    prior = records(root)
    version = cast(int, manifest["first_delta_version"]) + len(prior)
    source = Path(cast(str, manifest["source_delta_path"]))
    latest = DeltaTable(source).version()
    limits = Limits(**cast(dict[str, int], manifest["limits"]))
    if latest < version:
        return None
    if latest - version + 1 > limits.maximum_undiscovered_versions:
        raise AvailabilityError("Undiscovered Delta version backlog exceeds local bound")
    differences = discover_versions(source, version, version)
    if differences[0].current.delta_table_id != manifest["source_delta_table_id"]:
        raise AvailabilityError("Delta table ID changed during incremental discovery")
    segment_path = build_segments(source, version, version, root / "segments")[0]
    segment, ready = inspect_segment(segment_path)
    batch_path: Path | None = None
    batch_id: str | None = None
    if ready:
        outbox = build_outbox([segment_path], root / "outboxes")
        batch_path = prepare_batch(outbox, root / "batches", maximum=5000)
        batch_id = cast(str, inspect_batch(batch_path)["publication_batch_id"])
    record: dict[str, object] = {
        "contract_version": CONTRACT,
        "delta_version": version,
        "segment_id": segment["segment_id"],
        "segment_path": str(segment_path.resolve()),
        "ready_event_count": len(ready),
        "publication_batch_id": batch_id,
        "batch_path": str(batch_path.resolve()) if batch_path else None,
    }
    path = root / "versions" / f"{version}.json"
    if path.exists():
        if read_object(path) != record:
            raise AvailabilityError("Existing discovery record conflicts")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as output:
                output.write(canonical(record))
        except FileExistsError as error:
            raise AvailabilityError("Concurrent discovery record claim") from error
    records(root)
    return record
