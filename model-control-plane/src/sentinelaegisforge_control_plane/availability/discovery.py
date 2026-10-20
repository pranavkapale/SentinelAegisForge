"""Bounded read-only first-visible Delta-version discovery by validated snapshots."""

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from deltalake import DeltaTable
from deltalake.exceptions import DeltaError

from sentinelaegisforge_control_plane.datasets.errors import (
    FeatureDataError,
    FeatureSourceError,
    SourceSchemaError,
)
from sentinelaegisforge_control_plane.datasets.feature_source import (
    FeatureSnapshot,
    StatisticalFeatureSource,
)
from sentinelaegisforge_control_plane.monitoring.contract import row_fingerprint

from .contract import MAX_READY_EVENTS, AvailabilityError

MAX_VERSIONS = 64


@dataclass(frozen=True)
class VersionDifference:
    version: int
    previous: FeatureSnapshot | None
    current: FeatureSnapshot
    previous_rows: dict[str, str]
    current_rows: dict[str, str]
    new_rows: dict[str, str]


def _fingerprints(snapshot: FeatureSnapshot) -> dict[str, str]:
    columns = tuple(snapshot.table.column_names)
    rows = cast(list[dict[str, object]], snapshot.table.to_pylist())
    return {cast(str, row["event_id"]): row_fingerprint(row, columns) for row in rows}


def _read(source: Path, version: int) -> FeatureSnapshot:
    try:
        if DeltaTable(source, version=version).count() > MAX_READY_EVENTS:
            raise AvailabilityError(f"Delta version {version} exceeds local row limit")
        return StatisticalFeatureSource(source, version).read()
    except AvailabilityError:
        raise
    except (FeatureDataError, FeatureSourceError, SourceSchemaError) as error:
        raise AvailabilityError(f"Unverifiable Delta version {version}: {error}") from error
    except (DeltaError, OSError, ValueError) as error:
        raise AvailabilityError(f"Unverifiable Delta version {version}: {error}") from error


def discover_versions(
    source: Path, start: int, end: int, *, maximum: int = MAX_READY_EVENTS
) -> list[VersionDifference]:
    if (
        type(start) is not int
        or type(end) is not int
        or start < 0
        or start > end
        or type(maximum) is not int
        or not 1 <= maximum <= MAX_READY_EVENTS
        or end - start + 1 > MAX_VERSIONS
    ):
        raise AvailabilityError(
            "Require inclusive nonnegative range of at most 64 versions and bounded maximum"
        )
    source = source.resolve()
    previous: FeatureSnapshot | None = None
    if start > 0:
        previous = _read(source, start - 1)
        if previous.delta_version != start - 1:
            raise AvailabilityError(f"Missing Delta predecessor version {start - 1}")
        if previous.row_count > MAX_READY_EVENTS:
            raise AvailabilityError("Predecessor snapshot exceeds local row limit")
    result: list[VersionDifference] = []
    total_new = 0
    for version in range(start, end + 1):
        current = _read(source, version)
        if current.delta_version != version:
            raise AvailabilityError(f"Missing or substituted Delta version {version}")
        if current.row_count > MAX_READY_EVENTS:
            raise AvailabilityError(f"Delta version {version} exceeds local row limit")
        if previous is not None and (
            previous.delta_table_id != current.delta_table_id
            or previous.source_schema_fingerprint != current.source_schema_fingerprint
            or previous.protocol != current.protocol
        ):
            raise AvailabilityError(f"Table ID, 25-column schema, or protocol changed at {version}")
        old = _fingerprints(previous) if previous is not None else {}
        now = _fingerprints(current)
        missing = old.keys() - now.keys()
        changed = {
            event_id for event_id in old.keys() & now.keys() if old[event_id] != now[event_id]
        }
        if missing or changed:
            raise AvailabilityError(
                f"Append-only feature source violated at version {version}: "
                f"removed={len(missing)} changed={len(changed)}"
            )
        new = {event_id: now[event_id] for event_id in now.keys() - old.keys()}
        total_new += len(new)
        if total_new > maximum:
            raise AvailabilityError(f"Version range exceeds maximum {maximum} ready events")
        result.append(VersionDifference(version, previous, current, old, now, new))
        previous = current
    return result
