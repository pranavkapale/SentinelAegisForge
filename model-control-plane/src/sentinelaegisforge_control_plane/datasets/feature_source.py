"""Read one fixed Delta version through delta-rs, never raw Parquet behind Delta."""

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from .dataset_contract import feature_rows, schema_fingerprint
from .errors import FeatureSourceError


@dataclass(frozen=True)
class FeatureSnapshot:
    delta_path: Path
    delta_version: int
    delta_table_id: str
    protocol: dict[str, object]
    table: pa.Table
    source_schema_fingerprint: str

    @property
    def row_count(self) -> int:
        return int(self.table.num_rows)


class StatisticalFeatureSource:
    def __init__(self, delta_path: Path, version: int | None = None) -> None:
        if version is not None and (type(version) is not int or version < 0):
            raise FeatureSourceError("Delta version must be a nonnegative integer")
        self.delta_path = delta_path.resolve()
        self.version = version

    def read(self) -> FeatureSnapshot:
        try:
            delta = DeltaTable(self.delta_path, version=self.version)
            # The handle is pinned to this version; do not call update_incremental during a build.
            version = delta.version()
            protocol = delta.protocol()
            table = delta.to_pyarrow_table()
        except Exception as error:
            raise FeatureSourceError(
                f"Cannot read Delta feature source {self.delta_path}: {error}"
            ) from error
        feature_rows(table)
        return FeatureSnapshot(
            self.delta_path,
            version,
            str(delta.metadata().id),
            {
                "min_reader_version": protocol.min_reader_version,
                "min_writer_version": protocol.min_writer_version,
                "reader_features": sorted(protocol.reader_features or []),
                "writer_features": sorted(protocol.writer_features or []),
            },
            table,
            schema_fingerprint(table.schema),
        )
