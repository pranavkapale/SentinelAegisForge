# ADR-024: Immutable offline dataset snapshots

## Status

Accepted

## Context

Module B needs reproducible training-data semantics before any model exists. Module A's corrected feature output is append-only but fresh-lineage replay can create repeated event IDs. Delta library version numbers alone do not prove Scala/Python compatibility.

## Decision

Read only governed `transaction_statistical_features_v2` artifacts through delta-rs into Arrow, using pinned `deltalake==1.6.6` and `pyarrow==25.0.1` under Python 3.13. Capture a fixed Delta version before materialization, preserve the entire ordered 25-column contract, validate exact logical types and feature consistency, and fail on duplicate event IDs anywhere in that version. The optional `--source-version` allows rebuilding an older version after later appends. Never read raw source Parquet behind Delta, repair bad features or silently choose one duplicate.

Filter features by `ingestion_time <= AS_OF_TIME`, not by existence in the latest table or by event time. Join one latest eligible independent label per available event under ADR-023. Preserve exact decimal monetary values and currency; statistical doubles retain their established approximations. Source shape cannot distinguish historical v1 monetary semantics from v2: operators must supply a governed corrected-v2 lineage. Risk-decision tables are structurally rejected and never used as labels.

Dataset contract `transaction-fraud-v1` produces 29 columns: all source fields plus int8 `fraud_label`, UTC-microsecond `label_observed_at`, string `label_source` and int64 `label_revision`. Sort by event_time, then event_id. Preserve identity/lineage for audit but declare a separate candidate feature allowlist; high-cardinality IDs, timestamps, lineage and label metadata are not baseline model features. No split, balancing, encoding or training occurs.

Each SHA-256 snapshot directory contains `dataset.parquet` and `manifest.json`. Identity uses canonical UTF-8 JSON (sorted object keys, compact separators, literal Unicode, no NaN, final LF) over contract/identity versions, normalized source path, source table ID/version/schema fingerprint, UTC AS_OF_TIME, normalized label path/exact byte hash, label-resolution version, ordered logical dataset fingerprint and effective Python/Delta/Arrow versions. The table ID is captured from Delta, not newly generated. Paths are provenance inputs: relocation can change snapshot identity. Output root is not an identity input. No current timestamp, hostname or generated UUID enters identity.

The source-schema fingerprint hashes the ordered array of field names and Arrow logical-type strings as compact sorted-key JSON without a final LF; nullability and implementation metadata are not inputs. Logical dataset identity hashes a canonical header (algorithm version and output-schema fingerprint), then one LF-terminated canonical ordered value array per sorted row. Decimals use fixed-scale text, timestamps UTC microsecond text, doubles exact `float.hex()`, and strings/integers/null retain JSON types. All source and label/provenance values participate.

Persist row/class/exclusion counts, source protocol/version/schema, hashes, as-of time, provenance, candidate/non-feature declarations and runtime versions. Parquet SHA-256 verifies artifact integrity; logical identity is the reproducibility contract. Bytes match in pinned-runtime tests, not a guarantee across writer versions or platforms.

Existing snapshots must verify manifest semantics, canonical identity, schema, row/class counts, logical fingerprint, Parquet integrity and expected rebuilt logical data. Never overwrite conflicting or incomplete artifacts. Exclusive directory/file creation prevents silent replacement; an interrupted or concurrent incomplete build fails visibly and requires intentional inspection, not automated repair. New snapshots never edit earlier ones.

## Alternatives considered

- Latest mutable data on each training run: simple, but cannot reconstruct a prior source version or knowledge boundary without recorded immutable inputs.
- CSV-only export: portable and inspectable, but requires separate precision/type/null contracts, particularly for money and timestamps. Parquet plus a readable manifest preserves Arrow types.
- Spark inside Module B: aligns with Module A's runtime, but adds a JVM/PySpark toolchain for a local snapshot workload that Arrow and delta-rs already support.

## Trade-offs

The local builder materializes tables/rows in memory; this is not a large-scale extraction or throughput claim. Strict schema and identity checks reject ambiguous data instead of guessing. Historical rebuilds depend on retained Delta log/data versions and preserved label bytes. Pinning runtime provenance into identity intentionally gives changed runtimes a new snapshot namespace.

## Consequences

Real Scala Delta reads and T1/T2 delayed-label evidence gate adoption. Earlier Scala state and feature semantics remain unchanged. AS_OF filtering proves the specified ingestion/label contract, not reconstructed historical feature materialization time or external label availability. Temporal splits, training, production ground-truth ingestion, class-imbalance treatment, MLflow and model lifecycle remain deferred.
