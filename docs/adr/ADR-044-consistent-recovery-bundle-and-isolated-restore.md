# ADR-044 — Consistent recovery bundle and isolated restoration

## Status

Accepted for bounded synthetic research recovery. Implements Phase 30 roadmap W1 only.

## Context

Process restart does not prove recovery after storage loss. The research lineage spans Kafka
KRaft metadata, Schema Registry's `_schemas`, Delta histories, RocksDB checkpoints, durable
notification/inference artifacts, SQLite Registry metadata and immutable model provenance.
Many identities contain absolute paths. Reinitializing a topic, checkpoint or model would
substitute a new identity rather than restore the old one.

## Decision

Capture a cooperating single-operator, quiesced cutoff. Inspect the selected dependency
closure and pinned histories, stop pipeline writers and broker/Registry, archive the entire
actual Kafka volume read-only, and use SQLite's backup API. Content-inventory fingerprints
and semantic identities exclude capture time. Unsigned hashes establish integrity, not
operator authorization. Preserve original bytes and historical logical absolute paths.

Restore only into fresh targets. Validate archives before extraction, reject links, traversal,
devices, duplicates, unsafe modes and expansion beyond 100,000 entries/20 GiB. Refuse overwrite
or fallback to original files. Validate every authority before resuming existing applications
with original checkpoints, transaction application IDs, group offsets and research model.

The operator selected `backup_isolated_storage/` as a **same-disk simulation**, and Docker
as the isolated runtime. Use a new broker volume, internal network without published ports,
and a runtime image containing only code/dependencies. Mount only the restored `.local`
hierarchy at its original absolute path. Share the restored Kafka network namespace so
historical localhost endpoints need no rewriting. No source volume or Docker socket is
mounted into that runtime. Source services remain stopped.

A completed Docker simulation may be `VERIFIED_RESTORED`; the overall disaster-recovery
assessment remains `PARTIALLY_VERIFIED` until independently stored backup media is tested.
A second directory is not a separate physical storage failure domain.

## Alternatives considered

- **Online copies:** less downtime, but require coordinated writer/transaction capture across
  several authorities. Not established by this phase.
- **Message exports and fresh checkpoints:** useful replay experiments but lose Kafka identities
  and bounded Spark state; cannot satisfy faithful recovery.
- **VM or separate host and external disk:** stronger isolation and storage independence;
  preferred for a later independent-media rehearsal, unavailable for this local exercise.
- **Path migration:** useful portability work, but would require explicitly versioned identity
  migration. Preserving the namespace avoids silently rewriting provenance.

- **Latest Parquet files or model-only backups:** smaller, but omit retained Delta versions,
  checkpoints, notification history and the model's dataset/training/Registry closure.
- **Schema re-registration or model reconstruction:** can recreate useful behavior, but changes
  historical schema IDs/model bytes and is not faithful restoration.
- **Uncontrolled SQLite file copies:** simple but can omit committed WAL transactions; use the
  supported backup API and inspect logical contents under writer quiescence.
- **Ordinary process restart:** proves retained-state restart, not restoration from a verified copy.
- **Cloud/Kubernetes or distributed snapshots now:** may support broader recovery designs, but
  introduce substantial infrastructure before the bounded local contract is demonstrated.

## Trade-offs

Quiescence introduces downtime and assumes cooperating local operators. The bundle is bounded
and synthetic-only, not encrypted/authenticated archival storage. Full broker storage includes
internal topics and other retained records needed to faithfully restore this single broker.
Local filesystem payloads include only the selected transitive dependency closure.

## Consequences

Keep original source data and earlier evidence. Failed attempts remain inspectable; never repair
from source after restore. Probability parity, exact event membership, historical row digests,
stateful continuation and zero unexplained backlog are separate gates from file-copy success.
No production RPO/RTO, HA, security certification, lifetime uniqueness, model promotion, serving
or payment enforcement follows from this result. W2–W8 and Phase 32 remain unimplemented.
