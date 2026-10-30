# Quiesced synthetic research recovery

Phase 31 implements roadmap W1. The source boundary is one selected Phase 28/29 derivative,
plus its actual transitive model/data dependencies. This is local synthetic research recovery,
not a production disaster-recovery service.

```text
Verified selected lineage → stop writers → consistent cutoff
 → full offline Kafka volume + exact file inventory + SQLite backup
 → verified bundle → verified backup copy
 → fresh isolated namespace → read-only identity inspection
 → original checkpoint/state resume → exact source-to-result reconciliation
```

## Authorities and closure

`recovery.lineage.dependencies` uses the existing integration, stream, candidate and state
compatibility inspectors. It checks every retained version of all four Delta tables and the
training feature table, not only the latest data files. The graph contains corpus/publication
records, source fences, four checkpoints and tables, outboxes/acknowledgement journals/receipts/
results, the original candidate/training/snapshot/label provenance, MLflow artifacts and SQLite,
Registry receipt and Phase 18 dossier. Historical synthetic truth is provenance, never a model
input. Phase 19's separate prediction ledger and Phase 22 feedback are not dependencies of this
selected inference lineage; exclusions are recorded explicitly.

Cutoff evidence includes stable file hashes, Delta table IDs and full row/ID digests, checkpoint
configuration and files, model-tree identity, Registry research-only governance, pending work,
Kafka cluster/topic IDs, beginning/end/group offsets and original Registry subject versions/IDs.
The complete broker volume includes KRaft, `_schemas`, group offsets and internal topics.

## Consistency and identity

Only a quiesced cooperating local operator may capture. Enumerate JVM/Python writers (the editor
Metals language server is explicitly distinguished), reject pending inference claims, cleanly
stop Kafka/Registry and reject any running container mounting the source volume. Hash inventories
before and after capture/build. SQLite uses `Connection.backup`, including committed WAL content;
compare logical schema/rows and `integrity_check` instead of assuming physical page layouts match.
The original source is never reset or removed. A stopped broker alone is not writer quiescence.

`synthetic-recovery-bundle-v1` binds the cutoff identity, lineage, Git revision, runtime/configuration
identities and dependency fingerprint. Capture timestamps and measured durations are separate
operational metadata. The manifest hashes payload and evidence files. Hashes are not signatures.
Failed builds/copies remain incomplete and are never reported successful.

Archive checks reject absolute/traversing paths, links, special files, collisions, sparse entries,
unsafe modes, and excessive expansion. Extraction requires a fresh destination and performs an
independent byte check. Source symlinks and existing targets are refused. The local bounds are
100,000 files and 20 GiB per archive/inventory, and 64 retained Delta versions per selected table.
This is not a hostile concurrent-writer filesystem service.

## Explicit operator interface

All paths supplied to the recovery CLI are absolute. No operation is part of `make verify`.
Use `make recovery RECOVERY_ACTION=<action> RECOVERY_ARGS='...'` or directly:

```sh
cd model-control-plane
uv run --frozen python -m sentinelaegisforge_control_plane.recovery.cli --help
```

Actions, in order:

1. `dependencies --selection <json>`: inspect the five paths `repository`, `integration`,
   `stream`, `candidate`, `assessment` and their closure.
2. `capture --selection <json> --output <fresh-cutoff-dir>`: probe identities, quiesce and archive.
3. `build --cutoff <cutoff.json> --kafka-archive <kafka-volume.tar> --output <fresh-bundle>`.
4. `verify --bundle <bundle>`; `copy --bundle <bundle> --output <fresh-backup>`.
5. `restore-files --bundle <backup> --output <fresh-restored-root>`.
6. `stage-docker --bundle <backup> --restored <restored-root> --output <fresh-evidence-dir>`.
7. `preflight-docker --output <evidence-dir>`: offline, original absolute namespace, model inspector.
8. `start-docker --output <evidence-dir>`: start only after preflight; compare Kafka/schema identities.
9. `resume-docker --output <evidence-dir>`: existing JVM entry points and Python research worker.
10. `finish --output <evidence-dir>`; `inspect-rehearsal --output <evidence-dir>`.

The documented fixture has 30 events in three ten-event waves. Before capture, wave 0 is fully
materialized, ten notifications accepted and five results durable; wave 1 is published and
materialized through rolling features, leaving statistical state continuation pending. Wave 2
is unpublished. `recovery.scenario.prepare` invokes existing producer/Scala/handoff commands,
refuses repeated preparation and never fabricates feature rows. Existing deterministic scenario
creation and input-fence commands precede preparation.

The restore runner uses the same compiled Scala entry points, configurations, checkpoints and
transaction IDs. Its scoped command adapter reuses the Phase 28 orchestration while replacing
host sbt/Make startup with the resolved JVM classpath; it does not reimplement any processing.
It completes statistical wave 1 and inference backlog, then publishes and processes wave 2.
Original Delta history and durable results must remain byte/content equivalent.

## Build the isolated runtime

The restore image uses pinned Python 3.13.9 and Temurin JDK 21 base digests. It installs the
existing frozen dependencies; no project dependency is added. Create a fresh context, export
`uv export --frozen --no-dev --no-emit-project --format requirements-txt --output-file <context>/requirements.txt`
from the Python module and `sbt 'export Runtime / fullClasspath' > <context>/classpath.txt` from
Scala. Then run `python -m sentinelaegisforge_control_plane.recovery.image --repository <repo>
--context <context> --coursier-cache <actual-coursier-v1-path>` using the project's Python.
Build from the repository root:

```sh
docker build --build-arg REPOSITORY_PATH=<original-absolute-repository> \
  -t sentinel-recovery-runtime:phase31 -f scripts/recovery-runtime.Dockerfile <context>
```

The image includes code/dependencies only, never original `.local` contents. The restore records
its image content ID. The source broker remains stopped. Restored Kafka uses a fresh named volume,
an internal Docker network and no host ports. Registry and processing share its network namespace,
so `localhost:9092/8081` resolve to the restored services. Actual container mounts are checked
before execution; only restored data, the read-only backup and new evidence are permitted.
A source-only sentinel must be absent. No Docker socket or original data mount is allowed.
Stop the restored services with `docker compose -f <evidence>/compose.json stop`; retain evidence
and volumes for inspection. No automatic reset/delete command is provided.

## Verification and interpretation

Do not call file restoration recovery-complete. Preflight checks all bytes, retained Delta history,
checkpoint/state compatibility, original model and SQLite/Registry. Broker startup separately
checks cluster/topic IDs, offsets and schema metadata. Resume uses restored RocksDB, original
source offsets and sink transaction IDs. Exact membership across all four Delta stages, ready
notifications, receipts and terminal results must reconcile; independent model probabilities
must agree with preserved/reference evidence. New offsets after legitimate resumed publication
are separate from historical boundaries.

The user-selected `backup_isolated_storage/` is a **same-host, same-disk simulation**. A successful
Docker rehearsal is recorded separately from independent-media recovery. No off-host/disk-loss
protection has been demonstrated. Measured durations are observations, not production RPO/RTO.
The model stays version 1, `REGISTERED_RESEARCH_ONLY`, `production_eligible=false`, with the
unchanged `NOT_ELIGIBLE_FOR_PROMOTION` dossier. No W2–W8 work or next phase is implied.

## Live preflight corrections

The first volume preflight correctly refused a mismatch. Investigation found identical bytes,
paths, ownership and permissions; the Kafka image's tar changed directory modification times
when creating children. The comparison now excludes directory mtime only, retaining file mtime,
mode, owner, size, SHA-256 and hierarchy. A focused test protects that distinction.

Schema Registry startup appended two verified `{keytype: NOOP, magic: 0}` records to `_schemas`.
The gate permits at most 16 directly inspected NOOP suffix records, while preserving original
schema definitions/IDs/compatibility, topic IDs, all application offsets and research group offsets.
It rejects unexplained suffixes or lost history. Offline full-volume hashes establish the historical
storage boundary before startup; observed internal startup progress is recorded separately.

Implementation references: [SQLite backup API](https://docs.python.org/3.13/library/sqlite3.html#sqlite3.Connection.backup)
and [Docker volume backup/restore operations](https://docs.docker.com/engine/storage/volumes/#back-up-restore-or-migrate-data-volumes).
