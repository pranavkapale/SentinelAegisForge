# ADR-029: Local MLflow experiment tracking and run identity

## Status

Accepted

## Context

Phase 12 snapshots and Phase 13 training runs already have immutable identities, manifests, metrics, predictions and integrity checks. Phase 14 supplied a reconciled synthetic end-to-end example. These artifacts are authoritative but difficult to discover and compare across runs. Tracking must not retrain, redefine metrics, upload private scenario truth, or imply production model approval.

## Decision

Use pinned MLflow 3.17.0 as an index over **verified existing** Phase 13 runs. The local backend is an explicitly located SQLite database, with an explicit local artifact root. `inspect_run` and `inspect_snapshot` must pass before MLflow is mutated. An optional Phase 14 corpus ID is logged only when the caller supplies explicit corpus and all four Delta paths and the existing event-ID/coordinate reconciliation plus snapshot source/label provenance checks pass.

The deterministic `training_run_id` is the canonical engineering identity; MLflow's generated `run_id` is a distinct tracking address. The adapter logs manifest-derived parameters, source identities and synthetic-only warnings; validation/test metrics are copied from the verified metrics JSON. It uploads only the training manifest, metrics JSON and a small provenance summary. The verified prediction-file path/hash are retained in metadata, not uploaded. No model binary, labels, private sidecar or corpus is uploaded.

A local `flock` on the SQLite database's adjacent lock file serializes **cooperating local adapter writers** around lookup and creation. A sequential repeat verifies tags, parameters, exact metrics and downloaded artifact hashes before returning the same MLflow run. Duplicate identity claims, conflicts and incomplete statuses fail closed. A new run is marked `FINISHED` only after read-back; upload/logging failure leaves a visible `FAILED` claim (or `RUNNING` if termination itself fails), requiring investigation rather than automatic repair.

## Alternatives considered

- Replace Phase 13 artifacts with MLflow as the source of truth: simpler discovery, but would weaken existing immutable identity and integrity contracts.
- Use MLflow's legacy local file backend: easy startup, but SQLite provides a clearer local metadata store and query behavior.
- Run an HTTP tracking server or external database now: useful for teams, but unnecessary for single-machine experiment indexing and introduces operational/security obligations.
- Rely on MLflow tags alone for uniqueness: insufficient because tags have no global unique constraint and concurrent creators can race.

## Trade-offs

Read-back downloads three small artifacts, and explicit source inspection/reconciliation costs local I/O. This is intentional correctness overhead, not a performance claim. The lock is local-file cooperation, not distributed exactly-once tracking; external MLflow writers can still create conflicting claims, which the adapter detects on later lookup. Failed partial runs are not automatically repaired.

## Consequences

Tracking and inspection work without Docker or an MLflow server. The optional UI binds to localhost. Existing Phase 12/13 artifacts remain authoritative and unchanged. Synthetic metrics are integration evidence only; no candidate/champion state, model registry, serialized model, promotion, serving, drift or retraining is established.
