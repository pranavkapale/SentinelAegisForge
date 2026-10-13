# ADR-031: Model Registry candidate governance

## Status

Accepted

## Context

Phase 16 produces a technically verified, immutable local offline candidate. Phase 15 tracks the earlier training experiment but did not produce that model binary. A discoverable catalog must preserve those separate identities and synthetic-only limitations without implying deployment approval.

## Decision

Use MLflow 3.17.0 Model Registry in the existing local SQLite tracking backend. Registration first reruns Phase 16 candidate inspection and verifies training, snapshot, tracking, source bytes, input contract, runtime and archived prediction equivalence. A typed eligibility result permits only `REGISTERED_RESEARCH_ONLY`, with `production_eligible=false`. The registry version uses the Phase 16 `file://` model directory as its source; no artifact copy or independent backup is implied. The immutable candidate ID, Phase 15 tracking run ID and MLflow-assigned model name/version are distinct. The Phase 15 run ID is a provenance tag, never the model version's producing `run_id`.

Use a local cooperating-writer lock and complete paginated version search for sequential idempotence. New versions begin with `REGISTRATION_INCOMPLETE`; only after source read-back is the status tag set to `COMPLETE`, followed by exact-version metadata/source verification and an exclusive immutable receipt. Partial claims remain visible and require investigation. Existing conflicting or duplicate claims fail closed. Mutable Registry tags are corroborated against the candidate and authoritative run, not treated as signatures. Inspection uses explicit numeric versions; ordinary metadata inspection avoids deserialization, while explicit artifact verification invokes the trusted Phase 16 loader and archived-prediction checks. No aliases, stages, promotion or serving action exists.

## Alternatives considered

- Register during Phase 16 packaging: fewer steps, but it would blur technical artifact verification and governance.
- Treat latest version as champion: convenient selection, but version order is not approval evidence.
- Promote automatically on synthetic AP/ROC AUC: automated, but the scenario is not representative fraud-quality evidence.
- Build a custom Registry database: maximum control, but duplicates existing MLflow catalog capabilities and raises maintenance cost.

## Trade-offs

Registration and inspection redo local integrity/provenance I/O. The local `file://` source must remain accessible and unchanged; the SQLite Registry is not an artifact backup or portable model package. A cooperating filesystem lock does not serialize external MLflow writers or provide a global uniqueness constraint. Hashes detect source changes but are not signatures against privileged alteration of all evidence. Synthetic-only labels, missing calibration, absent shadow/canary evidence, business approval and serving contract preclude production eligibility.

## Consequences

A registered version is a research catalog entry, not an approved model. MLflow `READY` means version creation completed, not quality or governance approval. Partial versions are never silently repaired or deleted. Stronger artifact storage, signed approvals, access control, candidate/champion selection and deployment remain separate decisions.
