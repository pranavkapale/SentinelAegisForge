# ADR-032: Model validation evidence policy

## Status

Accepted

## Context

Phase 17 registers a technically verified research candidate, but Registry `READY` and synthetic evaluation do not establish production fraud quality or deployment readiness. Reviewers need a reproducible statement of what is verified, what failed, and what remains unsupported, without modifying training or registering an approved model.

## Decision

Assess an explicit numeric Registry version through the existing deep Registry, candidate, training, dataset and tracking inspectors. When an explicit verified Phase 15 corpus ID exists, recheck the retained Phase 14 private plan/truth/label hashes and link labels to the Phase 12 source hash; never use Phase 11 rule dispositions as ground truth. Evaluate the resulting typed evidence using immutable `model-validation-policy-v1`: ten ordered requirements, three evidence states (`VERIFIED`, `FAILED`, `NOT_ESTABLISHED`), and fail-closed mandatory blocking. SHA-256 over canonical policy semantics identifies the policy; SHA-256 over canonical source identities and artifact hashes identifies the assessment. Publish a small immutable local dossier containing assessment JSON, evidence references/hashes and a reviewer report. Repeated assessment verifies exact bytes; incomplete or conflicting dossiers fail closed. Source corruption fails *before* publication, while absent representative evidence is recorded as `NOT_ESTABLISHED`.

Only artifact integrity, archived prediction reconstruction and provenance are currently VERIFIED. Ingestion-time split mechanics are separately VERIFIED, but representative out-of-time performance is NOT_ESTABLISHED. Synthetic labels and metrics cannot establish real-world quality, discrimination, calibration or segment robustness. Operational and business approval evidence is absent. The only Phase 18 outcome is `NOT_ELIGIBLE_FOR_PROMOTION`, `production_eligible=false`. This policy is an evidence assessment, not an approval authority or cryptographic signature.

## Alternatives considered

- Treat Registry `READY` or candidate technical verification as approval: simple but conflates catalog integrity with model quality and business authority.
- Use synthetic test scores as production acceptance evidence: measurable, but the scenario does not represent real fraud outcomes; a threshold would be invented.
- Accept operator-supplied JSON assertions or an override flag: flexible, but lacks a verified evidence contract and can silently turn missing evidence into a pass.
- Add an external policy/workflow engine now: may help later multi-party governance, but adds machinery without an approval or evidence-source contract.

## Trade-offs

Deep inspection repeats local I/O and trusted candidate reconstruction. Dossier content is reproducible only while the referenced local evidence remains available. SHA-256 detects ordinary changes but cannot prove authenticity against a privileged actor who can rewrite all evidence. The fixed v1 policy cannot admit new positive evidence without a versioned policy/collector change; this is intentional fail-closed behavior.

## Consequences

Each dossier preserves exact Registry, candidate, dataset, training and policy identities, along with synthetic-only metrics and explicit blockers. Inspection and comparison are read-only and do not select or promote a model. Real-label provenance, representative evaluation, calibration, segment/operational checks and accountable approval need future independently verified contracts before any positive promotion claim can be considered.
