# ADR-030: Reproducible offline model artifacts

## Status

Accepted

## Context

Phase 13 kept its fitted logistic Pipeline only in memory. Its verified immutable snapshot, training manifest, metrics and validation/test predictions remain available. Phase 15 tracks those records but contains no model binary. An executable candidate must not be mistaken for the recovered original estimator or for an approved fraud model.

## Decision

Reconstruct the Phase 13 Pipeline using its verified Phase 12 snapshot and unchanged ingestion-time cohorts, fit-label eligibility, feature projection and estimator configuration. Fit only the original eligible training cohort. Gate publication on per-`(split,event_id)` equivalence to every archived validation/test positive-class probability (`math.isclose`, relative and absolute tolerance `1e-12`), with `[0,1] = [LEGIT,FRAUD]` checked explicitly. The original metrics and predictions remain authoritative.

Save the complete Pipeline as a local MLflow sklearn Model with explicit `skops` serialization. Pin the already-transitive `skops` package directly and statically allow only `numpy.dtype`, which the pinned Pipeline requires. This allowlist is not learned from an untrusted artifact. Validate artifact hashes and exact runtime versions before any deserialization; only locally generated, trusted candidates may be loaded. MLflow Model metadata can itself contain a trust list, so `skops` is not a license to load arbitrary external files.

Use `offline-logistic-candidate-v1` and a SHA-256 candidate ID over canonical training/dataset identity, input/preprocessing/estimator contracts, serialization and runtime versions. The candidate ID excludes model bytes; a separate sorted file-hash tree protects artifact integrity. A local exclusive publication claim leaves incomplete transfers visibly invalid. Existing candidates are verified, not overwritten. Technical status is `TECHNICALLY_VERIFIED`, never production approval.

## Alternatives considered

- Fit again and save without comparing archived predictions: simpler, but a changed cohort or library setting could silently create a different model under the old run identity.
- Persist pickle/cloudpickle without a trust boundary: broadly compatible, but arbitrary deserialization is a poor default for candidate inspection.
- Automatically register the model: useful after governance exists, but a registry version would prematurely imply selection/promotion semantics.
- Deploy the synthetic baseline immediately: would convert integration evidence into an unsupported production-fraud claim.

## Trade-offs

Reconstruction and full prediction comparison require local fitting and snapshot access. `skops` needs a narrow static type allowlist and exact runtime provenance; byte-for-byte serialized output is not promised across environments. Integrity hashes detect accidental/local corruption, not malicious replacement of both artifact and manifest by a privileged actor.

## Consequences

An offline score is an uncalibrated FRAUD-class probability estimate, not a decision or payment action. Phase 12/13 artifacts and Phase 15 tracking remain authoritative and unchanged. No model registry, promotion, serving, drift, retraining orchestration or fraud-quality approval is introduced.
