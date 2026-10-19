# ADR-037: Controlled non-enforcing shadow inference

## Status

Accepted

## Context

The Phase 16 candidate is technically reproducible, but Phase 17 Registry version 1 remains research-only and the Phase 18 dossier says `NOT_ELIGIBLE_FOR_PROMOTION`. Phase 19 has frozen offline scores for a fixed corrected statistical Delta version. Phase 22 adds separate provider-like feedback; it does not authorize inference deployment. We need to exercise request handling and compare an independently executed model with those frozen scores without suggesting actual online feature availability or payment control.

## Decision

Use a bounded local `shadow-replay-plan-v1` over one fixed, verified Delta table/version and explicit UTC ingestion-time window. Sort by `(ingestion_time,event_id)` and derive deterministic request IDs from the plan ID and event ID. Resolve an explicit numeric Registry version, verify its research-only status and trusted Phase 16 candidate, then load the model once for `shadow-execution-v1`. Preserve the strict 11-feature input projection and `[0,1]=[LEGIT,FRAUD]` mapping. One logical request receives one terminal result (`SCORED`, `INPUT_REJECTED`, or `EXECUTION_FAILED`). Identical redelivery within one local execution reuses its committed result; conflicting reuse fails. Known request-local inference failures are isolated, with sanitized codes and no automatic retry. Unanticipated global failures stop publication. Results, audit and report are published under an exclusive immutable local identity and compared by event ID, row fingerprint and probability to the verified Phase 19 ledger at `1e-12` relative/absolute tolerance.

The only persistent writes are ignored local plan, execution and parity artifacts. No live transport, payment API, Registry mutation, label input or risk-engine feedback exists. `execution_scope=LOCAL_SYNTHETIC_SHADOW_REPLAY` and `feature_availability_evidence=NOT_ESTABLISHED` are mandatory. Historical transaction ingestion time does not prove when a complete statistical feature row became available. This is not a production shadow deployment, online-latency observation or promotion authorization.

## Alternatives considered

- Deploy the synthetic candidate now: would provide actual traffic, but contradicts the production-ineligible dossier and missing readiness evidence.
- Reload per event or call the Phase 16 CLI per request: simpler reuse but repeated verification/deserialization obscures lifecycle and cost.
- Start a live Kafka shadow consumer or HTTP server: useful only after feature-readiness, transport, isolation and operational contracts exist.
- Rely on aggregate counts without request idempotency: cannot detect replay conflicts or missing identities.
- Add automatic retries: may mask model failures or duplicate effects without an error taxonomy and bounded policy.
- Feed probabilities into Phase 11/payment decisions: would couple research inference to an unauthorized action path.

## Trade-offs

Deep source/candidate inspection and single-row model calls make the local replay slower than a batch scorer; this phase measures correctness, not throughput. Local in-memory idempotence is not a globally unique or exactly-once inference guarantee. Exclusive directory publication leaves interrupted output visible and invalid; SHA-256 detects ordinary byte changes but is not an authenticity signature. There is no process-isolated timeout or cancellation guarantee.

## Consequences

The model can be executed under an explicit research-only, non-enforcing boundary and compared with Phase 19 without retraining or modifying upstream artifacts. Real feature availability, operational serving, failure recovery, access control, deadline cancellation, model approval and payment side effects remain deferred.
