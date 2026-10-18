# ADR-036: Durable outcome feedback ingestion

## Status

Accepted

## Context

Phase 12 labels represent only FRAUD/LEGIT and Phase 19 evaluates a frozen prediction ledger against synthetic delayed labels. A future independent investigation source may issue pending states, corrections and retractions, delivered late or out of order. Model probabilities and Phase 11 rules cannot establish ground truth. Real provider authorization is unavailable in this phase.

## Decision

Use the source-neutral `outcome-feedback-envelope-v1`: provider ID, immutable provider-record ID, stable provider-case ID, positive source revision, canonical transaction `event_id`, one of PENDING/CONFIRMED_FRAUD/CONFIRMED_LEGIT/RETRACTED, UTC provider update time and UTC local-boundary receipt time. These normalized statuses are project contract codes, not purported universal bank codes. `(provider_id,provider_record_id)` identifies a delivery; `(provider_id,provider_case_id,source_revision)` identifies a case revision. Neither is transaction identity.

Ingest a complete bounded JSONL batch into a new immutable `outcome-feedback-ledger-v1` after strict parsing, delivery deduplication, case/revision validation and reconciliation against one fixed verified Phase 10.1 Delta source. Identical delivery repeats collapse; changed reuse or a different record ID asserting the same case revision fails. Revisions must be contiguous from one and retain one event ID. A batch with a missing predecessor fails. Each snapshot preserves normalized records, source batch SHA-256/provenance, fixed Delta ID/version/schema and event-universe fingerprint; an optional verified base creates a new snapshot, never an in-place append. Incomplete publication remains invalid.

Pure AS_OF resolution uses `effective_available_at(N) = max(received_at(1..N))`. Thus a revision received before its predecessor does not become retrospectively effective. `source_updated_at <= received_at` and `received_at >= transaction ingestion_time` are required. Pending and retracted cases are not binary labels. Current confirmed FRAUD/LEGIT states alone are eligible; later retraction removes eligibility at later cutoffs, without changing earlier snapshots. Conflicting current confirmed outcomes across providers are ambiguous and block evaluation; no implicit provider precedence exists. The evaluator joins only these resolved outcomes to Phase 19's verified frozen probabilities in a distinct `feedback-outcome-monitoring-v2` snapshot. It reuses Phase 19 metric calculations but not its historical synthetic-corpus snapshot contract. Phase 12 labels and all old Phase 19 snapshots remain unchanged.

The deterministic `LOCAL_PROVIDER_FIXTURE` uses existing scored event IDs and hand-assigned synthetic statuses, not Phase 14 private truth, model probabilities or Phase 11 dispositions. Provider strings are provenance only, never proof of authority. Fixture `received_at` is a controlled test time, not independently attested real receipt. The Phase 18 `VAL004_REAL_WORLD_LABEL_QUALITY` gap remains `NOT_ESTABLISHED`.

## Alternatives considered

- Missing feedback as LEGIT would inflate coverage but invent negative outcomes and hide maturity bias.
- One mutable row per event would simplify current lookup but erase corrections, retractions and historical AS_OF evidence.
- Last-write-wins by timestamp would be easy but can select an incomplete or contradictory provider history, especially under out-of-order delivery.
- Model predictions or deterministic REVIEW/CLEAR as labels would be circular rather than independent truth.
- A provider-declared `TRUSTED_BANK` name would not authenticate source delivery.
- Kafka feedback ingestion could support an eventual live flow, but this phase has no authorized external source or operational transport contract.
- Extending Phase 12's binary label schema in place would reinterpret existing immutable snapshots and cannot naturally represent pending/retracted states.

## Trade-offs

The local batch importer requires complete histories and retained Delta/source evidence; it fails closed rather than buffering missing revisions. Separate envelopes, ledgers and projections add mapping code but preserve auditability and old contracts. Local exclusive publication and SHA-256 hashes detect ordinary conflicts/corruption, not a globally exactly-once source or a cryptographic authenticity guarantee. The current resolver fails on conflicting confirmed providers instead of defining source precedence.

## Consequences

The feedback ledger and AS_OF monitoring snapshots can be inspected without Docker, Kafka, Spark or model loading. Real provider authentication, signatures, transport security, access control, retention, authority precedence and business approval remain deferred. No Registry mutation, promotion, model serving, retraining or automated action follows from these synthetic fixtures.
