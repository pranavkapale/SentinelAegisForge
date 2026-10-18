# Durable outcome feedback and AS_OF evaluation

```text
External-style feedback envelope
            ↓
      Strict validation
            ↓
 Provider-delivery deduplication
            ↓
 Case/revision completeness + fixed Delta event reconciliation
            ↓
 Immutable feedback ledger
            ↓
 Cumulative-receipt AS_OF resolution ── frozen Phase 19 predictions
            ↓
 Immutable conditional-quality snapshot
            X
 Retraining / promotion: not implemented
```

## Contracts and identities

`outcome-feedback-envelope-v1` has exactly eight JSONL fields: `provider_id`, `provider_record_id`, `provider_case_id`, `source_revision`, `event_id`, `status`, `source_updated_at`, `received_at`. IDs are separate: event ID identifies the transaction occurrence; provider record ID identifies one immutable notification; provider case ID groups revisions; source revision orders a case; feedback ledger ID identifies the verified local snapshot. The normalized statuses are PENDING, CONFIRMED_FRAUD, CONFIRMED_LEGIT and RETRACTED. This is a local contract, not a claim about any institution's code set. Strict parsing rejects unknown/missing/duplicate fields, noncanonical UUIDs, non-UTC or sub-microsecond timestamps, invalid revisions and malformed JSONL. No payment secrets or raw case notes are accepted.

`source_updated_at` is provider-claimed case-update time. `received_at` is when the notification was available to this local boundary. A transaction's `ingestion_time` is the earlier Module A observation clock. The local fixture supplies deterministic receipt times; it cannot prove real-world receipt. The contract requires `source_updated_at <= received_at` and receipt no earlier than transaction ingestion. An old provider update never backdates evaluation availability.

One import takes a JSONL batch, an explicit fixed version of the validated 25-column Phase 10.1 statistical Delta table, an optional verified base ledger and an output root. The source is read through delta-rs with its schema/data checks. Unknown event IDs fail. The immutable ledger records Delta path/table ID/version/schema fingerprint, a sorted event-ID/ingestion-time universe fingerprint, source batch path/byte SHA-256, canonical normalized records, logical/byte hashes, counts, revision coverage and suppressed identical deliveries. A changed delivery ID payload, duplicate case revision or missing revision fails the entire build. Old ledger directories are never modified; incomplete outputs fail inspection. Raw input files may be retained for provenance but ledger verification does not treat their later filesystem presence as authenticity proof.

## Resolution and monitoring

For each `(provider_id,provider_case_id)` complete revision history, revision N is available only from `max(received_at of revisions 1..N)`. At an explicit UTC cutoff select the highest effective revision per case. This handles out-of-order delivery without retroactive leakage. PENDING is unresolved; RETRACTED removes a prior confirmation from current eligibility; later confirmation can restore eligibility. A case not yet received is `NOT_YET_OBSERVED`; an event with no case is `NO_CASE`. Conflicting current confirmed providers produce `AMBIGUOUS` and block evaluation. Confirmed FRAUD maps to 1 and confirmed LEGIT to 0 only for the metrics projection. No Phase 12 binary history is rewritten, and no pending/retracted case is exported as LEGIT.

`feedback-outcome-monitoring-v2` checks the exact Phase 19 prediction ledger and feedback ledger share one Delta source ID/version/schema. The frozen probabilities are read, never recomputed. The denominator is every scored event. Mutually exclusive confirmed, pending, retracted, not-yet-observed and no-case buckets reconcile to that population. Pending/retracted age since transaction ingestion is reported in coverage. Each resolved row retains selected provider/case/record/revision and effective availability evidence; aggregate and segment metrics reuse Phase 19 evaluation and remain unavailable when class/row support is insufficient. Quality is conditional on confirmed outcomes and can change as cases mature or retract even though predictions do not change. The Phase 19 synthetic-corpus `delayed-outcome-monitoring-v1` artifacts are neither changed nor reinterpreted.

The `LOCAL_PROVIDER_FIXTURE` selects nine real scored event IDs and hand-assigns controlled statuses across two input batches. It intentionally includes replay, pending, correction, retraction, delayed confirmation and revision 3 received before revision 2. The statuses are synthetic test data independent of Phase 14 truth, Phase 11 rules and model scores. It does not establish external source authenticity or actual fraud quality. The Phase 18 model remains production-ineligible.

Before real provider use, authentication/signature verification, authorization, privacy/access control, retention, source precedence and incident handling need explicit contracts. No HTTP endpoint, Kafka feedback topic, Registry mutation, model promotion, serving, retraining or automated alert is implemented.
