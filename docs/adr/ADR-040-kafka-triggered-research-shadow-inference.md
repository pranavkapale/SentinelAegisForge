# ADR-040: Kafka-triggered research shadow inference with a durable inbox

## Status

Accepted for bounded local research. No serving or promotion authorization.

## Context

ADR-039 transports committed-feature notifications and persists receipts before Kafka offset commits. Those receipts prove acceptance, not model execution. Phase 23 scored a fixed historical Delta snapshot without Kafka. A research consumer must bridge those boundaries without adding model inference to the Kafka poll/commit loop or confusing a replayed physical record with a new inference request.

## Decision

Use a separate stable `sentinel-feature-ready-shadow-v1` consumer group and receipt root, retaining the Phase 25 validation, durable receipt-before-commit order, broker scan and partition-offset reconciliation. Seal a fully reconciled `research-shadow-inbox-v1` artifact keyed by the batch/outbox, cluster/topic/group and canonical ready-event/envelope identities. Physical duplicate attempts and mutable observation counts are evidence, not inbox identity. The independent Python worker rechecks the seal and receipt inventory, resolves each ready event against its exact first-visible corrected Delta version, and loads the explicitly registered research-only candidate through the existing trusted loader. It scores only the UTC post-training window; earlier valid notifications become `OUT_OF_SCOPE`.

The logical request ID hashes ready-event ID, candidate ID, model-tree hash and `kafka-shadow-inference-v1`. An fsynced local temporary file is linked exclusively into a per-inbox/candidate result namespace. A retry verifies and reuses an identical terminal result; a conflicting claim fails. Progress is derived from durable outcomes, not a cursor. The Phase 19 prediction ledger supplies independent ID, row-fingerprint and probability parity at `1e-12` relative/absolute tolerance. These are synthetic historical research outcomes only.

## Alternatives considered

- Running sklearn in the Kafka consumer would tie offset progress and availability to inference, complicating crash recovery and model outages.
- Reusing the original Phase 25 research group would share offsets/receipts and make independent shadow replay or outage recovery ambiguous.
- Committing Kafka offsets only after scoring would hold transport progress hostage to model availability, rather than relying on the durable notification inbox.
- A second Python Kafka client would duplicate the verified Phase 25 wire, receipt and offset implementation.
- Scoring from the newest Delta snapshot would discard first-visible-version provenance and potentially change inputs.
- Rerunning Phase 23 and calling it Kafka inference would never prove broker notification acceptance or receipt recovery.
- Scoring the pre-training observations as out-of-sample results would blur the saved model's training boundary.
- Kafka offset or delivery attempt as request identity would score intentional replay duplicates again.
- In-memory deduplication would not survive worker restart. A distributed result database is unnecessary for this bounded local research lineage.
- Automatically substituting another model on failure would change request identity and invalidate reproducible parity; this worker fails closed instead.
- Publishing probabilities into payment-decision streams would cross the research-only governance boundary without approval or action semantics.

## Trade-offs

Sealing requires a full retained broker scan and the source/outbox/receipt files to remain available. Per-request local files favor auditability over throughput. The worker caches bounded exact Delta snapshots rather than opening one snapshot per event. Local hard-link claims and fsync protect cooperating local writers but are not globally transactional, cross-host unique or hostile-writer-proof. An inference crash before result publication can cause another physical model invocation; after publication, restart reuses the durable logical result. The local research path does not establish original online feature-ready time or a latency SLO.

## Consequences

Kafka receipt, offset commit, sealed inbox, model invocation and durable result are separate recovery boundaries. Exactly one accepted local logical result per request identity can be demonstrated under the cooperating-writer contract, not exactly-once physical inference or end-to-end distributed transactions. The Registry candidate and Phase 18 production-ineligible dossier are unchanged. No labels, latent truth, fraud decisions or feature rows enter `features.ready`; no serving, payment enforcement, model promotion or retraining occurs.
