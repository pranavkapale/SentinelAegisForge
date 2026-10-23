# Kafka-triggered research shadow inference

The Phase 25 `features.ready` message contains a committed-feature identity and fingerprint, not a feature row. Phase 26 keeps the Kafka consumer and model worker independently restartable:

```text
verified Phase 24 outbox → Kafka features.ready → shadow-group durable receipt → offset commit
                                                ↓ after full broker reconciliation
                                         sealed research-shadow-inbox-v1
                                                ↓ exact first-visible Delta version
                                         trusted Registry v1 model input
                                                ↓
                                      durable logical terminal result
                                                ↓
                                      Phase 19 prediction parity
```

The dedicated group is `sentinel-feature-ready-shadow-v1`, with a separate receipt root from Phase 25's reference research group. Scala seals only after source, journal, broker physical records, receipt identities and committed group offsets reconcile. The inbox ID hashes stable batch/outbox, cluster/topic/group and ready-event/envelope identity. Receipt first coordinates are retained and verified, while later physical duplicate observations do not change the semantic inbox ID. The sealed artifact contains `ready_events.jsonl` and `inbox_manifest.json`; it references the durable receipt store rather than copying mutable observations. Changing topic identity or losing retained broker history fails reconciliation.

The Python worker does not use a Kafka client. It re-inspects the sealed bytes, batch/outbox and receipt content, then reads bounded exact corrected statistical Delta versions. Each event must be newly visible at its claimed version and have the same full-row fingerprint. Source/table/schema membership failures are global, not request-local rejected inputs. Logical processing order is first-visible Delta version then event ID; it is not a claim about cross-partition Kafka ordering. A trusted registered research candidate is checked against its saved model-tree hash and loaded once per bounded session. Its original training cutoff must equal the explicit UTC scoring start. The reference window is `[2030-01-04T00:00:00Z, 2030-01-10T00:00:00Z)`; 500 training-period notifications are expected to be `OUT_OF_SCOPE`, not scored, and 1,000 later notifications are expected to be scored, subject to actual verification.

`kafka-shadow-inference-v1` request identity hashes ready-event ID, candidate ID and model-tree hash. Terminal outcomes are `SCORED`, `OUT_OF_SCOPE`, `INPUT_REJECTED` and `INFERENCE_FAILED`. Only `SCORED` has a finite probability. Result files contain identifiers, source version/fingerprint, status and optional probability/failure code, never raw feature values, labels, latent scenario truth or payment details. They are exclusively published using a flushed/fsynced temporary file and local hard link. A crash before publication can repeat physical inference; a crash after publication reuses the verified logical result. Unexpected or conflicting files fail closed. Completion comes from durable result membership, not a mutable cursor. This is cooperating-local-writer idempotence, not a distributed exactly-once guarantee.

The worker compares all successful post-training IDs, source-row fingerprints and scores to the immutable Phase 19 ledger using `math.isclose` at `1e-12` relative/absolute tolerance. The immutable run artifact stores result/parity/report hashes. The prior Phase 23 replay remains separate evidence: this phase adds real Kafka notification acceptance and first-visible Delta resolution, not an original contemporaneous model-availability measurement. Synthetic historical probabilities are uncalibrated and do not establish representative production fraud quality. No Registry state, Phase 18 dossier, Phase 19 predictions, transaction stream, feature state or payment action is changed.

Resource limits are explicit for inbox events, cached Delta rows, invocation attempts and processing batch size. Ordinary `make verify` remains Docker-independent; live commands require the local single-node broker and retained Phase 25 outbox/receipts. The research environment has no production HA, TLS/ACL, cross-host result uniqueness, process-isolated deadlines, latency SLO or action policy.
