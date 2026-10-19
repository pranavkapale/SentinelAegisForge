# Controlled local shadow inference

```text
Fixed corrected statistical Delta version
    → immutable bounded request plan
    → exact research-only Registry version + verified trusted model
    → local request validation and inference
    → immutable terminal-result ledger + audit
    → immutable Phase 19 probability parity evidence
    ↛ live transaction or payment enforcement
```

This is a local synthetic historical replay, not a Kafka shadow consumer, HTTP model service, production deployment or proof of online feature readiness. Transaction `event_time` and `ingestion_time` remain source facts. The latter orders replay requests alongside `event_id`; consecutive `replay_sequence` is only a deterministic local execution order. No feature-ready timestamp or online latency is inferred. Artifacts carry `execution_scope=LOCAL_SYNTHETIC_SHADOW_REPLAY` and `feature_availability_evidence=NOT_ESTABLISHED`.

The plan references event IDs and exact 25-column source-row fingerprints, not copied customer/device/IP fields, outcomes or risk decisions. It pins Delta path, table ID, version, schema fingerprint, exact Registry name/version, candidate ID, model tree hash, input contract fingerprint and half-open UTC replay window. Fixed-source and Registry verification precede execution. The normal reference window is `[2030-01-04T00:00:00Z,2030-01-10T00:00:00Z)`; request limits default to at most 5,000 and the reference corpus contains 1,000 requests.

The trusted Phase 16 loader verifies artifact hashes, pinned runtime, static skops type allowance, complete sklearn Pipeline, feature names and positive-class mapping before returning one loaded model. Every request projects exactly the established 11 candidate features, preserving Decimal until the original model-input float boundary. Phase 11 CLEAR/REVIEW, feedback, labels and private synthetic truth never enter this projection. The local runner has no producer, consumer, server, enforcement or Registry-writing API.

Request lifecycle is accepted → validated → inference attempted → terminal result. An invalid request-local input yields `INPUT_REJECTED`; a known local inference failure yields `EXECUTION_FAILED`; a valid probability in `[0,1]` yields `SCORED`. Failure codes are stable and no exception text is published. A fundamental source, Registry, model or plan integrity failure aborts the whole run. No automatic retry or thread-based timeout exists. An identical repeated delivery within one run reuses its terminal logical result without rescoring; conflicting reuse of a request ID fails. This is local logical idempotence, not globally exactly-once execution. Every completed run reconciles terminal counts to unique planned requests and reports failures separately from full scoring success.

Output is `.local/ml/shadow-executions/<execution_id>/` with a canonical manifest, Parquet terminal rows, sanitized JSONL audit and compact report. The result rows contain request/event identities, sequence, candidate/version, status, optional FRAUD-class probability, optional failure code and source-row fingerprint; they contain no customer/device/IP, labels or risk disposition. SHA-256 semantic IDs and separate artifact hashes protect repeat inspection. Exclusive publication never overwrites prior evidence; incomplete artifacts fail inspection. Parity separately verifies the frozen Phase 19 ledger and compares event IDs, candidate, source-row fingerprint and probabilities with `math.isclose(rel_tol=1e-12, abs_tol=1e-12)`, reporting maximum absolute/relative differences. No Phase 19 bytes are changed.

Commands: `make build-shadow-replay-plan`, `make inspect-shadow-replay-plan`, `make execute-shadow-replay`, `make inspect-shadow-execution`, and `make verify-shadow-parity`. They require explicit fixed source/version/window and Registry version where applicable, and need no Docker, Spark, Kafka or MLflow HTTP server. These artifacts prove local request/execution contract mechanics only. They do not establish model calibration, production fraud quality, feature availability, latency SLOs, high availability or deployment eligibility. The Phase 18 dossier remains `NOT_ELIGIBLE_FOR_PROMOTION`.
