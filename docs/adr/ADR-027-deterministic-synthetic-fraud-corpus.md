# ADR-027: Deterministic synthetic fraud corpus

## Status

Accepted

## Context

The Phase 12/13 contracts need a larger, temporally varied labeled population for end-to-end validation. The existing four-row live label fixture deliberately fails training sufficiency. Synthetic outcomes must not be mistaken for external fraud truth, leak into transaction events, or be derived from the deterministic risk policy or model predictions.

## Decision

Use the versioned `synthetic-fraud-scenario-v1` Scala generator with explicit seed, UTC base time, bounded count/customer population, nine-day default horizon, USD/EUR mix and fixed behavior/label-delay profile versions. Generation reuses the validated transaction candidate contract and existing deterministic UUID source. A seeded latent profile and probabilistic outcome draw produce correlated but intentionally overlapping FRAUD/LEGIT outcomes; customer identity itself is not a fraud label. No wall clock or machine-local randomness determines logical content.

Write an inspectable transaction plan separately from private synthetic truth and delayed-label JSONL. They share only `event_id`. No outcome, profile or label metadata enters the canonical Avro transaction value, Kafka key, rolling/statistical state or Phase 13 model candidates. Deterministic SHA-256 hashes identify the configuration, plan, truth, labels and corpus. Generated artifacts live under ignored local storage, not in Git.

The local default is 1,500 transactions across three chronological waves; configuration bounds cap it at 2,000. Labels include short/long delays, some observations unavailable at the chosen AS_OF time, and a small set of contiguous outcome revisions. These are **synthetic scenario truth**, not verified fraud outcomes.

## Alternatives considered

- Derive labels from Phase 11 rules: convenient but circular, making risk-rule outputs the learning target rather than independent outcomes.
- Assign a fixed label per customer: easy to generate, but unrealistic for event-level outcomes and risks identity leakage.
- Import a public fraud dataset now: potentially more externally grounded, but would require separate license, provenance, event-contract, time and delayed-label mapping decisions; it would not prove this repository's controlled pipeline behavior.

## Trade-offs

The compact simulator is reproducible and directly tests existing contracts, but its latent process is deliberately simplified. Signal overlap avoids a trivially separable threshold task, yet it does not make the data representative of real payment fraud. Versioned private sidecars and hashes add small local artifact-management overhead.

## Consequences

The same configuration reproduces the same logical plan, outcomes and corpus identity; changing the seed changes them. Model metrics on this corpus validate pipeline/evaluation mechanics only. Real ground truth, population bias, calibration and production fraud quality remain unproven. The transaction Avro contract and risk engine are unchanged.
