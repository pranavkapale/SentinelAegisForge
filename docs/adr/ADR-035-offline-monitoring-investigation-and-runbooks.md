# ADR-035: Offline monitoring investigation and runbooks

## Status

Accepted

## Context

ADR-033 measures delayed outcomes against one frozen prediction ledger. ADR-034 separately measures feature and frozen-score distributions across two ingestion-time populations. These observations can support an engineer's investigation, but neither establishes a production reference, causal explanation or action threshold. Phase 18 still records a research-only, production-ineligible model.

## Decision

Publish `offline-monitoring-investigation-v1` packets from an explicitly selected, verified Phase 20 snapshot and its exact Phase 19 ledger. Two ordered, compatible Phase 19 outcome snapshots and an exact-version Phase 18 dossier are optional. Reuse the existing inspectors, then cross-check candidate, Registry, ledger, Delta source/version, feature contract and temporal scope before constructing findings. A corrupt or incompatible source is an integrity failure; an unavailable metric or absent optional source is an evidence limitation.

Findings retain measured values, availability reasons and field-level references. Deterministic presentation order is not severity. The versioned `offline-monitoring-runbook-v1` asks an engineer to inspect null/status history, category mix, within-currency monetary distributions, frozen-score changes, label maturity and governance gaps. It separates observed facts, possible avenues and unestablished causes. Phase 19 AS_OF changes reflect label availability for the same scores, while Phase 20 compares different ingestion-time populations; the two are not one causal sequence.

Canonical SHA-256 source and contract identity produces an immutable local packet with findings, report, runbook, source identities and byte hashes. A repeat verifies existing bytes; incomplete or conflicting output fails closed. Hashes are integrity checks, not signatures or independent source backups. The Registry and Phase 18 dossier are never mutated. The model remains `NOT_ELIGIBLE_FOR_PROMOTION`.

## Alternatives considered

- Alert from a fixed PSI/KS threshold: simple, but no representative baseline or validated alert policy exists.
- Retrain automatically on measured changes: could automate response later, but neither a cause nor production-label quality is established.
- Treat synthetic replay as an operational incident: useful for testing workflows, but misleading as production evidence.
- Add an orchestration service now: might support recurring reviews, but adds state and operations without an incident contract.
- Collapse Phase 19 and 20 metrics into one health score: compact, but combines incomparable temporal and statistical quantities without calibration.

## Trade-offs

Deep reinspection repeats local I/O and requires retained Delta, ledger, monitoring, dossier and Registry bytes when those contexts are included. Static runbook guidance cannot replace a human investigation. Missing history, population composition, label selection and currency mix may explain measurements, but the packet cannot prove any one cause. Same-currency monetary comparisons avoid false cross-currency financial totals; no FX normalization occurs.

## Consequences

An engineer can inspect an immutable, evidence-linked diagnostic packet without rescoring, training, Docker, Kafka or an MLflow server. No severity, alert, incident, retraining, promotion, serving or automated remediation is created. Synthetic offline observations remain non-production evidence.
