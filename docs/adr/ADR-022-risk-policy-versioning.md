# ADR-022: Risk policy identity and evolution

## Status

Accepted

## Context

A human label alone cannot distinguish silently changed thresholds. Deterministic decisions need the exact effective policy identity and must remain auditable without decision-time timestamps or randomness.

## Decision

Persist human-managed `policy_version` and lowercase hexadecimal SHA-256 `policy_fingerprint`. The digest identifies configuration; it is not a security signature. Include the human version in the fingerprint.

Canonical input is UTF-8 with LF separators and a final LF, in this exact order:

1. `decision_logic_version=deterministic-risk-v1`
2. `policy_version=<UTF-8 byte length>:<version>`
3. `decision_mapping=no_matches:CLEAR;any_match:REVIEW`
4. R001 ID, reason and `prior_transaction_count_5m>=<decimal count>`, separated by `|`.
5. R002 ID, reason and `status=READY;z_present;amount_zscore>=<hex double>`.
6. R003 ID, reason and `status=READY;z_present;prior_transaction_count_5m>=<decimal count>;amount_zscore>=<hex double>`.

IDs and reasons are the ordered identifiers in ADR-021. Counts use base-ten `Long` representation; z thresholds use JDK `Double.toHexString` for exact finite numeric identity, not locale-dependent formatting. Versions must be nonblank, trimmed and contain no control characters. Counts must be at least one; z thresholds must be positive and finite. Any threshold or version change changes the effective digest. Changes to rule semantics, readiness or mapping require review and a decision-logic version change.

Historical decisions are append-only. Future intentional re-evaluation uses a new version/fingerprint; conceptual identity is business event plus policy identity, while physical grain is one evaluation per source feature row per policy identity. Changing policy requires explicit review of version, fingerprint, output path, checkpoint and transaction ID. Prefer a fresh checkpoint lineage with a new Delta application ID and separate output path, not checkpoint editing or overwriting historical decisions.

The initial default checkpoint is `.local/checkpoints/risk-decisions-v1`, output `.local/delta/transaction_risk_decisions` and application ID `sentinel-transaction-risk-decisions-v1`. Retrying the same app ID/batch ID suppresses another Delta append; resuming the same checkpoint consumes only new source data. A fresh replay under the same policy can append another evaluation. Neither the fingerprint nor these batch semantics enforce lifetime event+policy uniqueness. No MERGE or historical replay orchestration is implemented.

## Alternatives considered

- Version label only: easy to read, but cannot expose accidental threshold changes under a reused label.
- Fingerprint only: exact configuration identity, but poor human governance and release communication.
- Timestamp/random decision identity: distinguishes runs, but breaks deterministic replay and does not describe effective policy.
- Central configuration-management service: supports coordinated promotion, but is unnecessary before policy deployment requirements exist.

## Trade-offs

Canonical formatting must itself be versioned and tested. A digest alone does not reconstruct thresholds: the architecture document, effective application log and governed policy configuration describe them. Separate lineages cost storage and deliberate replay planning but preserve audit history.

## Consequences

Pure tests prove stable identity and sensitivity to all parameters. Durable decisions retain both identifiers and source evidence. No configuration service, security-signature claim, retrospective replay, decision uniqueness MERGE or end-to-end exactly-once guarantee is added.
