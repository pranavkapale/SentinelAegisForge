# ADR-021: Deterministic risk policy

## Status

Accepted

## Context

The corrected Phase 10.1 boundary carries customer-wide velocity and customer+currency statistical context. An explainable decision layer is needed before ML, calibrated policy or enforcement exists. Phase 8's currency-ambiguous diagnostic total is not a monetary decision input.

## Decision

Consume only `transaction_statistical_features_v2`. Evaluate each row independently with a pure engine and explicit immutable policy. Preserve all source columns and append rule explanations to an unpartitioned `transaction_risk_decisions` Delta table.

Canonical rule order is R001/R002/R003:

- `R001_HIGH_TRANSACTION_VELOCITY_5M`: customer-wide `prior_transaction_count_5m >= highVelocityThreshold5m`; reason `HIGH_TRANSACTION_VELOCITY_5M`.
- `R002_HIGH_AMOUNT_ZSCORE`: `READY`, present positive-side `amount_zscore >= highAmountZScoreThreshold`; reason `HIGH_AMOUNT_ZSCORE`.
- `R003_VELOCITY_AND_AMOUNT_ANOMALY`: `READY`, count `>= combinedVelocityThreshold5m` and z-score `>= combinedAmountZScoreThreshold`; reason `VELOCITY_AND_AMOUNT_ANOMALY`.

Retain every overlapping match in canonical order. No matches means `CLEAR`; any match means `REVIEW`. These are advisory dispositions, not approval, blocking or fraud probability. There is no numeric composite score, absolute-z rule, raw amount threshold or history recomputation. Reject malformed/inconsistent readiness instead of falling back to a disposition. Z-score readiness must agree with the Phase 10 count, mean, standard deviation and numerical floor.

The AvailableNow application maps a Delta stream without watermarks, grouping, state or RocksDB. Its separate checkpoint and sink use `foreachBatch`, dedicated `txnAppId` and `txnVersion=batchId`. Policy identity/evolution is governed by ADR-022. Required runtime policy parameters have no defaults. Demonstration thresholds are engineering fixtures, not production calibration.

## Alternatives considered

- Weighted numeric score: compact ranking, but weights/scale need a defined calibration purpose and obscure individual reasons without further governance.
- Single monolithic boolean expression: simple for a small policy, but loses separately identifiable overlapping explanations.
- Immediate ML model: can capture richer patterns, but requires labels, training, evaluation and lifecycle controls not yet implemented.
- External rules engine: useful for large dynamic policy systems, but introduces service/runtime complexity unnecessary for three deterministic rules.

## Trade-offs

Fixed rules are inspectable and replayable but do not establish fraud effectiveness. Strict feature validation stops bad inputs rather than silently proceeding. Positive anomaly rules intentionally ignore unusually low amounts. Customer-wide velocity and currency-specific z-scores are combined without cross-currency money comparison.

## Consequences

Persist source evidence, policy identity, ordered rule IDs and aligned reason-code arrays. No model, score, payment side effect or new stateful processor is introduced. Production threshold calibration, review workflow and any enforcement contract remain separate future work.
