# ADR-016: Customer rolling feature semantics

## Status

Accepted

Monetary membership is refined for feature semantic v2 by [ADR-020](ADR-020-currency-safe-monetary-feature-semantics.md): sums use the current currency; velocity remains customer-wide. The original v1 decision below is retained as history, not the current monetary contract.

## Context

The Phase 7 deduplicated Delta table is the stable semantic input for per-transaction customer features. A current transaction must not contribute to features intended to describe its prior history. Input can arrive out of event-time order, and Spark does not guarantee a useful order for equal-time records in a customer iterator.

## Decision

Version 1 defines only `prior_transaction_count_5m` and `prior_amount_sum_10m`. For an event at `t`, each feature reads previously observed events of the same customer in `[t - window, t)`. The lower bound is inclusive; the current event and every event at `t` are excluded. The count window is five minutes. The exact decimal sum window is ten minutes and is stored as `decimal(38,4)`.

The processor orders input by event time and event ID, calculates all outputs in an equal-timestamp group from prior state, then inserts the group into state. State lookups filter by event time, so a stored event later than `t` cannot influence an out-of-order event at `t`. Event ID ordering makes output stable but cannot change feature values.

The output is online/as observed. A late eligible event can influence later calculations, but it does not rewrite feature rows already emitted. Retrospective recomputation and backfill remain separate decisions.

## Alternatives considered

- Include the current event: sometimes useful for post-transaction summaries, but leaks the transaction into its own prior-history features.
- Processing-time windows: simple to operate, but changes answers based on delivery delay rather than transaction event time.
- SQL window aggregates: concise for bounded datasets, but less direct for this explicit arrival-aware state and cleanup policy.
- Calculate after updating state: simple processor ordering, but violates the prior-only feature contract.

## Trade-offs

The rule is testable and prevents current-event and equal-time leakage. Arrival history still matters: online outputs are not perfect retrospective event-time reconstructions. These window definitions are versioned semantics, not tuning parameters.

## Consequences

Changing either window, the half-open bounds, feature-before-update order, or equal-time grouping requires feature-contract and checkpoint-compatibility review. Fraud scoring, unique merchant/device features, and statistical anomaly features are not part of this decision.
