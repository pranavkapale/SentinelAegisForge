# ADR-020: Currency-safe monetary feature semantics

## Status

Accepted

## Context

The v1 transaction contract accepts multiple currency strings. Phase 9 originally summed customer amounts without currency membership, and Phase 10 originally kept one customer-wide Welford baseline. Raw USD and EUR amounts are different monetary units; adding or statistically comparing them is dimensionally invalid. The verified time, observation-order, and retry boundaries must remain intact before decisioning begins.

## Decision

Feature semantic v2 retains customer-wide behavioral velocity: `prior_transaction_count_5m` counts all prior customer events in `[t-5m,t)`, regardless of currency. `prior_amount_sum_10m` includes only prior events in `[t-10m,t)` whose currency equals the current transaction's currency. The output sum is denominated in that existing `currency` column. Phase 9 remains keyed by `customer_id`; each retained event stores event-time microseconds, exact amount, and currency.

Phase 10 groups by explicit `CustomerCurrencyKey(customerId, currency)`. Each pair has one constant-sized Welford ValueState and its own latest event time, inactivity timer, Kafka partition, and last observed offset. Prior count, mean, sample standard deviation, z-score, and readiness status use only previously observed amounts for that pair. Scoring still precedes update. USD state can expire or reactivate independently while EUR state stays active.

Kafka remains keyed by UTF-8 `customer_id`. Currency switching is not a partition change. Offsets must advance among observations belonging to each pair; gaps caused by other currencies are valid. Existing partition-lineage checks remain enforced per pair.

Use new `customer-rolling-features-v2` and `customer-statistical-features-v2` checkpoint defaults, dedicated `sentinel-transaction-customer-features-v2` and `sentinel-transaction-statistical-features-v2` transaction IDs, and fresh `_v2` output paths. The 20- and 25-column schemas remain structurally unchanged, but feature semantics are versioned. Never resume v1 state with v2 code, mutate checkpoint files, or append corrected rows into a v1 feature table. Existing v1 data/checkpoints and historical verification remain untouched; they are superseded for new monetary-feature runs. A further new checkpoint lineage also needs a new transaction ID.

This refines ADR-016/017 monetary membership/state and ADR-018/019 statistical identity/lifecycle. Same-time exclusion, half-open bounds, out-of-order behavior, prior-observed ordering, numerical guards, and no-retroactive-correction semantics remain unchanged. The transaction `schema_version` stays 1; it is not the feature semantic version.

## Alternatives considered

- Raw cross-currency aggregation: simple and customer-local, but lacks a coherent monetary unit and is rejected.
- A single supported platform currency: simplifies monetary features, but would narrow the existing multi-currency contract and discard valid events without an adopted business requirement.
- FX-normalized base currency: enables cross-currency comparison, but needs authoritative rates, effective timestamps, precision, provenance, and missing-rate policy. Those are separate domain decisions, not available here.
- Customer+currency keys for all features: uniform grouping, but changes velocity into currency-specific counts, contrary to the customer-wide behavioral feature.

## Trade-offs

Phase 9 retains one extra string per event without duplicating queries or state. Phase 10 retains one constant-sized value per active pair rather than per active customer; the total number of active pairs still matters. Monetary features cannot compare currencies or provide an FX-normalized customer total. Fresh lineages require intentional reprocessing and separate output storage, but avoid silently mixing incompatible state and feature meanings.

## Consequences

Mixed-currency processor, independent-expiry, checkpoint-recovery, and full-path live evidence gate this correction. Existing transaction currency-format validation is unchanged; equality is not ISO-reference validation. No FX dependency, new output column, risk rule, score, decision, retrospective rewrite, or performance tuning is introduced.
