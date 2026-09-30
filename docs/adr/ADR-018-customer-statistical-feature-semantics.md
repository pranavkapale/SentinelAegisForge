# ADR-018: Customer statistical feature semantics

## Status

Accepted

## Context

The Phase 9 table already contains the original transaction, Kafka lineage, and prior-only event-time rolling features. A later decision engine needs a per-transaction statistical baseline without letting the current amount influence its own score. Late event-time arrivals make online observation order different from historical event-time order.

## Decision

Phase 10 reads `transaction_customer_features` and emits one `transaction_statistical_features` row per accepted source row. A customer-keyed Welford state holds prior count, mean, and M2. The current amount is scored from that prior state, emitted, then incorporated using Welford's update. Variance is the sample variance `M2 / (count - 1)` for count at least two. Mean is absent at count zero; standard deviation is absent below count two. A zero or numerically negligible standard deviation produces no z-score and an explicit `ZERO_VARIANCE` status, never infinity. Other statuses are `NO_HISTORY`, `INSUFFICIENT_VARIANCE_HISTORY`, and `READY`.

The baseline is previously **observed** customer amounts in the active statistical lifecycle, ordered by Kafka partition/offset, not all earlier `event_time` values. A late 12:05 record observed after 12:10 may use 12:10 in its prior baseline. This does not leak processing-time future information: 12:10 was already observed. Phase 9 deliberately uses different event-time window semantics. Previously emitted rows are never corrected retroactively.

The original amount remains exact `decimal(18,4)` and the Phase 9 sum remains exact `decimal(38,4)`. Statistical mean, M2, standard deviation, and z-score use checked finite `Double` approximations. Only variance in `[-1e-12, 0)` is clamped to zero as numerical roundoff; a more negative or non-finite value fails. Standard deviations at most `1e-12` are numerically zero for z-score purposes. These are numerical guards, not anomaly-decision thresholds.

## Alternatives considered

- Naive sum and sum-of-squares: compact, but vulnerable to cancellation for close large amounts.
- Retrospective event-time reconstruction: useful offline, but would require historical recomputation and corrections, not online prior-observed scoring.
- Rolling-window variance: useful for a different feature, but requires event history and different state/lifecycle semantics.
- Offline recomputation: appropriate for backfills or analysis, but not the current append-only online query.

## Trade-offs

Welford uses constant-sized state and avoids current-event leakage. Floating-point statistical values are approximate. Results depend on accepted observation order and lifecycle expiry; they are not lifetime or retrospectively corrected statistics.

## Consequences

The eventual risk engine may consume these features, but Phase 10 defines no threshold, fraud flag, or decision. Any change to scoring order, variance convention, numerical guards, or output schema needs contract and checkpoint compatibility review.
