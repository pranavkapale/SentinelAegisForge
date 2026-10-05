# ADR-023: Independent delayed outcome labels

## Status

Accepted

## Context

Fraud outcomes become available after transaction ingestion and can be corrected. Deterministic CLEAR/REVIEW dispositions describe engineering policy, not ground truth. Using them as outcomes would create circular learning. No external outcome integration currently exists.

## Decision

Use independent typed `TransactionOutcomeLabel` records: UUID `event_id`, `Outcome.FRAUD` or `Outcome.LEGIT`, timezone-aware UTC `label_observed_at`, nonblank `label_source` and positive integer `label_revision`. Persist FRAUD as 1 and LEGIT as 0 with timestamp, source and revision retained.

`label_observed_at` means when the outcome became available to the control plane, not event/ingestion/scoring time. JSONL is the local input: one complete revision per line, exact contract fields, UTF-8, strict UTC RFC3339 seconds with optional 1–6 fractional digits and `Z` or `+00:00`. Reject naïve/non-UTC timestamps rather than converting their meaning; never truncate sub-microsecond precision. Normalize accepted datetimes to UTC. The persisted revision uses int64, so its range is 1 through 2^63-1.

For each event, the file must contain a complete contiguous history starting at revision 1, with unique revision numbers and nondecreasing observation timestamps. File order is irrelevant. Equal observation times are allowed; the higher revision wins once eligible. A malformed future revision still invalidates the input history.

At explicit AS_OF_TIME select only revisions observed at or before that time, then the highest eligible revision. Validate every revision for known features against `label_observed_at >= ingestion_time`, including future revisions. Unlabeled available events are excluded and counted, never turned into negative examples. Eligible labels without an available feature fail the build. Future labels for unknown events are not joined; they become an error if eligible in a later snapshot.

Local verification labels are explicitly `SYNTHETIC_FIXTURE`, not production fraud truth. Chargebacks, manual review and dispute integrations remain unimplemented. Outcome assignment is independent of risk dispositions and feature values.

## Alternatives considered

- REVIEW/CLEAR as labels: convenient and immediately available, but reproduces policy judgments rather than learning independent outcomes.
- Unlabeled as legitimate: increases population coverage but falsely equates incomplete observation with a negative outcome and introduces delayed-label bias.
- One immutable label per event: simpler joins, but cannot represent legitimate corrections or reconstruct what was known before them.

## Trade-offs

Complete revision histories and strict joins require source governance and can reject partial extracts. Point-in-time labels preserve knowledge boundaries, but excluding unlabeled events does not by itself remove selection bias or establish production label quality.

## Consequences

Tests gate delayed labels, future corrections, cardinality, causality and provenance. Snapshot class counts describe only the explicitly labeled population. No label generation from risk rules, label enrichment service, balancing, training or model-quality evaluation is introduced.
