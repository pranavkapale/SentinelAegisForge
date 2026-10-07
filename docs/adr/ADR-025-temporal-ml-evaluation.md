# ADR-025: Ingestion-time baseline evaluation

## Status

Accepted

## Context

Phase 12 supplies immutable, independently labeled snapshots. Phase 10.1 statistics are prior-observed rather than strictly event-time ordered: an out-of-order transaction can have an older `event_time` but a later online observation. Delayed labels also make some historical training-period transactions unavailable for fitting at a proposed training cutoff.

## Decision

Use explicit UTC `train_end < validation_end < test_end <= snapshot AS_OF_TIME`. Split by `ingestion_time`: train `< train_end`; validation `[train_end, validation_end)`; test `[validation_end, test_end)`. Rows at or after `test_end` are excluded. No random split, row-percentage split or random cross-validation is used.

Only training-period rows with `label_observed_at <= train_end` enter fitting. Count the excluded delayed labels separately; never turn them into LEGIT. Validation and test are retrospective evaluation cohorts with labels selected by the Phase 12 AS_OF_TIME. Require positive configurable row minima (defaults 20/10/10) and both FRAUD and LEGIT in every cohort. The test cohort never enters fitting, preprocessing fit, parameter selection or threshold selection.

`event_time` is transaction-domain chronology. `ingestion_time` is the existing online observation/availability clock for this baseline. The stored ingestion timestamp is the explicit available chronology; this phase does not prove a separate feature-materialization timestamp. A future walk-forward design may add that evidence.

## Alternatives considered

- Event-time split: useful for event-domain chronology, but would not match Phase 10.1's prior-observed statistical state when events arrive out of event-time order.
- Random stratified split: can preserve class ratios, but mixes later observations into fitting and obscures deployment chronology.
- Kafka-offset split: reflects partition-local transport sequence but has no global ordering across partitions and is tied to broker topology.

## Trade-offs

Explicit chronological windows can have too few labels or one class, causing an intentional training rejection. Fit-label exclusions shrink the training cohort. A single split is a simple baseline, not a robust estimate across changing periods.

## Consequences

Training manifests record the boundaries, support per cohort, delayed fit exclusions and the exact Phase 12 dataset identity. Future walk-forward evaluation, label-maturity policy and representative population analysis require separate decisions.
