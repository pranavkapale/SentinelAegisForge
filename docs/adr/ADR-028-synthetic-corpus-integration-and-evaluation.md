# ADR-028: Synthetic corpus integration and evaluation

## Status

Accepted

## Context

A synthetic feature table written directly by Python would not exercise Kafka acknowledgements, registry-governed Avro, Spark event-time state or the corrected currency-safe feature pipeline. Multi-day data also requires deliberate watermark scheduling and isolated checkpoints. The existing Phase 12/13 validation gates must remain authoritative.

## Decision

Inspect the generated plan before publishing. Publish three bounded chronological waves through the existing Scala transaction producer, each with an exclusive attempt marker; implicit replay is refused. Every Kafka acknowledgement is inspected. Initialize a fresh `latest` Kafka-ingestion checkpoint before the first publication so preserved broker history is not silently mixed into the corpus. Use dedicated validated, deduplicated, rolling and statistical Delta paths, checkpoints and per-table transaction application IDs. Advance each wave through all stages before publishing the next. No existing Kafka volume or historical Phase 6–13 evidence is reset.

Reconcile generated event IDs against every required Delta boundary, compare validated Kafka coordinates to publisher acknowledgements, check row uniqueness and corrected rolling/statistical currency semantics, then use the actual fixed-version statistical Delta table in the unchanged Phase 12 snapshot builder. Independent delayed labels come from the private sidecar. Run the unchanged Phase 13 ingestion-time split and training-only preprocessing/estimator, with its conservative rule that selected labels observed after `train_end` are excluded from fitting. Record actual cohort support, exclusions, provenance and validation/test metrics under **Synthetic Scenario Evaluation — Not Production Fraud Performance**.

Ordinary `make verify` remains Docker-independent; real Kafka/Spark/Delta validation is an explicit, isolated workflow. Neither a successful acknowledgement nor Delta's batch transaction guard is an end-to-end exactly-once business guarantee.

## Alternatives considered

- Fabricate feature Delta rows directly: useful for unit tests but bypasses the actual stateful lineage and cannot prove full-path reconciliation.
- Use Phase 11 CLEAR/REVIEW as outcome labels: circular and inconsistent with independent delayed ground truth.
- Integrate an external public dataset immediately: valuable future evidence, but needs separate data-rights, mapping and bias analysis and does not replace a deterministic contract test.
- Publish the entire multi-day corpus before one streaming run: simpler operationally but risks watermark drops and distorted state histories under current event-time semantics.

## Trade-offs

Wave-by-wave execution takes more local runs and explicit paths. In return, watermark advancement is observable and source/sink identities can be audited. A publisher crash after partial delivery leaves an attempt marker and requires human investigation rather than an unsafe blind replay. The local fixed corpus size is not a throughput benchmark.

## Consequences

Counts alone are insufficient: ID sets and acknowledged Kafka coordinates must match. Missing events or late drops fail verification until explained. Phase 12/13 eligibility gates are not weakened to improve synthetic metrics. Scenario labels and model results are not production truth, a deployable model or evidence of fraud-detection performance.
