# ADR-033: Offline prediction ledger and delayed-outcome monitoring

## Status

Accepted

## Context

Phase 18 established that the exact research-only Registry version is technically traceable but not production-eligible. Delayed synthetic outcomes evolve after the transaction and after Phase 13 training. A repeatable offline evaluation needs to separate predictions made with a frozen model and fixed feature source from labels known at different observation cutoffs. Historical `ingestion_time` does not prove when a feature row was materialized, and the model was reconstructed later; replay is not evidence of historical online availability.

## Decision

Pin the numeric MLflow Registry version, trusted Phase 16 candidate, 25-column corrected statistical Delta table ID/version/schema, and explicit post-training UTC ingestion-time scoring window. Reuse the candidate's exact 11-feature projection and trusted loader. Persist one immutable, label-free `offline-prediction-ledger-v1` with deterministic row and ledger identities. Compare overlapping `(split,event_id)` FRAUD-class probabilities to the archived Phase 13 predictions at `1e-12` relative/absolute tolerance. Repeated builds verify rather than overwrite.

Separately, inspect the independent Phase 14 synthetic corpus and label history, reconcile all label IDs to the full fixed feature source, and resolve the highest eligible revision at an explicit UTC `evaluation_as_of >= score_end`. Unlabeled scored events remain in the coverage denominator but not quality metrics. Persist immutable `delayed-outcome-monitoring-v1` snapshots: point-in-time selected revisions, coverage, aggregate/segment support, quality metrics, report and artifact hashes. Ranking metrics are unavailable without configured row and both-class support. Compare snapshots by new labels versus revision changes and outcome flips; never rescore because a label matured. Metrics describe the labeled subset only and are conditional on label selection.

Identity hashes are deterministic canonical SHA-256 over source/model/contract/cutoff/configuration inputs; byte hashes separately protect artifacts. An incomplete or conflicting exclusive publication fails closed. These hashes are integrity checks, not signatures. Registry state and Phase 18 assessment stay unchanged; no metric initiates automated action.

## Alternatives considered

- Recompute predictions on each label update: may be useful for new-policy evaluation, but destroys the frozen-prediction comparison and confounds label maturation with model change.
- Treat absent labels as LEGIT: increases apparent coverage but invents negative outcomes and biases quality estimates.
- Evaluate only the Phase 12 labeled snapshot: reuses a convenient immutable artifact but excludes unlabeled scored events and cannot show prediction coverage or earlier revision knowledge independently.
- Use Phase 11 CLEAR/REVIEW as ground truth: available immediately, but circular and not an independent fraud outcome.
- Introduce Kafka model serving now: would enable online observations, but requires deployment, feature-availability, latency, operational and governance contracts the research model does not meet.

## Trade-offs

The local builder reads fixed Delta/source rows in memory and repeats trusted candidate and corpus checks. It requires retained local source/model/label bytes and does not offer a portable evidence vault, a concurrent global uniqueness guarantee or historical feature-materialization proof. Segment metrics on synthetic subsets do not establish fairness or real-world robustness. Outcome maturation may change measured quality solely through changing sample composition or corrected truth.

## Consequences

Offline Synthetic Historical Replay & Delayed-Outcome Evaluation is inspectable without Docker, a running MLflow server, model retraining or Registry mutation. It is not production monitoring, real shadow inference, model approval, drift alerting, retraining or fraud-performance validation. The current model remains research-only and production-ineligible.
