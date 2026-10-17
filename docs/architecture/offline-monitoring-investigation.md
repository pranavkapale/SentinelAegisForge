# Offline monitoring investigation

Phase 21 adds an evidence-first manual triage layer; it does not alter any Phase 18–20 artifact:

```text
Phase 18 dossier (optional) ─┐
Phase 19 T1/T2 (optional) ────┼─→ existing inspectors → identity checks
Phase 20 drift (required) ────┘                         ↓
                                      deterministic findings + runbook
                                                    ↓
                                   immutable local investigation packet
                                                    X alerts / retraining / promotion
```

The required drift snapshot selects a fixed Phase 19 ledger, model candidate, Registry version, corrected statistical Delta table/version and two disjoint UTC **ingestion-time** cohorts. The existing inspectors verify byte and logical integrity, including source-to-ledger reconciliation. Optional T1/T2 snapshots must share that ledger, candidate, Registry and Delta identity and have ordered **label-observation AS_OF** cutoffs. Their existing comparison separates new labels, changed revisions, outcome flips and unchanged labels. An optional Phase 18 dossier is deep-inspected against the exact Registry version; it remains `NOT_ELIGIBLE_FOR_PROMOTION`. Missing optional evidence is reported, never silently replaced.

`offline-monitoring-investigation-v1` findings have a category, stable ID, factual observation, exact measurement context, references of `(source ID, artifact, contract version, field path)`, limitation and manual investigation steps. Numeric features follow the established input-contract order; categorical fields follow the Phase 20 schema order; currencies sort alphabetically. Ordering is for presentation, not severity. Available mean, KS, PSI, TVD, JS, category and null measurements are retained regardless of magnitude. Unavailable metrics preserve their status/reason; insufficient support is not a corrupted artifact or a zero distance.

The versioned `offline-monitoring-runbook-v1` asks an engineer to verify identities, population counts, statistical-history nulls/status, category composition, within-currency monetary behavior, frozen-score distributions, delayed-label support and governance evidence. Changes in `prior_amount_mean`, `prior_amount_stddev`, `amount_zscore` and `statistical_feature_status` motivate inspection of per-currency history, customer composition, Phase 10 state lifecycle and event-time versus observed order; no cause is asserted. Raw USD/EUR magnitudes are never a shared financial total. The model's probabilities are uncalibrated; the score-at-least-0.5 share is not fraud prevalence. A new categorical value does not automatically imply an encoder failure.

Phase 20 compares **different transaction populations** by ingestion time. Phase 19 compares the **same frozen predictions** under different delayed-label visibility cutoffs. AP or ROC changes can result from new/revised labels, changing support and nonrandom maturation; they are not automatically changed discrimination or caused by input distribution movement. Transaction event time, ingestion time, label observation time and evaluation AS_OF have distinct meanings.

Packets live under ignored `.local/ml/investigations/<investigation_id>/` and contain canonical `investigation_manifest.json`, `findings.json`, `investigation_report.md` and `runbook.md`. SHA-256 identity covers verified source identities/hashes, contract and logic versions, selected optional sources and report configuration. Separate artifact hashes protect bytes. Repeat construction inspects rather than rewrites; incomplete or conflicting packets fail closed. Inspection rechecks upstream sources and regenerates expected deterministic content. Paths and local hashes are not portable attestations or signatures.

Use `make build-monitoring-investigation INVESTIGATION_DRIFT_PATH=<absolute-snapshot>` and optionally `INVESTIGATION_EARLIER_PATH`, `INVESTIGATION_LATER_PATH`, `INVESTIGATION_DOSSIER_PATH`. The two Phase 19 paths must be supplied together. `make inspect-monitoring-investigation INVESTIGATION_PACKET_PATH=<absolute-packet>` is read-only. Dossier inspection uses the existing local SQLite Registry configuration; neither command requires its UI or a running server.

This is synthetic historical-replay evidence. There is no calibrated production baseline, root-cause proof, alert policy, incident record, real-world fraud-quality evidence, approval or automated action.
