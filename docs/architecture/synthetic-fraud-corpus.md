# Deterministic synthetic fraud corpus (Phase 14)

## Boundary and purpose

The versioned `synthetic-fraud-scenario-v1` generator proves the existing event, streaming-feature, delayed-label and temporal-training contracts with a controlled population. It does **not** establish real fraud quality.

```text
explicit scenario configuration
    ├── transaction plan → existing Scala producer → Registry/Kafka
    │       → validated Delta → deduplicated Delta → rolling v2 Delta
    │       → statistical v2 Delta ──────────────────────────────┐
    └── private latent truth → delayed-label JSONL ─────────────┤
                                                               ↓
                                                  Phase 12 immutable snapshot
                                                               ↓
                                                  Phase 13 temporal baseline
```

Plan and truth are separate local artifacts joined only by `event_id`. No outcome or latent profile is added to the transaction Avro schema, Kafka payload/key or feature state. Phase 11 decisions are not inputs to the simulator's outcome draw, label resolver or estimator. Labels are **synthetic scenario truth**, not independently observed payment outcomes.

## Scenario contract and population

Configuration includes a contract version, seed, UTC microsecond-aligned base time, transaction count, customer count, horizon days, USD weight, fixed behavior-profile version and fixed label-delay-profile version. Defaults are 1,500 events, 90 customers, nine days and a 65% regular-activity USD draw; bounds reject fewer than 30 or more than 2,000 events, more than 300 customers, horizons outside 3–30 days or a single-currency weight. Generation uses the existing deterministic candidate generator for IDs, the Phase 1 validator for every event, and a seeded Scala PRNG for the remaining plan and latent outcomes. `Instant.now()` and random UUID generation do not determine content.

The plan has recurring customers, 20-event groups with short mixed-currency bursts, regular activity, occasional large amounts and a later-customer cohort. Some burst events have eligible out-of-order `event_time` while `ingestion_time` stays monotone; timestamps are explicit UTC microsecond values rather than host publication time. The first controlled customer has USD/EUR/USD events, which exercise customer-wide velocity and same-currency monetary baselines. USD and EUR are valid contract strings, not FX-converted units. Customer IDs, latent profiles and risk decisions are not candidate model features.

Latent `REGULAR`, `BURST`, `LARGE` and optional `CAMPAIGN` behavior changes a seeded probabilistic event-outcome draw. FRAUD and LEGIT overlap within behavior profiles; neither a z-score threshold nor CLEAR/REVIEW determines outcome. These simplified probabilities are simulation mechanics, not calibrated fraud rates.

The private truth sidecar records final outcome, initial outcome, profile, first observation, optional correction time, seed and contract version. JSONL labels carry event ID, FRAUD/LEGIT, observed-at time, `SYNTHETIC_SCENARIO_V1` provenance and contiguous positive revision. First observations are deterministically delayed 6, 72 or 720 hours; a small selected population has a later revision. The chosen AS_OF instant leaves some rows unlabeled, which Phase 12 excludes rather than treating as LEGIT. Phase 13 retains its conservative training-cutoff policy: a selected later revision observed after `train_end` makes that training-period row ineligible for fitting.

## Artifacts, identity and publishing

Ignored `.local/ml/scenarios/<corpus_id>/` contains `plan.tsv`, `truth.tsv`, `labels.jsonl` and `manifest.tsv`. The manifest records version/configuration, seed, population, currency/latent-outcome counts, event/ingestion ranges, label availability/revisions, wave ranges, runtime versions and SHA-256 artifact hashes. Canonically sorted configuration and artifact hashes determine `corpus_id`. The same configuration reproduces logical content and identity; changing the seed changes the corpus. Manifest/runtime version differences are inspected, never silently overwritten. All generated data stays outside Git.

The existing Scala producer publishes one bounded 500-event wave at a time, with customer-ID Kafka keys, canonical Registry-backed Avro values and the existing acknowledgement/idempotence settings. Each wave has an exclusive attempt marker and a detailed acknowledgement report. Implicitly rerunning a publish attempt is refused; partial delivery requires inspection, not a whole-corpus retry. This is not an exactly-once business-publishing guarantee.

For a live run, create a fresh isolated lineage and initialize its Kafka→validated checkpoint at `latest` **before** publishing; the existing broker volume may contain unrelated history. Use unique Delta paths, checkpoints and transaction application IDs for validated, deduplicated, rolling v2 and statistical v2 stages. Complete all four stages for wave 0 before publishing wave 1, then wave 2. For the verified nine-day example, wave ranges were Jan 1–3, Jan 4–6 and Jan 7–9, 2030 UTC. Their earliest event times were newer than the previous observed 10-minute watermark. Do not change production watermark policy to accommodate a corpus. If IDs are lost or late-drop metrics are nonzero, investigate instead of assuming all planned rows materialized.

`make generate-synthetic-corpus`, `make inspect-synthetic-corpus`, `make scenario-quality`, `make publish-synthetic-wave` and `make reconcile-synthetic-corpus` are focused developer interfaces. Generation/inspection/quality tests do not need Docker. Publication explicitly requires the local Kafka/Registry infrastructure; normal `make verify` does not start it. The Makefile's existing ingestion/feature targets accept path, checkpoint and application-ID overrides for each isolated wave.

For an isolated experiment, choose `CORPUS` as the generated directory and `RUN` as a fresh ignored `.local/phase14/<corpus_id>` directory. Initialize ingestion before publishing with `make ingest-delta STARTING_OFFSETS=latest DELTA_PATH="$RUN/validated" DELTA_CHECKPOINT_DIR="$RUN/checkpoints/validated" DELTA_TXN_APP_ID=<unique-validated-id>`. This first run should see no new corpus rows. For each wave `0`, `1`, then `2`, call `make publish-synthetic-wave SCENARIO_CORPUS_PATH="$CORPUS" SCENARIO_WAVE=<wave>` and rerun the same ingestion checkpoint/application ID. Then run the existing `deduplicate-transactions`, `process-rolling-features` and `process-statistical-features` targets in that order, overriding their source/target paths, dedicated checkpoint and dedicated transaction application ID with the same values for each subsequent wave. Do not reuse earlier phase checkpoint lineages or application IDs. Finally call `make reconcile-synthetic-corpus` with the corpus and all four Delta paths. Stop on any failure; a publish attempt marker is not permission to blindly rerun a partially delivered wave.

## Reconciliation and evaluation

Reconciliation compares all generated event IDs with every required Delta table, verifies no duplicates/unexpected IDs, checks publisher acknowledgement coordinates against validated Kafka coordinates, and invokes Phase 12's strict 25-column statistical source validation. It compares every rolling count and same-currency amount sum to the scenario plan using Phase 9's event-time ordering within each wave, plus the controlled first USD/EUR/USD statistical example. Aggregate counts alone cannot prove this.

Only after reconciliation, pin a statistical Delta version and build a Phase 12 immutable snapshot with the independent labels. Phase 12 preserves exact `decimal(18,4)` amounts, source lineage, label revisions and the feature allowlist. The selected AS_OF and explicit UTC train/validation/test boundaries determine actual eligible support. Phase 13 fits preprocessing only on eligible training rows; validation/test are evaluation-only. Its estimator configuration, class weighting and 0.5 classification threshold are unchanged. Report Average Precision, ROC AUC, precision, recall, F1, confusion counts and prevalence as **Synthetic Scenario Evaluation — Not Production Fraud Performance**. No metric target or scenario tuning based on results is used.

## Guarantees and limits

The verified local workflow can show reproducible plan/truth bytes, independent delayed-label histories, all-acknowledged publishing, ID/coordinate agreement at each durable stage, currency-safe features, an immutable snapshot and a temporally eligible baseline run. It does not prove representative fraud performance, calibrated probabilities, production throughput, no source-population bias, lifetime duplicate exclusion, end-to-end exactly-once processing, historical label reconstruction at every cutoff, or deployable model governance. MLflow, registry, serving, drift, retraining and payment action remain deferred.
