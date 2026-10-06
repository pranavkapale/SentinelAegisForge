# Offline ML dataset contract and delayed labels

## Current boundary

```text
transaction_statistical_features_v2
        ↓ fixed Delta version → Arrow
ingestion_time <= explicit AS_OF_TIME
        ↓ independent JSONL outcomes
latest eligible label revision per event_id
        ↓ unlabeled excluded; strict one-to-one join
ordered labeled dataset.parquet
        ↓
immutable manifest + fingerprints
```

Module B now reads local corrected feature tables through delta-rs, without PySpark, pandas or a model. Module A remains unchanged. `transaction_risk_decisions` is not an input: CLEAR/REVIEW is engineering policy, never FRAUD/LEGIT ground truth. The source schema must be the complete ordered 25-column Phase 10.1 contract, not the 31-column risk output. Historical v1 feature schemas have the same shape, so semantic provenance must come from a governed corrected-v2 path/lineage, not schema inference.

## Feature provenance and validation

Capture source path, Delta table ID/version, protocol, row count and schema fingerprint before building. An opened Delta handle stays at its captured version. Use `--source-version` to rebuild a historical version; do not update the handle during a build or copy underlying source Parquet outside Delta. Unsupported protocols fail explicitly. Local compatibility was observed against the actual Scala Delta 4.4.0 table, not inferred from package versions.

Require unique canonical UUID event IDs in the **entire selected Delta version**, even among rows excluded by AS_OF_TIME. Replay ambiguity fails; there is no keep-first/latest policy. Validate required values, positive decimal(18,4) transaction amounts, nonnegative decimal(38,4) rolling sums, nonnegative feature counts and Phase 10 readiness: NO_HISTORY has count zero/no statistics; INSUFFICIENT has count one/mean only; ZERO_VARIANCE has count >= 2/mean/stddev in [0,1e-12]/no z-score; READY has count >= 2/finite mean/stddev > 1e-12/finite z-score. Any positive prior observation count requires a finite, strictly positive mean because Phase 10.1 observes positive amounts. For READY, validate the stored z-score against `(float(exact current amount) - prior mean) / prior stddev` using `math.isclose(rel_tol=1e-9, abs_tol=1e-9)`. These tolerances accommodate Double roundoff, including results near zero; materially inconsistent values fail. This comparison does not replace the stored z-score or convert the persisted exact decimal amount. Reject nonfinite values or corrupt null combinations. Preserve all fields, money and currency without FX or recalculation.

## Independent label semantics

One UTF-8 JSONL line is one `TransactionOutcomeLabel` revision:

```json
{"event_id":"275afd8d-506d-4344-9070-5a69aea4815d","outcome":"LEGIT","label_observed_at":"2030-01-03T00:00:00Z","label_source":"SYNTHETIC_FIXTURE","label_revision":1}
```

`event_id` joins the business occurrence, not the transaction ID or Kafka coordinates. Outcomes are typed FRAUD/LEGIT; output uses 1/0 respectively and retains observed-at/source/revision. `label_observed_at` is the UTC time the outcome became available to the control plane, distinct from transaction event, ingestion or scoring times. Require nonblank provenance. Strict UTC timestamps accept Z/+00:00 and at most six fractional digits; reject naïve/non-UTC times or excess precision. Missing/extra/duplicate JSON fields and invalid revisions fail.

Complete event histories use unique contiguous revisions 1..N with nondecreasing observation timestamps. File order does not choose the winner; equal observation times resolve to the higher revision. Future revisions are validated but not selected before their observation time. Every revision for a known event must be observed no earlier than ingestion.

No chargeback, dispute or investigation integration exists. Live fixtures remain ephemeral and explicitly SYNTHETIC_FIXTURE. Neither labels nor their availability times are derived from velocity, z-score or policy decisions.

## Point-in-time joins and unlabeled behavior

AS_OF_TIME is mandatory, timezone-aware UTC and never defaults to now. Select source rows with ingestion_time <= AS_OF_TIME. Select labels with label_observed_at <= AS_OF_TIME, then highest eligible revision. These comparisons include the exact boundary. Included rows form a one-feature-to-one-label join. Any eligible label without an available feature fails; future unknown labels remain in input provenance but are not joined yet.

No eligible label means exclusion, not LEGIT. A delayed FRAUD label known Jan 5 cannot enter a Jan 3 snapshot. A LEGIT revision known Jan 3 and FRAUD correction known Jan 8 produce revision 1 at AS_OF Jan 5, revision 2 at Jan 10. The earlier snapshot is never rewritten. All labels, including future revisions, participate in the exact file-byte hash: changing future input can change snapshot identity while earlier selected logical rows stay unchanged. Hash provenance is not future-label leakage into features/targets.

## Dataset identity, output and manifest

Contract version `transaction-fraud-v1` is separate from transaction schema_version, Delta version, risk policy version and label revision. Output preserves 25 source columns plus fraud_label (int8), label_observed_at (UTC microseconds), label_source (string), label_revision (int64), sorted by event_time then event_id. Monetary decimals are never converted to float. Existing statistical doubles remain approximations.

Default root: `.local/ml/datasets/transaction-fraud-v1/<snapshot_id>/`, ignored by Git. Each directory contains only dataset.parquet and manifest.json. Snapshot identity is SHA-256 over the canonical immutable inputs specified in [ADR-024](../adr/ADR-024-offline-ml-dataset-snapshots.md). Source and label paths normalize absolute/symlink aliases; path relocation and runtime changes intentionally affect provenance identity. The source table ID is read from Delta. No wall clock or newly generated random identity is used.

The manifest records contract/algorithm versions, snapshot ID, AS_OF_TIME, source path/ID/version/protocol/schema fingerprint, label path/byte hash, full/available source rows, duplicate IDs, total/eligible label revisions, selected labels, labeled/class counts, unlabeled exclusions/unmatched labels, event-time range, candidate/non-feature declarations and Python/Delta/Arrow versions.

Logical fingerprint covers the output schema plus every ordered logical row: exact decimal text, UTC microsecond timestamp text, exact hexadecimal double representation, and JSON strings/integers/null. [ADR-024](../adr/ADR-024-offline-ml-dataset-snapshots.md) defines canonical JSON and schema hashing precisely. Parquet uses pinned writer settings; its SHA-256 is an integrity check, not a cross-version byte-determinism promise. Existing artifacts must match both logical semantics and recorded file integrity; rebuilding does not overwrite. Incomplete/conflicting artifacts fail and require intentional investigation. Publication is not a multi-file transactional service; concurrent/incomplete builds are rejected rather than automatically repaired.

## Candidate model columns, not automatic training inputs

The declared allowlist is amount, currency, country, transaction_type, prior_transaction_count_5m, prior_amount_sum_10m, prior_amount_observation_count, prior_amount_mean, prior_amount_stddev, amount_zscore and statistical_feature_status. Future encoding and currency-aware model interpretation are separate decisions; this phase does not transform or train on them.

All IDs, event/ingestion timestamps, schema_version, Kafka key/topic/partition/offset/timestamp and label fields are **not baseline candidate features**. They remain for audit and future temporal splitting. merchant_id, device_id and ip_address specifically require later cardinality/encoding/leakage analysis. Risk dispositions, matched rules, reason codes and policy identity neither appear in this source/dataset nor belong in candidate inputs.

## Local commands

```sh
make build-ml-dataset \
  ML_FEATURES_DELTA_PATH=/absolute/path/transaction_statistical_features_v2 \
  ML_LABELS_PATH=/absolute/path/independent-labels.jsonl \
  ML_AS_OF_TIME=2030-01-10T00:00:00Z \
  ML_SOURCE_DELTA_VERSION=0
make inspect-ml-dataset ML_DATASET_SNAPSHOT_PATH=/absolute/path/<snapshot_id>
```

ML_SOURCE_DELTA_VERSION is optional: absent means capture the latest version **once**, not an unrecorded mutable read. ML_DATASET_OUTPUT_ROOT overrides storage location without changing identity. The CLI is `python -m sentinelaegisforge_control_plane.datasets.cli build|inspect`. Targets do not start Scala or Docker. The package's pytest/Ruff/mypy checks remain Docker-independent.

## Guarantees and non-guarantees

Demonstrated guarantees: explicit fixed source version, unique input event identity, strict causal label history, no future-label selection, no unlabeled negatives, stable join/order/identity, exact decimal preservation, manifest provenance and immutable snapshot verification.

Not guaranteed: external ground-truth quality, complete population labels, absence of labeled-population selection bias, reconstructed feature materialization availability at historical AS_OF_TIME, retroactive feature correction, historical Delta availability after vacuum, cross-version Parquet bytes, or model quality. AS_OF semantics use stored ingestion/label times and existing online/as-observed features; they do not recompute historical Welford or rolling state.

No model, training, MLflow, model registry, random split, balancing, drift, retraining, serving or evaluation metrics exist. Time-based training/validation/test boundaries and the remaining ML lifecycle stay deferred.
