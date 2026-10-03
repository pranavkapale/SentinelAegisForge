# Deterministic explainable risk engine

## Boundary and input

```text
transaction_statistical_features_v2
        ↓ pure independent rule evaluation
ordered rule IDs + aligned reason codes
        ↓ no matches / any match
CLEAR / REVIEW
        ↓ retry-safe append
transaction_risk_decisions
```

The only input is the corrected 25-column Phase 10.1 table. Retain its event identity, customer, currency, event/ingestion times, exact amounts, Kafka lineage, rolling features and statistics unchanged. No Kafka read, lookup, feature recomputation or customer state exists in this stage. Phase 8 cumulative amounts and v1 feature tables are not decision inputs. Structural checks reject unexpected input schemas; operators must select a governed corrected-v2 source (the v1 schema shape alone cannot prove its semantic provenance).

Behavioral velocity is customer-wide. Monetary statistics are customer+currency-specific, denominated by the input `currency`. Neither rolling amount sums nor current amounts are compared to a global threshold. No FX conversion occurs.

## Policy configuration and rules

All five policy parameters are required: `POLICY_VERSION`, `HIGH_VELOCITY_THRESHOLD_5M`, `HIGH_AMOUNT_ZSCORE_THRESHOLD`, `COMBINED_VELOCITY_THRESHOLD_5M`, `COMBINED_AMOUNT_ZSCORE_THRESHOLD`. Counts must be >= 1 and z thresholds positive and finite. The immutable policy validates configuration before Spark starts; no policy defaults exist.

Evaluation order is always:

1. `R001_HIGH_TRANSACTION_VELOCITY_5M` / `HIGH_TRANSACTION_VELOCITY_5M`: `prior_transaction_count_5m >= highVelocityThreshold5m`.
2. `R002_HIGH_AMOUNT_ZSCORE` / `HIGH_AMOUNT_ZSCORE`: `READY` and present `amount_zscore >= highAmountZScoreThreshold`.
3. `R003_VELOCITY_AND_AMOUNT_ANOMALY` / `VELOCITY_AND_AMOUNT_ANOMALY`: `READY`, count `>= combinedVelocityThreshold5m` and z-score `>= combinedAmountZScoreThreshold`.

All independently matching rules survive, including overlap. Negative z-scores never become positive anomalies through absolute value. `CLEAR` means no configured rule matched, not guaranteed legitimacy or automatic approval. `REVIEW` means at least one advisory rule matched, not a payment block or implemented review workflow. There are no weights, severity sums, probabilities or composite risk scores.

## Statistical readiness and failures

The pure engine returns `Either[Vector[RiskInputError], RiskDecision]`. Counts cannot be negative; present mean/stddev/z must be finite; stddev cannot be negative. Context must match Phase 10:

- `NO_HISTORY`: count zero and no mean/stddev/z.
- `INSUFFICIENT_VARIANCE_HISTORY`: count one, mean present, stddev/z absent.
- `ZERO_VARIANCE`: count >= 2, mean and stddev present, stddev in `[0,1e-12]`, z absent.
- `READY`: count >= 2, mean/stddev/z present, stddev > Phase 10's `1e-12` numerical floor.

Unknown or inconsistent status is an explicit failure, never a fallback decision. Nonready rows with fake z-scores are rejected rather than allowed to trigger a rule. The Spark boundary also rejects null required counts/status. It raises a contextual invalid-feature error and fails the query; no DLQ or silent omission exists. The layer checks consistency, not a statistical recalculation of prior evidence.

## Determinism and policy identity

No clock, random value, I/O or mutable state enters rule evaluation. Persist both human `policy_version` and exact `policy_fingerprint`; SHA-256 is configuration identity, not a signature. [ADR-022](../adr/ADR-022-risk-policy-versioning.md) specifies the LF-terminated UTF-8 canonical representation including logic version, human version, ordered rules/reasons, gates, comparators, decision mapping and all thresholds. Hexadecimal double formatting protects exact threshold identity. Changing any threshold or human version changes the fingerprint. No `evaluated_at` wall-clock column is added.

## Durable output and recovery

The unpartitioned output has all 25 input columns followed by `risk_disposition`, `policy_version`, `policy_fingerprint`, `matched_rule_ids` (`array<string>`), `reason_codes` (`array<string>`) and `matched_rule_count` (`int`). CLEAR uses empty arrays and zero count. REVIEW carries ordered explanations and observed features, so the record is auditable without joins. Row encoding preserves source decimal types, including `decimal(38,4)` rolling sums; features are not mutated.

`TransactionRiskDecisionApp` reads Delta with `readStream`, maps independently and uses AvailableNow plus `foreachBatch`. There are no watermark, stateful operator, timers or RocksDB settings. The dedicated checkpoint tracks Delta source and sink progress; query diagnostics report input rows and zero state operators.

Defaults: `.local/delta/transaction_statistical_features_v2` source, `.local/delta/transaction_risk_decisions` target, `.local/checkpoints/risk-decisions-v1` checkpoint and `sentinel-transaction-risk-decisions-v1` sink application ID. Writes use `txnAppId + txnVersion=batchId`. A new checkpoint needs a new app ID. Policy change requires explicit lineage review and normally new version/fingerprint/checkpoint/app ID/output. Never edit Spark checkpoints or overwrite historical evaluations.

Grain is one deterministic policy evaluation per source feature row per policy identity. Event+policy is conceptual identity, not a lifetime enforced unique key. Same-checkpoint resume and same-app/batch retries are protected; fresh replay under the same policy may append another evaluation. No MERGE, historical replay implementation or exactly-once business claim exists.

## Local demonstration

The following policy is **development/verification only, NOT production-calibrated**:

```sh
make process-risk-decisions \
  POLICY_VERSION=phase11-verification-v1 \
  HIGH_VELOCITY_THRESHOLD_5M=3 \
  HIGH_AMOUNT_ZSCORE_THRESHOLD=2.0 \
  COMBINED_VELOCITY_THRESHOLD_5M=3 \
  COMBINED_AMOUNT_ZSCORE_THRESHOLD=1.0
make inspect-risk-decisions
```

Override source/output/checkpoint/application ID for an isolated experiment. Unit and temporary Delta tests run without Docker, including all three rules, readiness, invalid input/configuration, ordering, policy identity, source-evidence preservation, zero state operators, checkpoint resume and transactional sink retries. Live evidence belongs in PROJECT_STATE only after the real producer → Kafka → validated → deduplicated → rolling v2 → statistical v2 → risk path has passed.

## Non-guarantees and deferred work

No fraud-effectiveness or production-calibration claim, numeric risk score, ML/model serving, enforcement, review service, FX conversion, retroactive feature correction, policy replay or lifetime decision uniqueness exists. Production threshold calibration, fraud labels, operational policy promotion, review workflows, replay/idempotency across new checkpoints and any later model lifecycle remain separate decisions. This phase introduces no dependency and does not change earlier state/checkpoint contracts.
