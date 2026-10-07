# ADR-026: One offline logistic-regression baseline

## Status

Accepted

## Context

The first training implementation needs a transparent reference for preprocessing, temporal evaluation and provenance. The current live labeled corpus has four rows and cannot establish useful fraud-model quality.

## Decision

Use the Phase 12 `candidate_feature_columns` exactly. A training-only sklearn `Pipeline` binds `ColumnTransformer` to `LogisticRegression`. Currency, country, transaction type and statistical status use `OneHotEncoder(handle_unknown="ignore")`. Seven numeric columns use constant-zero `SimpleImputer(keep_empty_features=True)` followed by `StandardScaler`; exact snapshot decimals convert to floating model inputs only in this projection. The status category preserves the reason for missing statistical values. Learned encoder/scaler parameters come only from the eligible training cohort.

The fixed estimator is `LogisticRegression(solver="lbfgs", class_weight="balanced", C=1.0, tol=1e-4, max_iter=1000, fit_intercept=True)` with remaining library defaults recorded in each run manifest. `lbfgs` is deterministic for fixed inputs and does not use `random_state`; a seed would not add a real guarantee. Balanced class weighting changes fit contributions and does not resample or duplicate dataset rows. The classification threshold is fixed at 0.5, with no optimization.

Report validation and test separately. Average Precision is the primary ranking metric; ROC AUC, threshold precision/recall/F1, a 2×2 confusion matrix and class prevalence/support are also recorded. If no positive prediction exists, threshold precision is unavailable (`null`), rather than an invented numeric value. `predict_proba` gives a baseline probability estimate; no calibration is claimed.

Training contract `fraud-logistic-baseline-v1` uses a SHA-256 run ID over canonical dataset identity, splits, feature/preprocessing/estimator configuration, fixed threshold and runtime versions. Persist only an immutable manifest, separate metrics and validation/test predictions. The fitted pipeline stays in memory; no deployable model artifact exists yet.

## Alternatives considered

- Tree ensembles: can capture nonlinear relationships with less scaling, but add capacity and interpretation questions before the basic data/evaluation contract is proven.
- Gradient boosting, including LightGBM/XGBoost: strong tabular candidates that merit later comparison on representative data, but add dependencies and tuning choices now.
- Neural networks: flexible but unjustified for the current tiny labeled corpus and first baseline.

## Trade-offs

Logistic regression is limited in interactions and nonlinear effects. One-hot encoding can grow with categorical cardinality, though current candidates exclude high-cardinality IDs. Balanced weights and a 0.5 threshold are engineering settings, not a production operating point.

## Consequences

Controlled fixtures verify mechanics only. No fixture metric is evidence of fraud detection quality. Model artifact governance, MLflow, calibration, tuning, walk-forward evaluation, threshold policy, serving, drift and retraining remain future work.
