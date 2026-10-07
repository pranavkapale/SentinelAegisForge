"""One fixed sklearn baseline; fit operations receive training rows only."""

import math
from decimal import Decimal
from typing import cast

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, confusion_matrix, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .errors import TrainingContractError
from .temporal import (
    CATEGORICAL_FEATURE_COLUMNS,
    CLASSIFICATION_THRESHOLD,
    NUMERIC_FEATURE_COLUMNS,
)

PREPROCESSING_CONTRACT = {
    "categorical": {"transformer": "OneHotEncoder", "handle_unknown": "ignore"},
    "numeric": {
        "imputer": "SimpleImputer",
        "strategy": "constant",
        "fill_value": 0.0,
        "keep_empty_features": True,
        "scaler": "StandardScaler",
        "with_mean": True,
        "with_std": True,
    },
    "column_transformer": {"remainder": "drop", "sparse_threshold": 1.0},
    "decimal_to_model_float": ["amount", "prior_amount_sum_10m"],
}
ESTIMATOR_CLASS = "sklearn.linear_model.LogisticRegression"


def feature_frame(rows: tuple[dict[str, object], ...], candidates: tuple[str, ...]) -> pd.DataFrame:
    """Project the exact allowlist before converting money for the model matrix."""
    if set(candidates) != set(CATEGORICAL_FEATURE_COLUMNS) | set(NUMERIC_FEATURE_COLUMNS):
        raise TrainingContractError("Candidate columns do not match baseline preprocessing")
    frame = pd.DataFrame(
        [{name: row[name] for name in candidates} for row in rows], columns=candidates
    )
    for name in NUMERIC_FEATURE_COLUMNS:
        if name in ("amount", "prior_amount_sum_10m"):
            if not all(isinstance(value, Decimal) for value in frame[name]):
                raise TrainingContractError(f"{name} must remain Decimal until preprocessing")
            frame[name] = frame[name].map(float)
        frame[name] = pd.to_numeric(frame[name], errors="raise").astype("float64")
        if any(not math.isfinite(value) for value in frame[name].dropna()):
            raise TrainingContractError(f"Nonfinite model input {name}")
    return frame


def build_pipeline() -> Pipeline:
    numeric = Pipeline(
        [
            (
                "imputer",
                SimpleImputer(strategy="constant", fill_value=0.0, keep_empty_features=True),
            ),
            ("scaler", StandardScaler()),
        ]
    )
    preprocessing = ColumnTransformer(
        [
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore"),
                list(CATEGORICAL_FEATURE_COLUMNS),
            ),
            ("numeric", numeric, list(NUMERIC_FEATURE_COLUMNS)),
        ],
        remainder="drop",
        sparse_threshold=1.0,
    )
    # lbfgs has deterministic fitting for fixed inputs and ignores random_state.
    estimator = LogisticRegression(
        solver="lbfgs", class_weight="balanced", C=1.0, tol=1e-4, max_iter=1000
    )
    return Pipeline([("preprocessing", preprocessing), ("classifier", estimator)])


def estimator_parameters() -> dict[str, object]:
    estimator = cast(LogisticRegression, build_pipeline().named_steps["classifier"])
    return cast(dict[str, object], estimator.get_params(deep=False))


def fit_baseline(
    train_rows: tuple[dict[str, object], ...], candidates: tuple[str, ...]
) -> Pipeline:
    model = build_pipeline()
    labels = [cast(int, row["fraud_label"]) for row in train_rows]
    model.fit(feature_frame(train_rows, candidates), labels)
    return model


def probability_estimates(
    model: Pipeline, rows: tuple[dict[str, object], ...], candidates: tuple[str, ...]
) -> list[float]:
    classifier = cast(LogisticRegression, model.named_steps["classifier"])
    positive_index = list(classifier.classes_).index(1)
    values = model.predict_proba(feature_frame(rows, candidates))[:, positive_index]
    probabilities = [float(value) for value in values]
    if any(not math.isfinite(value) or not 0 <= value <= 1 for value in probabilities):
        raise TrainingContractError("Nonfinite/out-of-range baseline probability estimate")
    return probabilities


def evaluate(rows: tuple[dict[str, object], ...], probabilities: list[float]) -> dict[str, object]:
    labels = [cast(int, row["fraud_label"]) for row in rows]
    if len(labels) != len(probabilities) or set(labels) != {0, 1}:
        raise TrainingContractError("Binary evaluation requires aligned rows and both classes")
    predicted = [int(value >= CLASSIFICATION_THRESHOLD) for value in probabilities]
    tn, fp, fn, tp = (
        int(value) for value in confusion_matrix(labels, predicted, labels=[0, 1]).ravel()
    )
    return {
        "rows": len(labels),
        "positives": sum(labels),
        "negatives": len(labels) - sum(labels),
        "positive_prevalence": sum(labels) / len(labels),
        "average_precision": float(average_precision_score(labels, probabilities)),
        "roc_auc": float(roc_auc_score(labels, probabilities)),
        "precision_at_0_5": tp / (tp + fp) if tp + fp else None,
        "recall_at_0_5": tp / (tp + fn),
        "f1_at_0_5": 2 * tp / (2 * tp + fp + fn),
        "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
    }
