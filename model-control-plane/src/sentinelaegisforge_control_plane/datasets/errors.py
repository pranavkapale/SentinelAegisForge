"""Focused failures: invalid data is never silently repaired or dropped."""


class DatasetContractError(ValueError):
    """Base error for this offline dataset contract."""


class FeatureSourceError(DatasetContractError):
    """Delta source could not be opened or materialized through its transaction log."""


class SourceSchemaError(DatasetContractError):
    """Source does not implement the corrected statistical feature schema."""


class FeatureDataError(DatasetContractError):
    """Source contains ambiguous identities or corrupt feature values."""


class LabelContractError(DatasetContractError):
    """Outcome labels violate provenance, revision or causal constraints."""


class SnapshotConflictError(DatasetContractError):
    """An existing immutable snapshot does not match the expected artifacts."""
