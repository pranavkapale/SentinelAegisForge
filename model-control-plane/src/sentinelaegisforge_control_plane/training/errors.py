"""Expected failures at the offline training boundary."""


class TrainingContractError(ValueError):
    """Invalid snapshot, training configuration, or run artifact."""


class InsufficientTrainingDataError(TrainingContractError):
    """Chronological cohorts cannot support the required binary evaluation."""
