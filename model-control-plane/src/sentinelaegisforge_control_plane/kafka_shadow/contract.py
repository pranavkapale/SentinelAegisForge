"""Versioned identity and error boundary for bounded local shadow inference."""

from sentinelaegisforge_control_plane.availability.contract import digest

INFERENCE = "kafka-shadow-inference-v1"
RUN = "kafka-shadow-run-v1"


class ShadowKafkaError(ValueError):
    """Inbox, source, model, result or research parity failed closed."""


class RequestInferenceError(Exception):
    """An isolated request-local model invocation failure."""


def request_id(ready_event_id: str, candidate_id: str, model_tree_sha256: str) -> str:
    return digest([ready_event_id, candidate_id, model_tree_sha256, INFERENCE])
