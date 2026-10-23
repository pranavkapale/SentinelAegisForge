"""Exclusive durable logical terminal outcomes for cooperating local research workers."""

import math
import os
import re
import tempfile
from pathlib import Path

from sentinelaegisforge_control_plane.availability.contract import read_object
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

from .contract import INFERENCE, ShadowKafkaError, request_id

STATUS = {"SCORED", "OUT_OF_SCOPE", "INPUT_REJECTED", "INFERENCE_FAILED"}
FIELDS = {
    "inference_contract_version",
    "inference_request_id",
    "shadow_inbox_id",
    "ready_event_id",
    "event_id",
    "candidate_id",
    "model_tree_sha256",
    "registered_model_version",
    "first_visible_delta_version",
    "source_feature_row_fingerprint",
    "score_start",
    "score_end",
    "execution_status",
    "fraud_probability_estimate",
    "failure_code",
}
HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def result_path(root: Path, candidate_id: str, request_id: str) -> Path:
    if not HEX64.fullmatch(candidate_id) or not HEX64.fullmatch(request_id):
        raise ShadowKafkaError("Invalid candidate/request storage identity")
    return root.resolve() / candidate_id / f"{request_id}.json"


def inspect_result(path: Path, expected: dict[str, object] | None = None) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise ShadowKafkaError("Durable result is missing or linked")
    value = read_object(path)
    if set(value) != FIELDS or value["execution_status"] not in STATUS:
        raise ShadowKafkaError("Durable result contract differs")
    if (
        value["inference_contract_version"] != INFERENCE
        or value["inference_request_id"]
        != request_id(
            str(value["ready_event_id"]),
            str(value["candidate_id"]),
            str(value["model_tree_sha256"]),
        )
        or not all(
            isinstance(value[name], str) and value[name]
            for name in (
                "shadow_inbox_id",
                "event_id",
                "registered_model_version",
                "source_feature_row_fingerprint",
                "score_start",
                "score_end",
            )
        )
        or type(value["first_visible_delta_version"]) is not int
        or value["first_visible_delta_version"] < 0
    ):
        raise ShadowKafkaError("Durable result source/request identity differs")
    status = value["execution_status"]
    probability = value["fraud_probability_estimate"]
    failure = value["failure_code"]
    if status == "SCORED":
        if (
            type(probability) is not float
            or not math.isfinite(probability)
            or not 0 <= probability <= 1
            or failure is not None
        ):
            raise ShadowKafkaError("Invalid successful probability result")
    elif (
        probability is not None
        or failure
        != {
            "OUT_OF_SCOPE": None,
            "INPUT_REJECTED": "INVALID_MODEL_INPUT",
            "INFERENCE_FAILED": "MODEL_INFERENCE_ERROR",
        }[str(status)]
    ):
        raise ShadowKafkaError("Invalid terminal failure/probability combination")
    if path.stem != value["inference_request_id"]:
        raise ShadowKafkaError("Durable result path/identity differs")
    if expected is not None and value != expected:
        raise ShadowKafkaError("Conflicting durable inference result")
    return value


def claim_result(path: Path, result: dict[str, object]) -> bool:
    """Publish once by hard-linking an fsynced temporary file; never replace a result."""
    if path.exists():
        inspect_result(path, result)
        return False
    parent = path.parent
    if parent.is_symlink():
        raise ShadowKafkaError("Result namespace must not be a symlink")
    parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_json(result)
    descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
            directory = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            inspect_result(path, result)
            return True
        except FileExistsError:
            inspect_result(path, result)
            return False
    finally:
        os.unlink(temporary)


def durable_results(root: Path, candidate_id: str, requests: list[str]) -> list[dict[str, object]]:
    directory = result_path(root, candidate_id, "0" * 64).parent
    if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
        raise ShadowKafkaError("Invalid durable result namespace")
    wanted = set(requests)
    if len(wanted) != len(requests):
        raise ShadowKafkaError("Repeated inference request identity")
    actual = {item.stem for item in directory.glob("*.json")} if directory.exists() else set()
    if actual - wanted:
        raise ShadowKafkaError("Unexpected durable inference result for candidate namespace")
    return [
        inspect_result(result_path(root, candidate_id, request))
        for request in requests
        if request in actual
    ]


def counts(results: list[dict[str, object]], inbox_count: int) -> dict[str, int]:
    values = {
        status: sum(item["execution_status"] == status for item in results) for status in STATUS
    }
    values["unresolved"] = inbox_count - len(results)
    values["inbox_ready_events"] = inbox_count
    values["eligible_events"] = (
        values["SCORED"] + values["INPUT_REJECTED"] + values["INFERENCE_FAILED"]
    )
    if (
        values["unresolved"] < 0
        or sum(values[status] for status in STATUS) + values["unresolved"] != inbox_count
    ):
        raise ShadowKafkaError("Durable result reconciliation failed")
    return values
