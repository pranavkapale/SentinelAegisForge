"""Plan-only identity and isolated paths for a real Scala feature experiment.

Private scenario truth and labels are deliberately never opened here.
"""

import csv
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.availability.contract import digest, read_object
from sentinelaegisforge_control_plane.synthetic.corpus import PLAN_COLUMNS, VERSION

CONTRACT = "stateful-streaming-shadow-integration-v1"
CANDIDATE_ID = "b4fff4f1f74818165d04bae094b89f7c7b8dc7367d6e3dad10465b1a6160fadb"
STAGES = ("validated", "deduplicated", "rolling", "statistical")


class IntegrationError(ValueError):
    """The isolated integration identity or durable evidence is inconsistent."""


@dataclass(frozen=True)
class Plan:
    corpus_id: str
    seed: int
    rows: tuple[dict[str, str], ...]
    plan_sha256: str

    def wave_ids(self, wave: int) -> frozenset[str]:
        return frozenset(row["event_id"] for row in self.rows if row["wave"] == str(wave))


def read_plan(corpus: Path) -> Plan:
    """Verify the transaction plan without loading private synthetic outcomes."""
    manifest_path, plan_path = corpus / "manifest.tsv", corpus / "plan.tsv"
    if any(path.is_symlink() or not path.is_file() for path in (manifest_path, plan_path)):
        raise IntegrationError("Scenario plan/manifest must be regular files")
    manifest: dict[str, str] = {}
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        pair = line.split("\t", 1)
        if len(pair) != 2 or pair[0] in manifest:
            raise IntegrationError("Malformed scenario manifest")
        manifest[pair[0]] = pair[1]
    raw = plan_path.read_bytes()
    if (
        manifest.get("scenario_contract_version") != VERSION
        or manifest.get("corpus_id") != corpus.name
        or hashlib.sha256(raw).hexdigest() != manifest.get("plan_sha256")
    ):
        raise IntegrationError("Scenario plan identity or hash differs")
    with plan_path.open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source, delimiter="\t")
        if tuple(reader.fieldnames or ()) != PLAN_COLUMNS:
            raise IntegrationError("Scenario plan columns differ")
        rows = tuple(reader)
    if (
        len(rows) != int(manifest["generated_transaction_count"])
        or len({row["event_id"] for row in rows}) != len(rows)
        or any(row["wave"] not in {"0", "1", "2"} for row in rows)
        or any(None in row or None in row.values() for row in rows)
    ):
        raise IntegrationError("Invalid plan membership, wave or row")
    if any(not Plan(corpus.name, 0, rows, "").wave_ids(wave) for wave in range(3)):
        raise IntegrationError("All three scenario waves must be populated")
    return Plan(corpus.name, int(manifest["seed"]), rows, manifest["plan_sha256"])


def create_identity(corpus: Path, root: Path, fence_path: Path) -> dict[str, object]:
    plan = read_plan(corpus)
    fence = read_object(fence_path)
    if (
        fence.get("contract_version") != "transaction-topic-starting-fence-v1"
        or fence.get("topic") != "transactions.raw"
        or fence.get("partition_count") != 3
        or not all(type(fence.get(f"end_{part}")) is int for part in range(3))
        or not isinstance(fence.get("topic_id"), str)
        or not isinstance(fence.get("cluster_id"), str)
    ):
        raise IntegrationError("Invalid live transaction-topic starting fence")
    basis = {
        "contract_version": CONTRACT,
        "corpus_id": plan.corpus_id,
        "plan_sha256": plan.plan_sha256,
        "transaction_topic_id": fence["topic_id"],
        "transaction_cluster_id": fence["cluster_id"],
        "starting_offsets": [fence[f"end_{part}"] for part in range(3)],
        "candidate_id": CANDIDATE_ID,
        "logic_version": "phase28-three-wave-v1",
    }
    integration_id = digest(basis)
    location = root.resolve() / integration_id
    paths = {stage: str(location / "delta" / stage) for stage in STAGES}
    checkpoints = {stage: str(location / "checkpoints" / stage) for stage in STAGES}
    apps = {stage: f"phase28-{stage}-{integration_id[:20]}" for stage in STAGES}
    if (
        len(set(paths.values())) != 4
        or len(set(checkpoints.values())) != 4
        or len(set(apps.values())) != 4
    ):
        raise IntegrationError("Integration stage lineages overlap")
    return {
        **basis,
        "integration_id": integration_id,
        "corpus_path": str(corpus.resolve()),
        "transaction_fence_path": str(fence_path.resolve()),
        "group_id": f"sentinel-feature-ready-incremental-shadow-v1-{integration_id[:20]}",
        "paths": paths,
        "checkpoints": checkpoints,
        "transaction_app_ids": apps,
        "integration_path": str(location),
    }


def inspect_identity(path: Path) -> dict[str, object]:
    identity = read_object(path / "identity" / "integration_manifest.json")
    expected = create_identity(
        Path(cast(str, identity["corpus_path"])),
        path.parent,
        Path(cast(str, identity["transaction_fence_path"])),
    )
    if identity != expected or path.resolve() != Path(cast(str, identity["integration_path"])):
        raise IntegrationError("Integration identity, plan or transaction fence changed")
    return identity
