"""Docker-independent Phase 28 plan, lineage and fail-closed boundary tests."""

import hashlib
from pathlib import Path
from typing import cast

import pytest

from sentinelaegisforge_control_plane.availability.contract import read_object
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import (
    STAGES,
    IntegrationError,
    create_identity,
    inspect_identity,
    read_plan,
)
from sentinelaegisforge_control_plane.stateful_shadow_integration.evidence import (
    FILES,
)
from sentinelaegisforge_control_plane.stateful_shadow_integration.evidence import (
    inspect as inspect_evidence,
)
from sentinelaegisforge_control_plane.stateful_shadow_integration.reconciliation import (
    _publication,
    verify_wave,
)
from sentinelaegisforge_control_plane.synthetic.corpus import PLAN_COLUMNS


def _fixtures(root: Path) -> tuple[Path, Path]:
    corpus = root / "corpus-123"
    corpus.mkdir()
    row = {
        "event_id": "31cba284-4451-46c9-953f-edbf749ab317",
        "transaction_id": "tx-1",
        "customer_id": "customer-1",
        "merchant_id": "merchant-1",
        "event_time": "2030-01-01T00:00:00Z",
        "ingestion_time": "2030-01-01T00:00:01Z",
        "amount": "10.0000",
        "currency": "USD",
        "country": "US",
        "device_id": "device-1",
        "ip_address": "192.0.2.1",
        "transaction_type": "CARD_PAYMENT",
        "schema_version": "1",
        "wave": "0",
    }
    rows = []
    for wave in range(3):
        fields = dict(row)
        fields["wave"] = str(wave)
        fields["event_id"] = f"31cba284-4451-46c9-953f-edbf749ab31{wave}"
        rows.append("\t".join(fields[name] for name in PLAN_COLUMNS))
    plan = "\t".join(PLAN_COLUMNS) + "\n" + "\n".join(rows) + "\n"
    (corpus / "plan.tsv").write_text(plan, encoding="utf-8")
    manifest = {
        "scenario_contract_version": "synthetic-fraud-scenario-v1",
        "corpus_id": corpus.name,
        "seed": "2801",
        "generated_transaction_count": "3",
        "plan_sha256": hashlib.sha256(plan.encode()).hexdigest(),
    }
    (corpus / "manifest.tsv").write_text(
        "".join(f"{key}\t{value}\n" for key, value in manifest.items()), encoding="utf-8"
    )
    fence = root / "transaction-fence.json"
    fence.write_bytes(
        canonical_json(
            {
                "contract_version": "transaction-topic-starting-fence-v1",
                "topic": "transactions.raw",
                "topic_id": "topic-id",
                "cluster_id": "cluster-id",
                "partition_count": 3,
                "beginning_0": 0,
                "beginning_1": 0,
                "beginning_2": 0,
                "end_0": 100,
                "end_1": 200,
                "end_2": 300,
            }
        )
    )
    return corpus, fence


def test_identity_is_stable_and_all_stage_lineages_are_separate(tmp_path: Path) -> None:
    corpus, fence = _fixtures(tmp_path)
    first = create_identity(corpus, tmp_path / "integrations", fence)
    second = create_identity(corpus, tmp_path / "integrations", fence)
    assert first == second
    assert len({cast(dict[str, str], first["paths"])[stage] for stage in STAGES}) == 4
    assert len({cast(dict[str, str], first["checkpoints"])[stage] for stage in STAGES}) == 4
    assert len({cast(dict[str, str], first["transaction_app_ids"])[stage] for stage in STAGES}) == 4
    assert cast(str, first["group_id"]).startswith("sentinel-feature-ready-incremental-shadow-v1-")
    assert "truth" not in str(first)
    assert "label" not in str(first)


def test_plan_change_and_duplicate_event_fail_closed(tmp_path: Path) -> None:
    corpus, _ = _fixtures(tmp_path)
    plan = read_plan(corpus)
    assert [len(plan.wave_ids(wave)) for wave in range(3)] == [1, 1, 1]
    text = (corpus / "plan.tsv").read_text(encoding="utf-8")
    (corpus / "plan.tsv").write_text(text.replace("10.0000", "20.0000"), encoding="utf-8")
    with pytest.raises(IntegrationError, match="identity or hash"):
        read_plan(corpus)


def test_duplicate_event_rejected_even_with_matching_plan_hash(tmp_path: Path) -> None:
    corpus, _ = _fixtures(tmp_path)
    lines = (corpus / "plan.tsv").read_text(encoding="utf-8").splitlines()
    second = lines[2].split("\t")
    second[0] = lines[1].split("\t")[0]
    lines[2] = "\t".join(second)
    raw = ("\n".join(lines) + "\n").encode()
    (corpus / "plan.tsv").write_bytes(raw)
    manifest = (corpus / "manifest.tsv").read_text(encoding="utf-8")
    old_hash = next(
        line.split("\t")[1] for line in manifest.splitlines() if line.startswith("plan_sha256")
    )
    (corpus / "manifest.tsv").write_text(
        manifest.replace(old_hash, hashlib.sha256(raw).hexdigest()), encoding="utf-8"
    )
    with pytest.raises(IntegrationError, match="membership"):
        read_plan(corpus)


def test_fence_and_location_cannot_be_swapped(tmp_path: Path) -> None:
    corpus, fence = _fixtures(tmp_path)
    identity = create_identity(corpus, tmp_path / "integrations", fence)
    path = Path(cast(str, identity["integration_path"]))
    (path / "identity").mkdir(parents=True)
    (path / "identity" / "integration_manifest.json").write_bytes(canonical_json(identity))
    assert inspect_identity(path) == identity
    fence.write_bytes(canonical_json({"topic": "transactions.raw"}))
    with pytest.raises(IntegrationError, match="fence"):
        inspect_identity(path)


def test_missing_published_or_committed_stage_is_not_success(tmp_path: Path) -> None:
    corpus, fence = _fixtures(tmp_path)
    identity = create_identity(corpus, tmp_path / "integrations", fence)
    path = Path(cast(str, identity["integration_path"]))
    (path / "identity").mkdir(parents=True)
    (path / "identity" / "integration_manifest.json").write_bytes(canonical_json(identity))
    with pytest.raises(IntegrationError, match="publication evidence"):
        verify_wave(path, 0)


def test_publication_requires_exact_unique_acknowledgements(tmp_path: Path) -> None:
    corpus, _ = _fixtures(tmp_path)
    (corpus / "publish-attempt-wave-0.tsv").write_text("attempt\n", encoding="utf-8")
    report = corpus / "publish-report-wave-0.tsv"
    report.write_text(
        "requested\t1\nacknowledged\t1\nfailed\t0\nack\ttransactions.raw\t0\t101\n",
        encoding="utf-8",
    )
    assert _publication(corpus, 0, 1) == {(0, 101)}
    report.write_text(
        "requested\t1\nacknowledged\t1\nfailed\t0\nack\ttransactions.raw\t0\t101\n"
        "ack\ttransactions.raw\t0\t101\n",
        encoding="utf-8",
    )
    with pytest.raises(IntegrationError, match="detail count"):
        _publication(corpus, 0, 1)


def test_incomplete_evidence_never_inspects_as_success(tmp_path: Path) -> None:
    path = tmp_path / "evidence"
    path.mkdir()
    (path / "integration_manifest.json").write_bytes(
        canonical_json({"contract_version": "stateful-streaming-shadow-evidence-v1"})
    )
    assert len(FILES) > 1
    with pytest.raises(ValueError, match="Incomplete"):
        inspect_evidence(path)


def test_private_scenario_truth_cannot_change_integration_identity(tmp_path: Path) -> None:
    corpus, fence = _fixtures(tmp_path)
    original = create_identity(corpus, tmp_path / "integrations", fence)
    (corpus / "truth.tsv").write_text("private synthetic outcomes\n", encoding="utf-8")
    (corpus / "labels.jsonl").write_text('{"fraud":true}\n', encoding="utf-8")
    assert create_identity(corpus, tmp_path / "integrations", fence) == original


def test_new_transaction_offset_fence_creates_a_distinct_lineage(tmp_path: Path) -> None:
    corpus, fence = _fixtures(tmp_path)
    original = create_identity(corpus, tmp_path / "integrations", fence)
    changed = read_object(fence)
    changed["end_0"] = 101
    fence.write_bytes(canonical_json(changed))
    next_identity = create_identity(corpus, tmp_path / "integrations", fence)
    assert next_identity["integration_id"] != original["integration_id"]
    assert next_identity["checkpoints"] != original["checkpoints"]
    assert next_identity["transaction_app_ids"] != original["transaction_app_ids"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("contract_version", "wrong"),
        ("topic", "another.topic"),
        ("partition_count", 2),
        ("end_0", "100"),
        ("topic_id", None),
        ("cluster_id", None),
    ],
)
def test_invalid_transaction_fences_fail_before_initialization(
    tmp_path: Path, field: str, value: object
) -> None:
    corpus, fence = _fixtures(tmp_path)
    changed = read_object(fence)
    changed[field] = value
    fence.write_bytes(canonical_json(changed))
    with pytest.raises(IntegrationError, match="fence"):
        create_identity(corpus, tmp_path / "integrations", fence)


def test_missing_wave_fails_even_when_plan_hash_matches(tmp_path: Path) -> None:
    corpus, _ = _fixtures(tmp_path)
    lines = (corpus / "plan.tsv").read_text(encoding="utf-8").splitlines()
    second = lines[2].split("\t")
    second[-1] = "0"
    lines[2] = "\t".join(second)
    raw = ("\n".join(lines) + "\n").encode()
    (corpus / "plan.tsv").write_bytes(raw)
    manifest = (corpus / "manifest.tsv").read_text(encoding="utf-8")
    old_hash = next(
        line.split("\t")[1] for line in manifest.splitlines() if line.startswith("plan_sha256")
    )
    (corpus / "manifest.tsv").write_text(
        manifest.replace(old_hash, hashlib.sha256(raw).hexdigest()), encoding="utf-8"
    )
    with pytest.raises(IntegrationError, match="three scenario waves"):
        read_plan(corpus)


@pytest.mark.parametrize(
    "bad_ack",
    [
        "ack\twrong.topic\t0\t101",
        "ack\ttransactions.raw\t3\t101",
        "ack\ttransactions.raw\t0\t-1",
        "ack\ttransactions.raw\t0\t101\textra",
    ],
)
def test_invalid_kafka_acknowledgement_fails_closed(tmp_path: Path, bad_ack: str) -> None:
    corpus, _ = _fixtures(tmp_path)
    (corpus / "publish-attempt-wave-0.tsv").write_text("attempt\n", encoding="utf-8")
    (corpus / "publish-report-wave-0.tsv").write_text(
        f"requested\t1\nacknowledged\t1\nfailed\t0\n{bad_ack}\n", encoding="utf-8"
    )
    with pytest.raises(IntegrationError, match="Kafka"):
        _publication(corpus, 0, 1)


def test_corrupt_immutable_evidence_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "evidence"
    path.mkdir()
    hashes: dict[str, str] = {}
    for name in FILES - {"integration_manifest.json"}:
        raw = b"evidence\n"
        (path / name).write_bytes(raw)
        hashes[name] = hashlib.sha256(raw).hexdigest()
    (path / "integration_manifest.json").write_bytes(
        canonical_json(
            {
                "contract_version": "stateful-streaming-shadow-evidence-v1",
                "synthetic_research_only": True,
                "artifact_sha256": hashes,
            }
        )
    )
    inspect_evidence(path)
    (path / "spark_progress_evidence.json").write_bytes(b"changed\n")
    with pytest.raises(ValueError, match="Corrupt integration artifact"):
        inspect_evidence(path)
