"""Docker-independent Phase 29 identity, state and evidence failure gates."""

import subprocess
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from sentinelaegisforge_control_plane.availability.contract import read_object
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.reliability import (
    cli,
    contract,
    evidence,
    observation,
    operations,
    outage,
    retention,
    state_examples,
)
from sentinelaegisforge_control_plane.reliability.contract import ReliabilityError
from sentinelaegisforge_control_plane.stateful_shadow_integration import cli as integration_cli
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import Plan


def _identity(name: str) -> dict[str, object]:
    return {
        "integration_id": name,
        "corpus_id": f"corpus-{name}",
        "plan_sha256": f"plan-{name}",
        "group_id": f"group-{name}",
        "paths": {
            stage: f"/tmp/{name}/{stage}"
            for stage in ("validated", "deduplicated", "rolling", "statistical")
        },
        "checkpoints": {
            stage: f"/tmp/{name}/checkpoint/{stage}"
            for stage in ("validated", "deduplicated", "rolling", "statistical")
        },
        "transaction_app_ids": {
            stage: f"app-{name}-{stage}"
            for stage in ("validated", "deduplicated", "rolling", "statistical")
        },
    }


def test_matrix_requires_four_distinct_lineages(monkeypatch: pytest.MonkeyPatch) -> None:
    identities = {label: _identity(label) for label in "ABCD"}
    monkeypatch.setattr(contract, "inspect_identity", lambda path: identities[path.name])
    paths = {label: Path(label) for label in "ABCD"}
    assert contract.matrix(paths)["experiments"] == {label: label for label in "ABCD"}
    with pytest.raises(ReliabilityError, match="A, B, C and D"):
        contract.matrix({"A": Path("A")})
    identities["B"]["integration_id"] = "A"
    with pytest.raises(ReliabilityError, match="IDs overlap"):
        contract.matrix(paths)


def test_matrix_rejects_cross_experiment_path_collision(monkeypatch: pytest.MonkeyPatch) -> None:
    identities = {label: _identity(label) for label in "ABCD"}
    monkeypatch.setattr(contract, "inspect_identity", lambda path: identities[path.name])
    cast_paths = identities["B"]["checkpoints"]
    assert isinstance(cast_paths, dict)
    cast_paths["rolling"] = "/tmp/A/checkpoint/rolling"
    with pytest.raises(ReliabilityError, match="lineages overlap"):
        contract.matrix({label: Path(label) for label in "ABCD"})


def test_changed_workload_needs_a_distinct_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    identities = {label: _identity(label) for label in "ABCD"}
    identities["B"]["plan_sha256"] = identities["A"]["plan_sha256"]
    monkeypatch.setattr(contract, "inspect_identity", lambda path: identities[path.name])
    with pytest.raises(ReliabilityError, match="same transaction plan"):
        contract.matrix({label: Path(label) for label in "ABCD"})


def test_state_compatibility_rejects_changed_semantics_and_unrecorded_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _identity("A")
    identity["checkpoints"] = {
        stage: str(tmp_path / stage)
        for stage in ("validated", "deduplicated", "rolling", "statistical")
    }
    monkeypatch.setattr(contract, "inspect_identity", lambda _: identity)
    monkeypatch.setattr(contract, "_table_id", lambda _: "source-table-id")
    integration = tmp_path / "integration"
    integration.mkdir()
    first = contract.state_compatibility(integration)
    assert contract.inspect_state_compatibility(integration) == first
    assert contract.state_compatibility(integration) == first
    monkeypatch.setattr(contract, "WATERMARK", "20 minutes")
    with pytest.raises(ReliabilityError, match="fingerprint changed"):
        contract.state_compatibility(integration)
    monkeypatch.setattr(contract, "WATERMARK", "10 minutes")
    monkeypatch.setattr(contract, "STATE_PROVIDER", "different-state-provider")
    with pytest.raises(ReliabilityError, match="fingerprint changed"):
        contract.state_compatibility(integration)
    monkeypatch.setattr(
        contract,
        "STATE_PROVIDER",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    )
    monkeypatch.setattr(contract, "_table_id", lambda _: "different-source-table")
    with pytest.raises(ReliabilityError, match="fingerprint changed"):
        contract.state_compatibility(integration)
    monkeypatch.setattr(contract, "_table_id", lambda _: "source-table-id")
    identity["transaction_app_ids"] = {**identity["transaction_app_ids"], "statistical": "new-app"}  # type: ignore[dict-item]
    with pytest.raises(ReliabilityError, match="record differs"):
        contract.inspect_state_compatibility(integration)
    blank = tmp_path / "blank"
    blank.mkdir()
    (tmp_path / "statistical").mkdir()
    with pytest.raises(ReliabilityError, match="lacks a compatibility record"):
        contract.state_compatibility(blank)


def test_target_table_identity_must_not_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _identity("A")
    monkeypatch.setattr(contract, "inspect_identity", lambda _: identity)
    ids = iter(("table-one", "table-one", "table-two"))
    monkeypatch.setattr(contract, "_table_id", lambda _: next(ids))
    assert contract.target_identity(tmp_path)["table_id"] == "table-one"
    contract.target_identity(tmp_path)
    with pytest.raises(ReliabilityError, match="identity changed"):
        contract.target_identity(tmp_path)


@pytest.mark.parametrize(
    "field", ["STATE_SEMANTICS", "INACTIVITY", "SPARK_VERSION", "SCALA_VERSION"]
)
def test_restart_rejects_changed_runtime_or_state_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    identity = _identity("runtime-test")
    identity["checkpoints"] = {
        stage: str(tmp_path / stage)
        for stage in ("validated", "deduplicated", "rolling", "statistical")
    }
    monkeypatch.setattr(contract, "inspect_identity", lambda _: identity)
    monkeypatch.setattr(contract, "_table_id", lambda _: "rolling-id")
    contract.state_compatibility(tmp_path)
    monkeypatch.setattr(contract, field, "incompatible-value")
    with pytest.raises(ReliabilityError, match="record differs"):
        contract.inspect_state_compatibility(tmp_path)


@pytest.mark.parametrize("status", ["NOT_EXECUTED", "FAILED", None])
def test_incomplete_fault_evidence_cannot_claim_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str | None
) -> None:
    monkeypatch.setattr(operations, "inspect_state_compatibility", lambda _: {})
    monkeypatch.setattr(operations, "inspect_identity", lambda _: _identity("C"))
    directory = tmp_path / "reliability"
    directory.mkdir()
    (directory / "statistical-postcommit-recovery.json").write_bytes(
        canonical_json(
            {
                "integration_id": "C",
                "status": status,
            }
        )
    )
    with pytest.raises(ReliabilityError, match="incomplete"):
        operations.inspect_recovery(tmp_path)


def test_recovery_inspection_rechecks_failure_log_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _identity("C")
    monkeypatch.setattr(operations, "inspect_state_compatibility", lambda _: {})
    monkeypatch.setattr(operations, "inspect_identity", lambda _: identity)
    (tmp_path / "reliability").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "process-statistical-features-wave-0-injected.log").write_text(
        "IntentionalDeltaPostCommitFailure: changed log"
    )
    (tmp_path / "reliability" / "statistical-postcommit-recovery.json").write_bytes(
        canonical_json(
            {
                "integration_id": "C",
                "status": "VERIFIED",
                "fault_log_sha256": "invalid",
                "checkpoint": cast(dict[str, str], identity["checkpoints"])["statistical"],
                "txn_app_id": cast(dict[str, str], identity["transaction_app_ids"])["statistical"],
            }
        )
    )
    with pytest.raises(ReliabilityError, match="failure log changed"):
        operations.inspect_recovery(tmp_path)


def test_spark_progress_requires_real_structured_batches(tmp_path: Path) -> None:
    log = tmp_path / "spark.log"
    log.write_text("Statistical feature progress: batchId=0\n", encoding="utf-8")
    with pytest.raises(ReliabilityError, match="Missing structured"):
        observation._progress(log, "statistical")
    log.write_text(
        'Statistical feature progress JSON: {"batchId":0,"numInputRows":3,"stateOperators":[]}\n',
        encoding="utf-8",
    )
    assert observation._progress(log, "statistical")["batches"] == [
        {"batchId": 0, "numInputRows": 3, "stateOperators": []}
    ]
    log.write_text("Statistical feature progress JSON: not-json\n", encoding="utf-8")
    with pytest.raises(ReliabilityError, match="Malformed Spark progress"):
        observation._progress(log, "statistical")


def test_state_inventory_requires_real_regular_files(tmp_path: Path) -> None:
    root = tmp_path / "checkpoint"
    root.mkdir()
    (root / "state.bin").write_bytes(b"abc")
    assert observation._inventory(root) == {"files": 1, "bytes": 3}
    with pytest.raises(ReliabilityError, match="Missing or linked"):
        observation._inventory(tmp_path / "absent")


def test_comparison_rejects_identity_and_missing_timing() -> None:
    first: dict[str, object] = {
        "contract_version": observation.CONTRACT,
        "integration_id": "A",
        "invocation_timings": [{"durations": {"statistical": 2.0}}],
        "source_event_count": 10,
        "customer_currency_state_keys": 3,
        "runtime_semantics": {"spark": "4.2.0"},
        "resource_context": {"cpu": "test"},
        "observation_method": "test clock",
    }
    second = {**first, "integration_id": "B", "source_event_count": 20}
    report = observation.compare(first, second)
    assert report["sample_summary"]["A"]["stage_durations"]["statistical"]["sample_count"] == 1  # type: ignore[index]
    with pytest.raises(ReliabilityError, match="independent"):
        observation.compare(first, first)
    with pytest.raises(ReliabilityError, match="No invocation"):
        observation.compare(first, {**second, "invocation_timings": []})
    with pytest.raises(ReliabilityError, match="Invalid invocation duration"):
        observation.compare(
            first,
            {**second, "invocation_timings": [{"durations": {"statistical": -1.0}}]},
        )
    with pytest.raises(ReliabilityError, match="matching runtime_semantics"):
        observation.compare(first, {**second, "runtime_semantics": {"spark": "different"}})


def test_dossier_inspection_detects_corruption_and_identity(tmp_path: Path) -> None:
    basis = {
        "contract_version": contract.CONTRACT,
        "experiment_integrations": {label: label for label in "ABCD"},
    }
    reliability_id = evidence._hash(canonical_json(basis))
    target = tmp_path / reliability_id
    target.mkdir()
    for name in evidence.FILES - {"reliability_manifest.json"}:
        (target / name).write_bytes(b"evidence\n")
    manifest = {
        **basis,
        "reliability_id": reliability_id,
        "artifact_sha256": {
            name: evidence._hash(b"evidence\n")
            for name in evidence.FILES - {"reliability_manifest.json"}
        },
        "synthetic_research_only": True,
    }
    (target / "reliability_manifest.json").write_bytes(canonical_json(manifest))
    assert evidence.inspect(target) == read_object(target / "reliability_manifest.json")
    (target / "model_parity.json").write_bytes(b"corrupt")
    with pytest.raises(ReliabilityError, match="Corrupt"):
        evidence.inspect(target)


def test_dossier_rejects_replacing_the_preserved_research_candidate(tmp_path: Path) -> None:
    with pytest.raises(ReliabilityError, match="preserved research candidate"):
        evidence.build(
            {}, {}, tmp_path, tmp_path / "different-candidate", tmp_path, tmp_path, tmp_path
        )


def test_observation_cli_exports_canonical_comparison_input_without_overwriting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report: dict[str, object] = {"contract_version": observation.CONTRACT, "value": 1}
    monkeypatch.setattr(cli, "inspect_observation", lambda *_: report)
    output = tmp_path / "observation.json"
    arguments = [
        "observe",
        "--integration",
        str(tmp_path),
        "--stream",
        str(tmp_path),
        "--output",
        str(output),
    ]
    assert cli.main(arguments) == 0
    assert read_object(output) == report
    assert cli.main(arguments) == 0
    report["value"] = 2
    assert cli.main(arguments) == 1
    assert read_object(output)["value"] == 1


def test_kafka_retention_gap_and_topic_identity_fail_closed() -> None:
    required: dict[str, object] = {
        "topic": "transactions.raw",
        "topic_id": "topic-1",
        "cluster_id": "cluster-1",
        "partition_count": 3,
        "end_0": 10,
        "end_1": 20,
        "end_2": 30,
    }
    current = {
        **required,
        "beginning_0": 0,
        "beginning_1": 20,
        "beginning_2": 29,
        "end_0": 11,
        "end_1": 21,
        "end_2": 31,
    }
    assert retention.kafka_range(required, current)["ranges"]["1"]["required"] == 20  # type: ignore[index]
    with pytest.raises(ReliabilityError, match="retention gap in partition 0"):
        retention.kafka_range(required, {**current, "beginning_0": 11})
    with pytest.raises(ReliabilityError, match="topic identity changed"):
        retention.kafka_range(required, {**current, "topic_id": "topic-2"})


def test_feature_ready_retention_probe_is_structured_and_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fence = {
        "topic": "features.ready",
        "cluster_id": "cluster",
        "topic_id": "ready-topic",
        "end_0": 10,
        "end_1": 20,
        "end_2": 30,
    }
    path = tmp_path / "initial.json"
    path.write_bytes(canonical_json(fence))
    monkeypatch.setattr(
        retention,
        "inspect_stream",
        lambda _: {"fence_path": str(path), "group_id": "research-group"},
    )
    current = {**fence, "partition_count": 3, "beginning_0": 0, "beginning_1": 0, "beginning_2": 0}
    log = tmp_path / "offsets.log"
    log.write_text("Feature-ready retention JSON: " + canonical_json(current).decode())
    assert retention.feature_ready_range(tmp_path, log)["group_id"] == "research-group"
    log.write_text(
        "Feature-ready retention JSON: " + canonical_json({**current, "beginning_0": 11}).decode()
    )
    with pytest.raises(ReliabilityError, match="retention gap"):
        retention.feature_ready_range(tmp_path, log)
    log.write_text("human-readable offset map only")
    with pytest.raises(ReliabilityError, match="unambiguous"):
        retention.feature_ready_range(tmp_path, log)


def test_missing_delta_history_version_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(ReliabilityError, match="version 7 is unavailable"):
        retention.delta_version(tmp_path / "unmaterialized", 7)


def test_missing_historical_delta_log_does_not_fall_forward(tmp_path: Path) -> None:
    source = tmp_path / "delta"
    write_deltalake(source, pa.table({"event_id": ["a"]}), mode="append")
    write_deltalake(source, pa.table({"event_id": ["b"]}), mode="append")
    assert retention.delta_version(source, 0)["version"] == 0
    log = source / "_delta_log" / "00000000000000000000.json"
    held = tmp_path / "held-log.json"
    log.rename(held)
    try:
        with pytest.raises(ReliabilityError, match="version 0 is unavailable"):
            retention.delta_version(source, 0)
    finally:
        held.rename(log)
    assert retention.delta_version(source, 0)["version"] == 0


def test_postcommit_row_fingerprint_includes_feature_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rows: list[dict[str, object]] = [
        {"event_id": "event-a", "amount": Decimal("10.0000"), "prior_amount_mean": 5.0}
    ]

    class FakeArrow:
        def to_pylist(self) -> list[dict[str, object]]:
            return rows

    class FakeMetadata:
        id = "delta-id"

    class FakeTable:
        def __init__(self, _: Path, version: int | None = None) -> None:
            pass

        def to_pyarrow_table(self, filesystem: object = None) -> FakeArrow:
            return FakeArrow()

        def metadata(self) -> FakeMetadata:
            return FakeMetadata()

        def version(self) -> int:
            return 0

    monkeypatch.setattr(operations, "DeltaTable", FakeTable)
    before = operations._commit_rows(tmp_path)
    rows[0] = {**rows[0], "prior_amount_mean": 6.0}
    after = operations._commit_rows(tmp_path)
    assert before["event_id_sha256"] == after["event_id_sha256"]
    assert before["row_sha256"] != after["row_sha256"]
    rows.append(dict(rows[0]))
    with pytest.raises(ReliabilityError, match="Duplicate statistical event IDs"):
        operations._commit_rows(tmp_path)


def test_broker_failure_evidence_requires_real_failed_log_and_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = _identity("D")
    integration = tmp_path / "integration"
    stream = tmp_path / "stream"
    integration.mkdir()
    stream.mkdir()
    manifest = {
        "source_delta_path": identity["paths"]["statistical"],  # type: ignore[index]
        "stream_id": "stream-D",
    }
    monkeypatch.setattr(outage, "inspect_identity", lambda _: identity)
    monkeypatch.setattr(outage, "inspect_stream", lambda _: manifest)
    monkeypatch.setattr(
        outage, "backlog_snapshot", lambda _: {"publication_backlog_ready_events": 10}
    )
    monkeypatch.setattr(
        outage,
        "records",
        lambda _: [{"publication_batch_id": "batch-D", "batch_path": str(stream / "batch")}],
    )

    class FakeMetadata:
        id = "delta-D"

    class FakeTable:
        def __init__(self, _: str) -> None:
            pass

        def metadata(self) -> FakeMetadata:
            return FakeMetadata()

        def version(self) -> int:
            return 0

    monkeypatch.setattr(outage, "DeltaTable", FakeTable)
    log = tmp_path / "failed.log"
    log.write_text("KafkaException: broker unavailable\n", encoding="utf-8")
    with pytest.raises(ReliabilityError, match="did not fail"):
        outage.record_failed_publication(integration, stream, "batch-D", log, 0)
    attempt = outage.record_failed_publication(integration, stream, "batch-D", log, 2)
    assert cast(str, attempt["delivery_certainty"]).startswith("UNKNOWN")
    monkeypatch.setattr(
        outage,
        "verify_final",
        lambda *_: {
            "source_event_count": 30,
            "research": {"terminal_inference_results": 30},
        },
    )
    (stream / "acceptance").mkdir()
    (stream / "acceptance" / "batch-D.json").write_bytes(canonical_json({"accepted": True}))
    (integration / "handoffs").mkdir()
    (integration / "handoffs" / "0.json").write_bytes(
        canonical_json(
            {
                "inference_intentionally_paused": True,
                "progress": {"inference_backlog_ready_events": 10},
            }
        )
    )
    (integration / "handoffs" / "1.json").write_bytes(
        canonical_json({"backpressure_pause_observed": True, "progress": {"x": 1}})
    )
    (integration / "reliability" / "backpressure-refusal.json").write_bytes(
        canonical_json(
            {
                "projected_inference_backlog": 20,
                "configured_limit": 15,
            }
        )
    )
    for point in ("recovered-published", "research-paused", "final"):
        (integration / "reliability" / f"backlog-{point}.json").write_bytes(
            canonical_json(
                {
                    "acceptance_backlog_ready_events": 10 if point == "recovered-published" else 0,
                    "publication_backlog_ready_events": 0,
                    "inference_backlog_ready_events": 0,
                    "discovery_backlog_versions": 0,
                }
            )
        )
    broker_outage = cast(dict[str, object], outage.verify(integration, stream)["broker_outage"])
    assert broker_outage["status"] == "VERIFIED"
    log.write_text("changed", encoding="utf-8")
    with pytest.raises(ReliabilityError, match="failure log changed"):
        outage.verify(integration, stream)


def test_state_example_requires_observed_watermark(tmp_path: Path) -> None:
    with pytest.raises(ReliabilityError, match="No observed statistical watermark"):
        state_examples.examples(tmp_path / "missing", None, {"batches": [{}]})  # type: ignore[arg-type]


def test_retained_and_expired_examples_use_watermark_and_independent_statistics(
    tmp_path: Path,
) -> None:
    rows: list[dict[str, object]] = []
    for event, currency, timestamp, amount, count, mean, stddev, status in (
        ("a", "USD", "2030-01-01T12:00:00+00:00", "100", 0, None, None, "NO_HISTORY"),
        (
            "b",
            "USD",
            "2030-01-01T12:01:00+00:00",
            "200",
            1,
            100.0,
            None,
            "INSUFFICIENT_VARIANCE_HISTORY",
        ),
        ("c", "USD", "2030-01-01T13:30:00+00:00", "300", 2, 150.0, 5000**0.5, "READY"),
        ("d", "EUR", "2029-12-30T12:00:00+00:00", "1000", 0, None, None, "NO_HISTORY"),
        ("e", "EUR", "2030-01-01T14:00:00+00:00", "1200", 0, None, None, "NO_HISTORY"),
    ):
        rows.append(
            {
                "event_id": event,
                "customer_id": "customer-a",
                "currency": currency,
                "event_time": datetime.fromisoformat(timestamp),
                "amount": Decimal(amount),
                "kafka_offset": len(rows),
                "prior_amount_observation_count": count,
                "prior_amount_mean": mean,
                "prior_amount_stddev": stddev,
                "statistical_feature_status": status,
            }
        )
    path = tmp_path / "statistical"
    write_deltalake(path, pa.Table.from_pylist(rows))
    plan = Plan(
        "corpus",
        1,
        tuple(
            {
                "event_id": cast(str, row["event_id"]),
                "wave": "1" if row["event_id"] in {"c", "e"} else "0",
            }
            for row in rows
        ),
        "plan",
    )
    progress: dict[str, object] = {
        "batches": [
            {
                "eventTime": {"watermark": "2030-01-01T13:00:00Z"},
                "stateOperators": [{"numRowsRemoved": 1}],
            }
        ]
    }
    report = state_examples.examples(path, plan, progress)
    retained = cast(dict[str, object], report["retained"])
    assert retained["currency"] == "USD"
    assert retained["expected_prior_mean"] == retained["observed_prior_mean"] == 150.0
    assert retained["expected_prior_stddev"] == retained["observed_prior_stddev"]
    expired = cast(dict[str, object], report["expired_and_reactivated"])
    assert expired["currency"] == "EUR"
    assert expired["later_prior_count"] == 0
    cast(list[dict[str, object]], progress["batches"])[0]["stateOperators"] = [
        {"numRowsRemoved": 0}
    ]
    assert state_examples.examples(path, plan, progress)["expired_and_reactivated"] is None


def test_real_handoff_respects_configured_inference_tick_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        integration_cli,
        "inspect_stream",
        lambda _: {"limits": {"maximum_inference_events_per_tick": 10}},
    )
    backlogs = iter((10, 0))
    monkeypatch.setattr(
        integration_cli, "progress", lambda _: {"inference_backlog_ready_events": next(backlogs)}
    )
    calls: list[dict[str, str]] = []

    def make(target: str, identity: dict[str, object], extra: dict[str, str], label: str) -> float:
        calls.append(extra)
        return 1.0

    monkeypatch.setattr(integration_cli, "_make", make)
    timings: list[dict[str, object]] = []
    integration_cli._drain(tmp_path, {"integration_path": str(tmp_path)}, 0, timings)
    assert len(calls) == 1
    assert calls[0]["INCREMENTAL_TICK_LIMIT"] == "10"
    assert timings[0]["stage"] == "research_inference"


def test_backlog_layers_distinguish_publication_acceptance_and_inference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = {"topic_id": "topic", "cluster_id": "cluster"}
    summary = {
        "prepared_ready_events": 2,
        "accepted_ready_events": 1,
        "completed_inference_results": 0,
        "discovery_backlog_versions": 0,
        "inference_backlog_ready_events": 1,
    }
    monkeypatch.setattr(outage, "inspect_stream", lambda _: manifest)
    monkeypatch.setattr(outage, "progress", lambda _: summary)
    monkeypatch.setattr(
        outage, "records", lambda _: [{"publication_batch_id": "batch", "batch_path": "batch-path"}]
    )
    monkeypatch.setattr(outage, "inspect_batch", lambda _: {"outbox_path": "outbox"})
    monkeypatch.setattr(
        outage,
        "inspect_outbox",
        lambda _: (
            {},
            [
                {"ready_event_id": "ready-a", "event_id": "a"},
                {"ready_event_id": "ready-b", "event_id": "b"},
            ],
        ),
    )
    directory = tmp_path / "journals" / "batch" / "acknowledgements"
    directory.mkdir(parents=True)
    for ready, event in (("ready-a", "a"), ("ready-b", "b")):
        (directory / f"{ready}.json").write_bytes(
            canonical_json(
                {
                    "ready_event_id": ready,
                    "event_id": event,
                    "publication_batch_id": "batch",
                    "topic_id": "topic",
                    "cluster_id": "cluster",
                }
            )
        )
        observed = outage.backlog_snapshot(tmp_path)
        assert observed["inference_backlog_ready_events"] == 1
        if event == "a":
            assert observed["publication_backlog_ready_events"] == 1
            assert observed["acceptance_backlog_ready_events"] == 0
        else:
            assert observed["publication_backlog_ready_events"] == 0
            assert observed["acceptance_backlog_ready_events"] == 1
    summary["accepted_ready_events"] = 3
    with pytest.raises(ReliabilityError, match="Inconsistent layered"):
        outage.backlog_snapshot(tmp_path)


def test_native_local_feature_read_exits_without_arrow_callback_shutdown_hang(
    tmp_path: Path,
) -> None:
    from test_datasets import feature, source

    delta = source(tmp_path, [feature(1)])
    code = (
        "import sys; from pathlib import Path; "
        "import sentinelaegisforge_control_plane.datasets.feature_source as f; "
        "s=f.StatisticalFeatureSource(Path(sys.argv[1])).read(); "
        "assert s.row_count == 1; print(s.delta_version)"
    )
    for _ in range(3):
        result = subprocess.run(
            [sys.executable, "-c", code, str(delta)],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "0"
