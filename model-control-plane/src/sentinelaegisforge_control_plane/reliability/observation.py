"""Measured state, distribution, timing and retention observations; no labels opened."""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import re
from collections import Counter
from pathlib import Path
from statistics import median
from typing import cast

import pyarrow.fs as pa_fs
from deltalake import DeltaTable

from sentinelaegisforge_control_plane.availability.contract import read_object
from sentinelaegisforge_control_plane.datasets.manifest import canonical_json
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import (
    STAGES,
    inspect_identity,
    read_plan,
)
from sentinelaegisforge_control_plane.stateful_shadow_integration.reconciliation import (
    verify_final,
)

from .contract import ReliabilityError, inspect_state_compatibility
from .state_examples import examples as state_examples

CONTRACT = "stateful-streaming-performance-observation-v1"
STATUSES = {
    "NO_HISTORY",
    "INSUFFICIENT_VARIANCE_HISTORY",
    "ZERO_VARIANCE",
    "READY",
}
LOG_TARGETS = {
    "deduplicated": "deduplicate-transactions",
    "rolling": "process-rolling-features",
    "statistical": "process-statistical-features",
}


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _progress(log: Path, stage: str) -> dict[str, object]:
    raw = log.read_bytes()
    marker = {
        "deduplicated": "Deduplication progress JSON: ",
        "rolling": "Rolling feature progress JSON: ",
        "statistical": "Statistical feature progress JSON: ",
    }[stage]
    batches: list[dict[str, object]] = []
    for line in raw.decode("utf-8").splitlines():
        if marker not in line:
            continue
        try:
            value = json.loads(line.split(marker, 1)[1])
        except (json.JSONDecodeError, IndexError) as error:
            raise ReliabilityError(f"Malformed Spark progress in {log}") from error
        if not isinstance(value, dict) or not isinstance(value.get("batchId"), int):
            raise ReliabilityError(f"Invalid Spark progress in {log}")
        batches.append(value)
    if not batches:
        raise ReliabilityError(f"Missing structured Spark progress in {log}")
    return {"log_sha256": _sha(raw), "batches": batches}


def _inventory(path: Path) -> dict[str, int]:
    if not path.is_dir() or path.is_symlink():
        raise ReliabilityError(f"Missing or linked state path: {path}")
    files = 0
    size = 0
    for item in path.rglob("*"):
        if item.is_symlink():
            raise ReliabilityError(f"Linked state artifact: {item}")
        if item.is_file():
            files += 1
            size += item.stat().st_size
    return {"files": files, "bytes": size}


def inspect(integration: Path, stream: Path) -> dict[str, object]:
    """Require exact Phase 28 reconciliation before characterizing a workload."""
    identity = inspect_identity(integration)
    compatibility = inspect_state_compatibility(integration)
    final = verify_final(integration, stream)
    plan = read_plan(Path(cast(str, identity["corpus_path"])))
    scenario_manifest = {
        key: value
        for key, value in (
            line.split("\t", 1)
            for line in (Path(cast(str, identity["corpus_path"])) / "manifest.tsv")
            .read_text(encoding="utf-8")
            .splitlines()
        )
        if key
        in {
            "scenario_contract_version",
            "seed",
            "base_time",
            "transaction_count",
            "customer_count",
            "time_horizon_days",
            "usd_weight_percent",
            "generator_config_sha256",
            "java_version",
            "scala_version",
            "plan_sha256",
            "wave_count",
        }
        or key.startswith(("wave_0_", "wave_1_", "wave_2_"))
    }
    paths = cast(dict[str, str], identity["paths"])
    checkpoint_paths = cast(dict[str, str], identity["checkpoints"])
    validated = cast(
        list[dict[str, object]],
        DeltaTable(paths["validated"])
        .to_pyarrow_table(
            columns=["event_id", "customer_id", "currency", "kafka_partition"],
            filesystem=pa_fs.SubTreeFileSystem(paths["validated"], pa_fs.LocalFileSystem()),
        )
        .to_pylist(),
    )
    statistical = cast(
        list[dict[str, object]],
        DeltaTable(paths["statistical"])
        .to_pyarrow_table(
            filesystem=pa_fs.SubTreeFileSystem(paths["statistical"], pa_fs.LocalFileSystem()),
            columns=[
                "event_id",
                "customer_id",
                "currency",
                "prior_amount_observation_count",
                "prior_amount_mean",
                "prior_amount_stddev",
                "amount_zscore",
                "statistical_feature_status",
            ],
        )
        .to_pylist(),
    )
    statuses = Counter(str(row["statistical_feature_status"]) for row in statistical)
    if not set(statuses) <= STATUSES:
        raise ReliabilityError(f"Unsupported statistical status: {set(statuses) - STATUSES}")
    if len(validated) != len(statistical) or len(validated) != len(plan.rows):
        raise ReliabilityError("Validated/statistical/plan populations differ")
    progress: list[dict[str, object]] = []
    timings: list[dict[str, object]] = []
    for wave in range(3):
        timing = read_object(integration / "timings" / f"wave-{wave}.json")
        durations = cast(dict[str, float], timing["durations"])
        if timing.get("wave") != wave or any(
            not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0
            for value in durations.values()
        ):
            raise ReliabilityError("Invalid wave timing observation")
        timings.append(timing)
        handoff = read_object(integration / "timings" / f"handoff-{wave}.json")
        timings.append(handoff)
        for stage, target in LOG_TARGETS.items():
            label = (
                f"wave-{wave}-recovery"
                if stage == "statistical"
                and wave == 0
                and (integration / "reliability" / "statistical-postcommit-recovery.json").is_file()
                else str(wave)
            )
            entry = _progress(integration / "logs" / f"{target}-{label}.log", stage)
            progress.append(
                {
                    "wave": wave,
                    "stage": stage,
                    "attempt_id": _sha(
                        canonical_json(
                            {
                                "integration_id": identity["integration_id"],
                                "wave": wave,
                                "stage": stage,
                                "log_sha256": entry["log_sha256"],
                            }
                        )
                    ),
                    "wave_output_count": read_object(integration / "waves" / f"{wave}.json")[
                        "wave_event_count"
                    ],
                    **entry,
                }
            )
    counts = Counter(str(row["kafka_partition"]) for row in validated)
    if any(key not in {"0", "1", "2"} for key in counts):
        raise ReliabilityError("Observed Kafka partition is outside the local topic")
    source_keys = Counter((str(row["customer_id"]), str(row["currency"])) for row in statistical)
    customers_by_partition = {
        partition: len(
            {
                str(row["customer_id"])
                for row in validated
                if str(row["kafka_partition"]) == partition
            }
        )
        for partition in sorted(counts)
    }
    customer_counts = Counter(str(row["customer_id"]) for row in validated)
    partition_mean = len(validated) / 3
    checkpoints = {stage: _inventory(Path(checkpoint_paths[stage])) for stage in STAGES}
    delta_storage = {stage: _inventory(Path(paths[stage])) for stage in STAGES}
    research_storage = {
        name: _inventory(stream / name)
        for name in ("segments", "outboxes", "batches", "journals", "receipts", "results")
    }
    stat_log = integration / "logs" / "process-statistical-features-0.log"
    if not stat_log.is_file():
        stat_log = integration / "logs" / "process-statistical-features-wave-0-injected.log"
    masters = re.findall(r"--spark-master=([^\s\"']+)", stat_log.read_text(encoding="utf-8"))
    decomposed: list[dict[str, object]] = []
    for entry in progress:
        wave = cast(int, entry["wave"])
        stage = cast(str, entry["stage"])
        batches = cast(list[dict[str, object]], entry["batches"])
        durations_ms = [
            cast(dict[str, object], batch.get("durationMs", {})).get("triggerExecution")
            for batch in batches
        ]
        available = [float(value) for value in durations_ms if isinstance(value, (int, float))]
        trigger_seconds = sum(available) / 1000.0 if available else None
        wave_durations = cast(dict[str, float], timings[2 * wave]["durations"])
        invocation_key = (
            "statistical_recovery"
            if stage == "statistical"
            and (integration / "reliability" / "statistical-postcommit-recovery.json").is_file()
            and wave == 0
            else stage
        )
        invocation = wave_durations.get(invocation_key)
        if invocation is None:
            raise ReliabilityError(f"Missing {stage} wave {wave} invocation duration")
        decomposed.append(
            {
                "wave": wave,
                "stage": stage,
                "cold_start_wave": wave == 0,
                "invocation_seconds": invocation,
                "reported_trigger_seconds": trigger_seconds,
                "nontrigger_seconds": (
                    max(0.0, invocation - trigger_seconds) if trigger_seconds is not None else None
                ),
                "interpretation": (
                    "nontrigger includes startup, query setup, teardown and log overhead"
                ),
            }
        )
    statistical_wave_zero = next(
        entry for entry in progress if entry["wave"] == 0 and entry["stage"] == "statistical"
    )
    continuity = state_examples(Path(paths["statistical"]), plan, statistical_wave_zero)
    return {
        "contract_version": CONTRACT,
        "integration_id": identity["integration_id"],
        "corpus_id": plan.corpus_id,
        "plan_sha256": plan.plan_sha256,
        "seed": plan.seed,
        "scenario_configuration": scenario_manifest,
        "source_event_count": len(plan.rows),
        "customer_count": len({row["customer_id"] for row in plan.rows}),
        "currency_counts": dict(sorted(Counter(row["currency"] for row in plan.rows).items())),
        "kafka_partition_counts": dict(sorted(counts.items())),
        "distinct_customers_by_partition": customers_by_partition,
        "customer_event_count_distribution": dict(sorted(customer_counts.items())),
        "maximum_customer_event_count": max(customer_counts.values()),
        "median_customer_event_count": median(customer_counts.values()),
        "partition_imbalance_ratio": max(counts.values()) / partition_mean,
        "partition_imbalance_definition": "max partition records / mean partition records",
        "customer_currency_state_keys": len(source_keys),
        "customer_currency_observations": {
            f"{customer}/{currency}": count
            for (customer, currency), count in sorted(source_keys.items())
        },
        "statistical_status_counts": {
            status: statuses.get(status, 0) for status in sorted(STATUSES)
        },
        "prior_observation_count_distribution": dict(
            sorted(
                Counter(str(row["prior_amount_observation_count"]) for row in statistical).items(),
                key=lambda item: int(item[0]),
            )
        ),
        "null_counts": {
            field: sum(row[field] is None for row in statistical)
            for field in ("prior_amount_mean", "prior_amount_stddev", "amount_zscore")
        },
        "spark_progress": progress,
        "timing_decomposition": decomposed,
        "state_continuity_examples": continuity,
        "invocation_timings": timings,
        "checkpoint_inventory": checkpoints,
        "delta_storage_inventory": delta_storage,
        "research_storage_inventory": research_storage,
        "resource_context": {
            "cpu_architecture": platform.machine(),
            "logical_cpu_count": os.cpu_count(),
            "spark_master": masters[-1] if masters else None,
            "spark_state_store_provider": compatibility["state_provider"],
            "spark_version": compatibility["spark_version"],
            "scala_version": compatibility["scala_version"],
            "peak_rss": None,
            "peak_cpu_utilization": None,
            "jvm_heap_limit": None,
            "spark_driver_memory": None,
            "unavailable_measurements": (
                "No OS sampler or structured JVM heap/driver-memory export was collected"
            ),
        },
        "runtime_semantics": {
            key: compatibility[key]
            for key in (
                "spark_version",
                "scala_version",
                "state_semantics",
                "state_provider",
                "watermark_delay",
                "inactivity_timeout",
            )
        },
        "state_compatibility_fingerprint": compatibility["semantic_fingerprint"],
        "final_reconciliation": final,
        "observation_method": (
            "Spark StreamingQueryProgress JSON and Python perf_counter per bounded invocation"
        ),
        "production_capacity_claim": False,
    }


def compare(baseline: dict[str, object], changed: dict[str, object]) -> dict[str, object]:
    if baseline.get("contract_version") != CONTRACT or changed.get("contract_version") != CONTRACT:
        raise ReliabilityError("Cannot compare unsupported performance observations")
    if baseline.get("integration_id") == changed.get("integration_id"):
        raise ReliabilityError("Comparison requires independent integration lineages")
    for key in ("runtime_semantics", "resource_context", "observation_method"):
        if key not in baseline or baseline.get(key) != changed.get(key):
            raise ReliabilityError(f"Comparison requires matching {key}")
    if all(
        baseline.get(field) == changed.get(field)
        for field in ("source_event_count", "customer_count", "currency_counts")
    ):
        raise ReliabilityError("Changed workload has no observable population/profile difference")
    samples: dict[str, dict[str, object]] = {}
    for label, item in (("A", baseline), ("B", changed)):
        stage_samples: dict[str, list[float]] = {}
        for timing in cast(list[dict[str, object]], item["invocation_timings"]):
            for stage, seconds in cast(dict[str, float], timing.get("durations", {})).items():
                if not math.isfinite(seconds) or seconds < 0:
                    raise ReliabilityError("Invalid invocation duration")
                stage_samples.setdefault(stage, []).append(float(seconds))
        if not stage_samples:
            raise ReliabilityError("No invocation-duration samples")
        samples[label] = {
            "stage_durations": {
                stage: {
                    "sample_count": len(durations),
                    "minimum_seconds": min(durations),
                    "median_seconds": median(durations),
                    "maximum_seconds": max(durations),
                    "wave_zero_is_cold_start": True,
                }
                for stage, durations in sorted(stage_samples.items())
            },
            "source_events": item["source_event_count"],
            "customer_currency_state_keys": item["customer_currency_state_keys"],
        }
    return {
        "contract_version": "stateful-streaming-comparison-v1",
        "sample_summary": samples,
        "interpretation": (
            "Local bounded invocations only; no steady-state throughput or production SLO"
        ),
    }
