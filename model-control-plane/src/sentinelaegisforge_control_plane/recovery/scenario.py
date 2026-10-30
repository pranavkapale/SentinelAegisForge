"""One explicit 30-event recovery experiment, reusing existing bounded operations."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.incremental_shadow.operations import inspect as inspect_stream
from sentinelaegisforge_control_plane.incremental_shadow.progress import progress
from sentinelaegisforge_control_plane.reliability.contract import inspect_state_compatibility
from sentinelaegisforge_control_plane.reliability.operations import run_wave
from sentinelaegisforge_control_plane.stateful_shadow_integration.cli import (
    _handoff,
    _initialize,
    _make,
    _prime,
)
from sentinelaegisforge_control_plane.stateful_shadow_integration.contract import read_plan

from .contract import RecoveryError, write


def prepare(corpus: Path, root: Path, fence: Path) -> dict[str, object]:
    plan = read_plan(corpus)
    if len(plan.rows) != 30 or {len(plan.wave_ids(w)) for w in range(3)} != {10}:
        raise RecoveryError("Recovery rehearsal requires exactly three ten-event waves")
    identity = _initialize(
        Namespace(corpus=corpus, root=root / "integrations", transaction_fence=fence)
    )
    integration = Path(str(identity["integration_path"]))
    if (integration / "source-prime.json").exists():
        raise RecoveryError(
            "Preparation already started; inspect attempts, never replay automatically"
        )
    _prime(Namespace(integration=integration))
    run_wave(integration, 0)
    _make(
        "initialize-incremental-shadow",
        identity,
        {
            "INCREMENTAL_SOURCE_DELTA_PATH": cast(dict[str, str], identity["paths"])["statistical"],
            "INCREMENTAL_ROOT": str(root / "streams"),
            "INCREMENTAL_FENCE_PATH": str(root / "ready-fence.json"),
            "INCREMENTAL_GROUP": str(identity["group_id"]),
            "INCREMENTAL_TICK_LIMIT": "10",
            "INCREMENTAL_ACCEPTED_LIMIT": "100",
        },
        label="recovery-initialize",
    )
    streams = list((root / "streams").glob("*/identity/stream_manifest.json"))
    if len(streams) != 1:
        raise RecoveryError("Expected one isolated recovery research stream")
    stream = streams[0].parent.parent
    inspect_stream(stream)
    _handoff(
        Namespace(
            integration=integration,
            stream=stream,
            wave=0,
            pause_inference=True,
            demonstrate_backpressure=False,
        )
    )
    _make(
        "process-incremental-shadow-work",
        identity,
        {
            "INCREMENTAL_STREAM_PATH": str(stream),
            "INCREMENTAL_TICK_LIMIT": "5",
        },
        label="recovery-five-results",
    )
    # Real pending state: ten new acknowledged source events reach rolling Delta,
    # but the statistical checkpoint still represents wave zero.
    for target in (
        "publish-synthetic-wave",
        "ingest-delta",
        "deduplicate-transactions",
        "process-rolling-features",
    ):
        _make(target, identity, {"SCENARIO_WAVE": "1"})
    inspect_state_compatibility(integration)
    observed = progress(stream)
    if observed["inference_backlog_ready_events"] != 5:
        raise RecoveryError("Expected five durably accepted, unprocessed requests")
    result = {
        "integration": str(integration),
        "stream": str(stream),
        "corpus": str(corpus),
        "pending_statistical_wave": 1,
        "unpublished_wave": 2,
        "research_progress": observed,
    }
    write(root / "prepared.json", result)
    return result
