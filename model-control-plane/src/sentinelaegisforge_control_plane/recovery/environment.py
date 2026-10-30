"""Explicit local Docker capture, never resets or removes a source volume."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from .archive import inspect_archive
from .contract import CUTOFF_VERSION, RecoveryError, digest, inventory, sha, write
from .lineage import Selection, dependencies


def command(args: list[str], *, timeout: int = 120) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise RecoveryError(f"Command failed: {args[0:3]}: {result.stderr[-1500:]}")
    return result.stdout


def docker_object(args: list[str]) -> list[dict[str, object]]:
    value = json.loads(command(["docker", *args]))
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise RecoveryError("Unexpected Docker metadata")
    return cast(list[dict[str, object]], value)


def assert_source_stopped(project: str, volume: str) -> None:
    # A stopped service is insufficient if any other running container mounts its volume.
    for filter_value in (f"label=com.docker.compose.project={project}", f"volume={volume}"):
        if command(["docker", "ps", "-q", "--filter", filter_value]).strip():
            raise RecoveryError("Source project/volume still has a running container")
    if len(docker_object(["volume", "inspect", volume])) != 1:
        raise RecoveryError("Missing source Kafka volume")


def host_writers() -> list[str]:
    """Conservative process-name check, supplemented by stable byte inventories.

    This is a cooperating single-operator protocol, not hostile process attestation.
    Failure to enumerate processes fails closed. Never terminate a user process.
    """
    raw = command(["ps", "-axo", "pid=,comm="])
    found = []
    ancestors = {os.getpid(), os.getppid()}
    for line in raw.splitlines():
        pair = line.strip().split(None, 1)
        if len(pair) != 2 or int(pair[0]) in ancestors:
            continue
        executable = Path(pair[1]).name.lower()
        if executable.startswith(("python", "java", "spark", "mlflow", "sbt")):
            args = command(["ps", "-p", pair[0], "-o", "args="]).strip()
            # The editor language server does not own pipeline storage. No generic JVM exemption.
            if args.endswith(" scala.meta.metals.Main"):
                continue
            found.append(line.strip())
    return found


def kafka_snapshot(project: str, group: str) -> dict[str, object]:
    def cli(tool: str, *args: str) -> str:
        return command(
            [
                "docker",
                "compose",
                "-p",
                project,
                "exec",
                "-T",
                "kafka",
                f"/opt/kafka/bin/{tool}.sh",
                "--bootstrap-server",
                "localhost:19092",
                *args,
            ]
        )

    quorum = cli("kafka-metadata-quorum", "describe", "--status")
    cluster = re.search(r"ClusterId:\s*(\S+)", quorum)
    leader = re.search(r"LeaderId:\s*(\d+)", quorum)
    if cluster is None or leader is None:
        raise RecoveryError("Cannot read KRaft cluster identity")
    topics: dict[str, object] = {}
    for topic in ("transactions.raw", "features.ready", "_schemas"):
        description = cli("kafka-topics", "--describe", "--topic", topic)
        match = re.search(
            r"TopicId:\s*(\S+).*PartitionCount:\s*(\d+).*ReplicationFactor:\s*(\d+)", description
        )
        if match is None:
            raise RecoveryError(f"Missing required Kafka topic: {topic}")
        bounds: dict[str, object] = {}
        for name, at in (("beginning", "-2"), ("end", "-1")):
            lines = cli("kafka-get-offsets", "--topic", topic, "--time", at).splitlines()
            bounds[name] = {
                line.split(":")[1]: int(line.split(":")[2])
                for line in lines
                if line.startswith(topic + ":")
            }
        topics[topic] = {
            "topic_id": match[1],
            "partitions": int(match[2]),
            "replication_factor": int(match[3]),
            **bounds,
        }
    offsets = cli("kafka-consumer-groups", "--describe", "--group", group)
    committed: dict[str, int] = {}
    for line in offsets.splitlines():
        fields = line.split()
        if len(fields) >= 4 and fields[0] == group and fields[1] == "features.ready":
            committed[fields[2]] = int(fields[3])
    if set(committed) != {"0", "1", "2"}:
        raise RecoveryError("Required research group offsets are incomplete")
    schemas = command(
        [
            "docker",
            "compose",
            "-p",
            project,
            "exec",
            "-T",
            "schema-registry",
            "python3",
            "-c",
            _SCHEMA_PROBE,
        ]
    )
    schema = json.loads(schemas)
    if "transactions.raw-value" not in schema:
        raise RecoveryError("Required Schema Registry subject is missing")
    return {
        "cluster_id": cluster[1],
        "broker_node_id": int(leader[1]),
        "topics": topics,
        "group_id": group,
        "committed_offsets": committed,
        "schemas": schema,
    }


_SCHEMA_PROBE = """
import json,urllib.request,urllib.parse
def get(p):
    return json.load(urllib.request.urlopen('http://localhost:8081'+p,timeout=10))
result={}
for subject in get('/subjects'):
    s=urllib.parse.quote(subject,safe='')
    result[subject]={
      'versions':[get('/subjects/'+s+'/versions/'+str(v))
                  for v in get('/subjects/'+s+'/versions')],
      'compatibility':get('/config/'+s+'?defaultToGlobal=true')}
print(json.dumps(result,sort_keys=True))
"""


def capture(
    selection: Selection, output: Path, project: str = "sentinelaegisforge"
) -> dict[str, object]:
    """Probe identities, stop services, then inventory and archive read-only storage."""
    if output.exists():
        raise RecoveryError("Capture destination exists; preserve the previous attempt")
    output.mkdir(parents=True)
    start = time.perf_counter()
    source = dependencies(selection)
    from sentinelaegisforge_control_plane.incremental_shadow.operations import (
        inspect as inspect_stream,
    )

    group = str(inspect_stream(selection.stream)["group_id"])
    live = kafka_snapshot(project, group)
    config = json.loads(command(["docker", "compose", "-p", project, "config", "--format", "json"]))
    volume = str(config["volumes"]["kafka-data"]["name"])
    metadata = docker_object(["volume", "inspect", volume])[0]
    if cast(dict[str, str], metadata["Labels"]).get("com.docker.compose.project") != project:
        raise RecoveryError("Kafka volume does not belong to the selected Compose project")
    writers = host_writers()
    if writers:
        write(output / "quiescence-refused.json", {"writers": writers})
        raise RecoveryError("Host JVM/Python writers must be stopped before capture")
    command(["docker", "compose", "-p", project, "stop", "schema-registry", "kafka"])
    assert_source_stopped(project, volume)
    nodes = cast(list[dict[str, object]], source["graph"])
    paths = [Path(str(node["path"])) for node in nodes]
    before = inventory(paths)
    sentinel = selection.repository / ".local/recovery/source-only.json"
    if not sentinel.exists():
        write(sentinel, {"purpose": "source-only isolation witness; excluded from backup"})
    archive = output / "kafka-volume.tar"
    command(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            "0",
            "--mount",
            f"type=volume,src={volume},dst=/source,readonly",
            "--mount",
            f"type=bind,src={output},dst=/backup",
            "--entrypoint",
            "/bin/tar",
            "apache/kafka:4.3.1",
            "-C",
            "/source",
            "-cf",
            "/backup/kafka-volume.tar",
            ".",
        ],
        timeout=600,
    )
    kafka_files = inspect_archive(archive)
    required_names = {str(item["path"]) for item in kafka_files}
    if not any(name.endswith("meta.properties") for name in required_names):
        raise RecoveryError("Kafka archive lacks KRaft storage metadata")
    if not any("_schemas-" in name for name in required_names):
        raise RecoveryError("Kafka archive lacks Schema Registry storage")
    if before != inventory(paths) or host_writers():
        raise RecoveryError("Source writers or content changes detected during capture")
    value = {
        **source,
        "contract_version": CUTOFF_VERSION,
        "git_revision": command(["git", "rev-parse", "HEAD"]).strip(),
        "source_compose_project": project,
        "source_volume": volume,
        "source_volume_metadata": metadata,
        "broker": live,
        "source_only_sentinel": str(sentinel),
        "sqlite_path": str(selection.tracking.database),
        "quiescence": "OBSERVED_STOPPED_WITH_STABLE_INVENTORY",
        "inventory": before,
        "kafka_archive_sha256": sha(archive),
        "kafka_inventory": kafka_files,
    }
    value["cutoff_id"] = digest(value)
    value["captured_at"] = datetime.now(UTC).isoformat()
    value["timings"] = {"quiescence_and_capture_seconds": time.perf_counter() - start}
    write(output / "cutoff.json", value)
    return value
