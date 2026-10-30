"""Bounded Docker restore simulation: private network, fresh volume, restored-only mounts."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import cast

from .bundle import verify
from .contract import RecoveryError, read, write
from .environment import assert_source_stopped, command, docker_object, kafka_snapshot
from .lineage import compare_identity


def storage_identity(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    # The image's tar restores directory mtime before children are created. Directory mtime
    # is not Kafka storage identity; retain file mtime, bytes, mode, owner and full hierarchy.
    return [
        {k: v for k, v in item.items() if not (item["directory"] and k == "mtime")}
        for item in entries
    ]


def check_broker_identity(
    expected: dict[str, object], observed: dict[str, object], startup_noops: list[dict[str, object]]
) -> None:
    # Registry appends NOOPs while acquiring its store's latest offset on startup.
    # Original broker file bytes were checked offline; only this observed suffix is allowed.
    normalized = json.loads(json.dumps(observed))
    source_topics = cast(dict[str, dict[str, object]], expected["topics"])
    end = cast(dict[str, int], source_topics["_schemas"]["end"])["0"]
    observed_end = normalized["topics"]["_schemas"]["end"]["0"]
    if observed_end - end != len(startup_noops) or any(
        item != {"key": {"keytype": "NOOP", "magic": 0}, "value": None} for item in startup_noops
    ):
        raise RecoveryError("Unexplained Schema Registry startup records")
    normalized["topics"]["_schemas"]["end"]["0"] = end
    compare_identity(expected, normalized, "Kafka and Schema Registry")


def compose(file: Path, *args: str, timeout: int = 120) -> str:
    return command(["docker", "compose", "-f", str(file), *args], timeout=timeout)


def stage(
    bundle: Path,
    restored: Path,
    output: Path,
    project: str,
    image: str = "sentinel-recovery-runtime:phase31",
) -> dict[str, object]:
    verify(bundle)
    cutoff = read(bundle / "source_cutoff.json")
    if not re.fullmatch(r"sentinel-recovery-[a-z0-9-]{1,40}", project):
        raise RecoveryError("Use a distinct sentinel-recovery-* project")
    assert_source_stopped(str(cutoff["source_compose_project"]), str(cutoff["source_volume"]))
    if output.exists():
        raise RecoveryError("Restore evidence target must be fresh")
    repository = Path(cast(dict[str, str], cutoff["selection"])["repository"])
    local = restored / str(repository).lstrip("/") / ".local"
    from .contract import unlinked

    unlinked(local)
    if not local.is_dir() or local == repository / ".local":
        raise RecoveryError("Require freshly restored hierarchy, never the source path")
    volume = project + "-kafka-data"
    if command(["docker", "volume", "ls", "--filter", f"name=^{volume}$", "-q"]).strip():
        raise RecoveryError("Restore Kafka volume already exists")
    output.mkdir(parents=True)
    # Reuse the verified source broker properties, excluding startup provisioning and host ports.
    source = json.loads(command(["docker", "compose", "config", "--format", "json"]))
    kafka = source["services"]["kafka"]
    for key in ("ports", "networks", "depends_on"):
        kafka.pop(key, None)
    kafka["volumes"] = ["kafka-data:/var/lib/kafka/data"]
    kafka["restart"] = "no"
    registry = source["services"]["schema-registry"]
    for key in ("ports", "networks", "hostname"):
        registry.pop(key, None)
    registry["network_mode"] = "service:kafka"
    registry["environment"]["SCHEMA_REGISTRY_HOST_NAME"] = "kafka"
    registry["environment"]["SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"] = (
        "PLAINTEXT://localhost:19092"
    )
    registry["restart"] = "no"
    config = {
        "name": project,
        "services": {"kafka": kafka, "schema-registry": registry},
        "volumes": {"kafka-data": {"external": True, "name": volume}},
        "networks": {"default": {"internal": True}},
    }
    write(output / "compose.json", config)
    command(["docker", "volume", "create", "--label", "sentinel.recovery=true", volume])
    # The verified archive rejects links/devices/traversal. Preserve numeric owners.
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
            f"type=volume,src={volume},dst=/target",
            "--mount",
            f"type=bind,src={bundle},dst=/bundle,readonly",
            "--entrypoint",
            "/bin/tar",
            "apache/kafka:4.3.1",
            "-xpf",
            "/bundle/payload/kafka-volume.tar",
            "-C",
            "/target",
        ],
        timeout=600,
    )
    # Independently hash the copied broker files before allowing Kafka to modify its logs.
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
            f"type=volume,src={volume},dst=/target,readonly",
            "--mount",
            f"type=bind,src={output},dst=/evidence",
            "--entrypoint",
            "/bin/tar",
            "apache/kafka:4.3.1",
            "-C",
            "/target",
            "-cf",
            "/evidence/restored-kafka.tar",
            ".",
        ],
        timeout=600,
    )
    from .archive import inspect_archive

    compare_identity(
        storage_identity(cast(list[dict[str, object]], cutoff["kafka_inventory"])),
        storage_identity(inspect_archive(output / "restored-kafka.tar")),
        "offline Kafka volume",
    )
    image_id = docker_object(["image", "inspect", image])[0]["Id"]
    result = {
        "bundle": str(bundle),
        "restored": str(restored),
        "local_mount": str(local),
        "repository": str(repository),
        "project": project,
        "volume": volume,
        "image": image_id,
        "output": str(output),
        "broker_preflight": "VERIFIED",
        "compose": str(output / "compose.json"),
    }
    write(output / "restore-environment.json", result)
    return result


def runtime_args(
    environment: dict[str, object], action: str, *, offline: bool = False
) -> list[str]:
    project = str(environment["project"])
    network = (
        "none"
        if offline
        else "container:" + compose(Path(str(environment["compose"])), "ps", "-q", "kafka").strip()
    )
    return [
        "docker",
        "run",
        "--name",
        f"{project}-{action}",
        "--network",
        network,
        "--mount",
        f"type=bind,src={environment['local_mount']},dst={environment['repository']}/.local",
        "--mount",
        f"type=bind,src={environment['bundle']},dst=/bundle,readonly",
        "--mount",
        f"type=bind,src={environment['output']},dst=/evidence",
        str(environment["image"]),
        action,
        "--bundle",
        "/bundle",
        "--output",
        "/evidence",
    ]


def check_mounts(actual: list[dict[str, object]], expected: dict[str, str]) -> None:
    mapping = {str(item["Destination"]): str(item["Source"]) for item in actual}
    if (
        len(mapping) != len(actual)
        or mapping != expected
        or any(item.get("Type") != "bind" for item in actual)
    ):
        raise RecoveryError("Unexpected mount or original-source fallback in restore runtime")
    for item in actual:
        if item["Destination"] == "/bundle" and item["RW"] is not False:
            raise RecoveryError("Recovery bundle must be read-only")


def run_runtime(output: Path, action: str, *, offline: bool = False) -> dict[str, object]:
    environment = read(output / "restore-environment.json")
    if action not in {"inspect-restored", "resume"}:
        raise RecoveryError("Unsupported restore runtime action")
    cutoff = read(Path(str(environment["bundle"])) / "source_cutoff.json")
    assert_source_stopped(str(cutoff["source_compose_project"]), str(cutoff["source_volume"]))
    args = runtime_args(environment, action, offline=offline)
    # Inspect actual mounts/network before execution. Retain exited container as evidence.
    command(["docker", "create", *args[2:]])
    name = f"{environment['project']}-{action}"
    inspected = docker_object(["inspect", name])[0]
    check_mounts(
        cast(list[dict[str, object]], inspected["Mounts"]),
        {
            f"{environment['repository']}/.local": str(environment["local_mount"]),
            "/bundle": str(environment["bundle"]),
            "/evidence": str(output),
        },
    )
    host = cast(dict[str, object], inspected["HostConfig"])
    if host["NetworkMode"] != args[args.index("--network") + 1] or host.get("Privileged"):
        raise RecoveryError("Unexpected network/privilege configuration")
    write(output / f"{action}-container.json", inspected)
    started = time.perf_counter()
    command(["docker", "start", name])
    code = command(["docker", "wait", name], timeout=1800).strip()
    logs = command(["docker", "logs", name], timeout=30)
    (output / f"{action}.log").write_text(logs)
    if code != "0":
        raise RecoveryError(f"Isolated {action} failed ({code}); inspect docker logs {name}")
    result = {"action": action, "exit_code": 0, "seconds": time.perf_counter() - started}
    write(output / f"{action}-execution.json", result)
    return result


def start(output: Path) -> dict[str, object]:
    environment = read(output / "restore-environment.json")
    if not (output / "inspect-restored-execution.json").is_file():
        raise RecoveryError("Read-only offline restored preflight must pass before starting broker")
    cutoff = read(Path(str(environment["bundle"])) / "source_cutoff.json")
    assert_source_stopped(str(cutoff["source_compose_project"]), str(cutoff["source_volume"]))
    started = time.perf_counter()
    compose(Path(str(environment["compose"])), "up", "-d", "--wait", timeout=300)
    network = docker_object(["network", "inspect", str(environment["project"]) + "_default"])[0]
    if network["Internal"] is not True:
        raise RecoveryError("Restored broker network is not internal")
    observed = kafka_snapshot(
        str(environment["project"]), str(cast(dict[str, object], cutoff["broker"])["group_id"])
    )
    expected = cast(dict[str, object], cutoff["broker"])
    topics = cast(dict[str, dict[str, object]], expected["topics"])
    old_end = cast(dict[str, int], topics["_schemas"]["end"])["0"]
    new_topics = cast(dict[str, dict[str, object]], observed["topics"])
    new_end = cast(dict[str, int], new_topics["_schemas"]["end"])["0"]
    count = new_end - old_end
    if count not in range(17):
        raise RecoveryError("Unexpected Schema Registry startup offset range")
    noops = []
    if count:
        raw = compose(
            Path(str(environment["compose"])),
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            "localhost:19092",
            "--topic",
            "_schemas",
            "--partition",
            "0",
            "--offset",
            str(old_end),
            "--max-messages",
            str(count),
            "--timeout-ms",
            "10000",
            "--formatter-property",
            "print.key=true",
            "--formatter-property",
            "key.separator=|",
        )
        for line in raw.splitlines():
            key, value = line.split("|", 1)
            noops.append({"key": json.loads(key), "value": json.loads(value)})
    check_broker_identity(expected, observed, noops)
    result = {
        "broker": observed,
        "network": network,
        "internal_startup_noops": noops,
        "seconds": time.perf_counter() - started,
    }
    write(output / "broker-pre-resume.json", result)
    return result
