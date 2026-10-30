"""Build context from locked dependencies and sbt's existing resolved JVM classpath."""

import argparse
import shutil
from pathlib import Path

from .contract import RecoveryError, sha, write


def stage(repository: Path, context: Path, coursier_cache: Path) -> None:
    lines = (context / "classpath.txt").read_text().splitlines()
    resolved = [line for line in lines if line.startswith("List(") and line.endswith(")")]
    if len(resolved) != 1:
        raise RecoveryError("Expected one sbt 2 Runtime/fullClasspath export")
    destination = context / "jvm"
    destination.mkdir()
    entries = []
    for value in resolved[0][5:-1].split(", "):
        path = Path(
            value.split(">sha256-")[0]
            .replace("${OUT}", str(repository / "streaming-engine/target/out"))
            .replace("${CSR_CACHE}", str(coursier_cache))
        )
        target = destination / path.name
        if target.exists():
            raise RecoveryError("Duplicate JVM classpath filename")
        shutil.copyfile(path, target)
        entries.append({"file": target.name, "sha256": sha(target)})
    shutil.copytree(
        repository / "model-control-plane/src",
        context / "python",
        ignore=shutil.ignore_patterns("__pycache__", "*.egg-info"),
    )
    write(context / "classpath-inventory.json", entries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--coursier-cache", type=Path, required=True)
    args = parser.parse_args()
    stage(args.repository, args.context, args.coursier_cache)
