"""Small bounded recovery contracts and content identities, independent of Docker."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from pathlib import Path
from typing import cast

from sentinelaegisforge_control_plane.datasets.manifest import canonical_json

BUNDLE_VERSION = "synthetic-recovery-bundle-v1"
CUTOFF_VERSION = "recovery-cutoff-v1"
REHEARSAL_VERSION = "synthetic-recovery-rehearsal-v1"
LOGIC_VERSION = "quiesced-exact-path-v1"
MAX_FILES = 100_000
MAX_BYTES = 20 * 1024**3
MAX_METADATA = 32 * 1024**2


class RecoveryError(ValueError):
    """Required recovery evidence is missing, incompatible or unsafe."""


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def sha(path: Path) -> str:
    regular(path)
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def unlinked(path: Path) -> Path:
    """Reject symlinks in every ancestor, including dangling links; no alias fallback."""
    if not path.is_absolute() or ".." in path.parts:
        raise RecoveryError(f"Require an absolute non-traversing path: {path}")
    for item in (path, *path.parents):
        if item.is_symlink():
            raise RecoveryError(f"Symlink in recovery path: {item}")
    return path


def regular(path: Path) -> None:
    unlinked(path)
    if not path.is_file() or not stat.S_ISREG(path.stat().st_mode):
        raise RecoveryError(f"Missing/non-regular recovery file: {path}")


def read(path: Path) -> dict[str, object]:
    regular(path)
    if path.stat().st_size > MAX_METADATA:
        raise RecoveryError("Recovery metadata exceeds bounded size")
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
        if not isinstance(value, dict) or raw != canonical_json(value):
            raise RecoveryError(f"Noncanonical recovery object: {path}")
        return cast(dict[str, object], value)
    except (ValueError, TypeError) as error:
        raise RecoveryError(f"Invalid recovery object: {path}") from error


def write(path: Path, value: object) -> None:
    unlinked(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(canonical_json(value))
        output.flush()
        os.fsync(output.fileno())


def files(path: Path) -> list[Path]:
    unlinked(path)
    if not path.exists():
        raise RecoveryError(f"Missing required dependency: {path}")
    result: list[Path] = []
    for item in [path] if path.is_file() else sorted(path.rglob("*")):
        unlinked(item)
        if item.is_dir():
            continue
        regular(item)
        result.append(item)
        if len(result) > MAX_FILES:
            raise RecoveryError("Dependency exceeds file-count limit")
    if not result:
        raise RecoveryError(f"Required dependency is empty: {path}")
    return result


def inventory(paths: list[Path]) -> list[dict[str, object]]:
    entries = sorted({item for path in paths for item in files(path)})
    if len(entries) > MAX_FILES or sum(p.stat().st_size for p in entries) > MAX_BYTES:
        raise RecoveryError("Recovery inventory exceeds local resource limits")
    return [
        {
            "path": str(path),
            "size": path.stat().st_size,
            "mode": stat.S_IMODE(path.stat().st_mode),
            "sha256": sha(path),
        }
        for path in entries
    ]


def validate_graph(nodes: list[dict[str, object]]) -> None:
    by_id = {str(node["id"]): node for node in nodes}
    if len(by_id) != len(nodes) or not nodes:
        raise RecoveryError("Dependency IDs must be unique and nonempty")
    for node in nodes:
        if not node.get("reason") or not node.get("role"):
            raise RecoveryError("Each dependency requires a role and reason")
        unlinked(Path(str(node["path"])))
        edges = node.get("requires")
        if not isinstance(edges, list) or any(edge not in by_id for edge in edges):
            raise RecoveryError("Dependency closure contains an omitted node")
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> None:
        if key in visiting:
            raise RecoveryError("Dependency cycle")
        if key in visited:
            return
        visiting.add(key)
        for edge in cast(list[str], by_id[key]["requires"]):
            visit(edge)
        visiting.remove(key)
        visited.add(key)

    for key in by_id:
        visit(key)


def sqlite_identity(path: Path) -> dict[str, object]:
    regular(path)
    try:
        with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as database:
            if database.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise RecoveryError("SQLite integrity check failed")
            # Logical content comparison permits the supported backup API to change page layout.
            schema = database.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall()
            tables = [row[1] for row in schema if row[0] == "table"]
            rows = {}
            for name in tables:
                quoted = '"' + name.replace('"', '""') + '"'
                values = database.execute(f"SELECT * FROM {quoted}").fetchall()
                normalized = [
                    [value.hex() if isinstance(value, bytes) else value for value in row]
                    for row in values
                ]
                rows[name] = sorted(normalized, key=canonical_json)
            return {
                "logical_sha256": digest({"schema": schema, "tables": rows}),
                "journal_mode": database.execute("PRAGMA journal_mode").fetchone()[0],
                "tables": {name: len(value) for name, value in rows.items()},
            }
    except sqlite3.Error as error:
        raise RecoveryError(f"Cannot inspect SQLite: {path}: {error}") from error


def backup_sqlite(source: Path, target: Path) -> dict[str, object]:
    before = sqlite_identity(source)
    unlinked(target)
    # Claim before opening SQLite; never truncate or attach an existing destination.
    with target.open("xb"):
        pass
    try:
        with sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True) as src:
            with sqlite3.connect(str(target)) as dst:
                src.backup(dst)
        after = sqlite_identity(target)
        if before["logical_sha256"] != after["logical_sha256"]:
            raise RecoveryError("SQLite backup content differs")
        if before != sqlite_identity(source):
            raise RecoveryError("SQLite source changed during quiesced backup")
        return {"source": before, "backup": after, "backup_sha256": sha(target)}
    except sqlite3.Error as error:
        raise RecoveryError(f"SQLite backup failed: {error}") from error
