"""Strict two-pass tar validation; no extractall, links, devices or overwrite."""

from __future__ import annotations

import hashlib
import os
import shutil
import tarfile
from pathlib import Path, PurePosixPath

from .contract import MAX_BYTES, MAX_FILES, RecoveryError, regular, unlinked


def safe_name(name: str) -> str:
    # Docker tar may emit ./ prefixes; reject alternate encodings and traversal.
    while name.startswith("./"):
        name = name[2:]
    if name in {"", "."}:
        return ""
    path = PurePosixPath(name)
    if (
        path.is_absolute()
        or ".." in path.parts
        or "\\" in name
        or "\x00" in name
        or any(ord(char) < 32 for char in name)
        or path.as_posix() != name.rstrip("/")
    ):
        raise RecoveryError(f"Unsafe archive path: {name!r}")
    return path.as_posix()


def inspect_archive(
    path: Path, *, max_bytes: int = MAX_BYTES, max_files: int = MAX_FILES
) -> list[dict[str, object]]:
    regular(path)
    seen: set[str] = set()
    kinds: dict[str, bool] = {}
    result: list[dict[str, object]] = []
    total = 0
    try:
        with tarfile.open(path, "r:*") as archive:
            for member in archive:
                name = safe_name(member.name)
                if not name and member.isdir():
                    continue
                if not name or name in seen or not (member.isfile() or member.isdir()):
                    raise RecoveryError("Duplicate/link/device/unknown archive member")
                if member.mode & 0o7000 or member.size < 0 or member.sparse is not None:
                    raise RecoveryError("Unsafe mode, size or sparse archive member")
                seen.add(name)
                kinds[name] = member.isdir()
                total += member.size
                if len(seen) > max_files or total > max_bytes:
                    raise RecoveryError("Archive expansion exceeds configured limit")
                value = hashlib.sha256()
                if member.isfile():
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise RecoveryError("Unreadable archive member")
                    with stream:
                        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                            value.update(chunk)
                result.append(
                    {
                        "path": name,
                        "directory": member.isdir(),
                        "size": member.size,
                        "mode": member.mode,
                        "uid": member.uid,
                        "gid": member.gid,
                        "mtime": member.mtime,
                        "sha256": value.hexdigest(),
                    }
                )
        for name in seen:
            if any(
                str(parent) in kinds and not kinds[str(parent)]
                for parent in PurePosixPath(name).parents
            ):
                raise RecoveryError("File/directory archive collision")
        if not result:
            raise RecoveryError("Empty recovery archive")
        return sorted(result, key=lambda item: str(item["path"]))
    except (tarfile.TarError, OSError) as error:
        raise RecoveryError(f"Invalid recovery archive: {error}") from error


def extract_archive(path: Path, target: Path) -> list[dict[str, object]]:
    from .contract import sha

    archive_hash = sha(path)
    expected = inspect_archive(path)
    by_name = {str(item["path"]): item for item in expected}
    unlinked(target)
    if target.exists():
        raise RecoveryError("Restore destination already exists; require a fresh target")
    target.mkdir(parents=True)
    with tarfile.open(path, "r:*") as archive:
        for member in archive:
            name = safe_name(member.name)
            if not name:
                continue
            item = by_name.get(name)
            if (
                item is None
                or not (member.isdir() or member.isfile())
                or member.isdir() != item["directory"]
                or member.size != item["size"]
                or member.mode != item["mode"]
            ):
                raise RecoveryError("Archive changed between validation and extraction")
            output = target / name
            unlinked(output)
            if member.isdir():
                output.mkdir(parents=True, exist_ok=True)
                continue
            output.parent.mkdir(parents=True, exist_ok=True)
            stream = archive.extractfile(member)
            if stream is None:
                raise RecoveryError("Missing archive stream")
            # O_EXCL + O_NOFOLLOW and private fresh root; not a hostile multiwriter extractor.
            fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with stream, os.fdopen(fd, "wb") as destination:
                shutil.copyfileobj(stream, destination, length=1024 * 1024)
                destination.flush()
                os.fsync(destination.fileno())
            output.chmod(member.mode)
            os.utime(output, (member.mtime, member.mtime))
    for item in sorted(expected, key=lambda item: len(str(item["path"])), reverse=True):
        if item["directory"]:
            directory = target / str(item["path"])
            directory.chmod(int(str(item["mode"])))
    # Byte verification is mandatory even after tar parsing succeeds.
    if sha(path) != archive_hash:
        raise RecoveryError("Archive changed during extraction")
    for item in expected:
        if not item["directory"] and sha(target / str(item["path"])) != item["sha256"]:
            raise RecoveryError("Restored archive content differs")
    return expected
