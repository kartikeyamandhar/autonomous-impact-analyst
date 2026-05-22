"""Atomic, lock-guarded JSON read-modify-write.

The detection pipeline runs on a schedule; overlapping runs would otherwise
race on the same JSON file (lost updates, corruption). We guard read-modify-
write with an OS advisory lock and write via temp-file + os.replace so a reader
never sees a half-written file.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

try:
    import fcntl  # POSIX only
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None  # type: ignore[assignment]


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return default


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic on POSIX
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


@contextmanager
def locked_update(path: Path, default: Any) -> Iterator[list]:
    """Exclusive read-modify-write. Yields a 1-element list whose item is the
    loaded data; reassign element 0 to persist it on exit.

        with locked_update(path, {}) as box:
            box[0]["k"] = "v"
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_file = open(lock_path, "w")
    try:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        box = [read_json(path, default)]
        yield box
        write_json_atomic(path, box[0])
    finally:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()
