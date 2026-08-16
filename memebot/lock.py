"""Single-instance flock lock."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
from types import TracebackType
from typing import TextIO


class InstanceLockError(RuntimeError):
    def __init__(self, path: Path, pid: int | None) -> None:
        self.path = path
        self.pid = pid
        msg = f"another instance holds {path}"
        if pid is not None:
            msg += f" (pid {pid})"
        super().__init__(msg)


class InstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: TextIO | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+", encoding="utf-8")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fh.seek(0)
            raw = fh.read().strip()
            fh.close()
            pid = int(raw) if raw.isdigit() else None
            raise InstanceLockError(self.path, pid) from exc
        fh.seek(0)
        fh.truncate()
        fh.write(str(os.getpid()))
        fh.flush()
        self._fh = fh

    def release(self) -> None:
        fh = self._fh
        if fh is None:
            return
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()
        self._fh = None

    def __enter__(self) -> InstanceLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
