"""Container HEALTHCHECK entry."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

from memebot.heartbeat import is_healthy, read_heartbeat


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) < 2:
        return 1
    path = Path(args[0])
    try:
        stale = float(args[1])
    except ValueError:
        return 1
    try:
        payload = read_heartbeat(path)
    except OSError:
        return 1
    return 0 if is_healthy(payload, datetime.now(UTC), stale) else 1


if __name__ == "__main__":
    raise SystemExit(main())
