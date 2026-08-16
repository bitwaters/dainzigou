"""Heartbeat file and HEALTHCHECK logic."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path


def write_heartbeat(path: Path, started_at: str, last_collection_ok_at: str | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"started_at": started_at, "last_collection_ok_at": last_collection_ok_at},
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )


def read_heartbeat(path: Path) -> dict[str, str | None]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        "started_at": data.get("started_at"),
        "last_collection_ok_at": data.get("last_collection_ok_at"),
    }


def is_healthy(payload: dict[str, str | None], now: datetime, stale_after_sec: float) -> bool:
    started = payload.get("started_at")
    last = payload.get("last_collection_ok_at")
    if not started:
        return False
    start_dt = datetime.fromisoformat(started)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=UTC)
    if not last:
        return (now - start_dt).total_seconds() < stale_after_sec
    last_dt = datetime.fromisoformat(last)
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=UTC)
    return (now - last_dt).total_seconds() < stale_after_sec
