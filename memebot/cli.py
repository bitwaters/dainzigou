"""Ops CLI: status / export-signals / resend."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from memebot.config import AppConfig, load_config
from memebot.notify import Notifier, payload_from_signal_row
from memebot.store import Store


def _store(cfg_path: Path, env_path: Path) -> tuple[AppConfig, Store]:
    cfg = load_config(cfg_path, env_path)
    store = Store(
        Path(str(cfg.get("paths.db"))),
        Path(str(cfg.get("paths.backup_dir"))),
        int(cfg.get("storage.wal_autocheckpoint_pages")),
    )
    store.open()
    return cfg, store


def cmd_status(store: Store) -> str:
    day = datetime.now(UTC).date().isoformat()
    hanging = len(store.hanging_watches())
    return json.dumps(
        {
            "instance_id": store.kv_get("instance_id"),
            "started_at": store.kv_get("started_at"),
            "last_collection_ok_at": store.kv_get("last_collection_ok_at"),
            "daily_calls": store.daily_calls(day),
            "watch_calls": store.kind_calls(day, "watch"),
            "hanging_watches": hanging,
        },
        ensure_ascii=True,
        indent=2,
    )


def cmd_export(store: Store) -> str:
    rows = store.list_signals()
    out = [dict(r) for r in rows]
    negs = store.read(lambda c: c.execute("SELECT * FROM signal_outcomes").fetchall())
    return json.dumps({"signals": out, "outcomes": [dict(r) for r in negs]}, default=str, indent=2)


async def cmd_resend(cfg: AppConfig, store: Store, signal_id: int) -> str:
    row = store.get_signal(signal_id)
    if row is None:
        return f"signal {signal_id} not found"
    store.update_signal_status(signal_id, "pending")
    notifier = Notifier(store, cfg.raw, cfg.secrets.telegram_bot_token)
    payload = payload_from_signal_row(row)
    try:
        await notifier.send_signal(signal_id, payload)
    finally:
        await notifier.aclose()
    return f"resent {signal_id}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="memebot-cli")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--env", default=".env")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("export-signals")
    rs = sub.add_parser("resend")
    rs.add_argument("signal_id", type=int)
    args = parser.parse_args(argv)
    cfg, store = _store(Path(args.config), Path(args.env))
    try:
        if args.cmd == "status":
            print(cmd_status(store))
        elif args.cmd == "export-signals":
            print(cmd_export(store))
        else:
            print(asyncio.run(cmd_resend(cfg, store, args.signal_id)))
        return 0
    finally:
        store.close_sync()


if __name__ == "__main__":
    raise SystemExit(main())
