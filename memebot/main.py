"""Process entry: logging, instance lock, SIGTERM within grace_sec."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from memebot.config import AppConfig, ConfigError, load_config
from memebot.lock import InstanceLock, InstanceLockError
from memebot.runtime import Runtime
from memebot.store import Store


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
        force=True,
    )


async def _run(cfg: AppConfig, store: Store) -> None:
    runtime = Runtime(cfg, store)
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        logging.getLogger("memebot").info("shutdown requested")
        runtime.request_stop()

    loop.add_signal_handler(signal.SIGTERM, _request_stop)
    loop.add_signal_handler(signal.SIGINT, _request_stop)
    grace = float(cfg.get("runtime.shutdown.grace_sec"))
    try:
        await runtime.run()
    finally:
        await asyncio.wait_for(store.close(), timeout=grace)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="memebot")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--env", default=".env")
    args = parser.parse_args(argv)
    try:
        cfg = load_config(Path(args.config), Path(args.env))
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    setup_logging(str(cfg.get("runtime.log_level")))
    lock_path = Path(str(cfg.get("paths.lock_file")))
    lock = InstanceLock(lock_path)
    try:
        lock.acquire()
    except InstanceLockError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    store: Store | None = None
    try:
        store = Store(
            Path(str(cfg.get("paths.db"))),
            backup_dir=Path(str(cfg.get("paths.backup_dir"))),
            wal_autocheckpoint_pages=int(cfg.get("storage.wal_autocheckpoint_pages")),
        )
        store.open()
        asyncio.run(_run(cfg, store))
        return 0
    except Exception as exc:  # noqa: BLE001 — entrypoint
        logging.getLogger("memebot").exception("fatal: %s", exc)
        return 1
    finally:
        if store is not None:
            store.close_sync()
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
