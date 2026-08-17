"""SQLite persistence: new momentum schema, WAL, single writer thread."""

from __future__ import annotations

import asyncio
import json
import queue
import sqlite3
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from shutil import copy2
from typing import Any, TypeVar, cast

SCHEMA_VERSION = 1
TABLES = (
    "security_cache",
    "legs",
    "signals",
    "step_counts",
    "signal_outcomes",
    "credit_usage",
    "kv",
    "event_log",
)
STEP_NAMES = frozenset(
    {
        "radar_input",
        "gate_chain",
        "gate_quote",
        "gate_age",
        "gate_liq",
        "gate_fdv",
        "gate_turnover",
        "gate_m5",
        "detect_quota",
        "leg_cooldown",
        "gate_dex",
        "gate_buyers",
        "gate_wash",
        "gate_bs_ratio",
        "security_reject",
        "security_transient",
        "grade_input",
        "ohlcv_fail",
        "trades_fail",
        "trade_wash",
        "trade_imbalance",
        "net_buy_nonpositive",
        "pullback",
        "grade_none",
        "pushed_strong",
        "pushed_weak",
    }
)
CREDIT_KINDS = frozenset({"collect", "ohlcv", "trades", "goplus"})
CG_CREDIT_KINDS = frozenset({"collect", "ohlcv", "trades"})

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS security_cache (
  network TEXT NOT NULL,
  token_address TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pass', 'reject', 'transient')),
  mintable INTEGER,
  freezable INTEGER,
  expires_at TEXT NOT NULL,
  checked_at TEXT NOT NULL,
  PRIMARY KEY (network, token_address)
);
CREATE INDEX IF NOT EXISTS idx_security_cache_expires_at ON security_cache(expires_at);

CREATE TABLE IF NOT EXISTS legs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  network TEXT NOT NULL,
  token_address TEXT NOT NULL,
  high_price REAL,
  ended INTEGER NOT NULL DEFAULT 0,
  last_seen_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_legs_open
  ON legs(network, token_address) WHERE ended = 0;
CREATE INDEX IF NOT EXISTS idx_legs_last_seen_at ON legs(last_seen_at);

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  network TEXT NOT NULL,
  token_address TEXT NOT NULL,
  pool_address TEXT NOT NULL,
  grade TEXT NOT NULL CHECK (grade IN ('strong', 'weak')),
  leg_id INTEGER NOT NULL,
  price_at_signal REAL,
  fdv_usd REAL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  sent_at TEXT,
  fail_count INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (leg_id) REFERENCES legs(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_signals_live
  ON signals(network, token_address, grade, leg_id)
  WHERE status IN ('pending', 'sent');
CREATE INDEX IF NOT EXISTS idx_signals_status_created ON signals(status, created_at);

CREATE TABLE IF NOT EXISTS step_counts (
  date_utc TEXT NOT NULL,
  step TEXT NOT NULL,
  n INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (date_utc, step)
);

CREATE TABLE IF NOT EXISTS signal_outcomes (
  signal_id INTEGER PRIMARY KEY,
  attempts INTEGER NOT NULL DEFAULT 0,
  expire_price REAL,
  rel_change_pct REAL,
  peak_price REAL,
  drawdown_pct REAL,
  deep_drawdown INTEGER,
  evaluated_at TEXT,
  failed INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (signal_id) REFERENCES signals(id)
);

CREATE TABLE IF NOT EXISTS credit_usage (
  date_utc TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('collect', 'ohlcv', 'trades', 'goplus')),
  calls INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (date_utc, kind)
);

CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS event_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  ts TEXT NOT NULL,
  payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log(ts);
"""

T = TypeVar("T")


@dataclass(frozen=True)
class CleanupConfig:
    outcomes_retain_days: float
    event_log_retain_days: float
    legs_retain_days: float
    now: datetime


def cleanup_config_from_raw(raw: dict[str, Any], now: datetime | None = None) -> CleanupConfig:
    storage = raw["storage"]
    return CleanupConfig(
        outcomes_retain_days=float(storage["outcomes_retain_days"]),
        event_log_retain_days=float(storage["event_log_retain_days"]),
        legs_retain_days=float(storage["legs_retain_days"]),
        now=now or datetime.now(UTC),
    )


@dataclass
class _Job:
    fn: Callable[[sqlite3.Connection], Any]
    future: Future[Any]


class Store:
    def __init__(
        self,
        db_path: Path,
        backup_dir: Path,
        wal_autocheckpoint_pages: int,
    ) -> None:
        self.db_path = db_path
        self.backup_dir = backup_dir
        self.wal_autocheckpoint_pages = wal_autocheckpoint_pages
        self._queue: queue.Queue[_Job | None] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._local = threading.local()
        self._readers: list[sqlite3.Connection] = []
        self._readers_lock = threading.Lock()
        self._opened = False
        self._closed = False

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
        conn.execute(f"PRAGMA wal_autocheckpoint={int(self.wal_autocheckpoint_pages)}")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _backup(self, from_version: int) -> Path | None:
        if not self.db_path.is_file() or self.db_path.stat().st_size == 0:
            return None
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        dest = self.backup_dir / f"momentum-v{from_version}-{stamp}.db"
        copy2(self.db_path, dest)
        for suffix in ("-wal", "-shm"):
            extra = Path(f"{self.db_path}{suffix}")
            if extra.is_file():
                copy2(extra, Path(f"{dest}{suffix}"))
        return dest

    def _migrate(self) -> None:
        conn = self._connect()
        try:
            current = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if current > SCHEMA_VERSION:
                raise RuntimeError(
                    f"database user_version {current} is newer than code {SCHEMA_VERSION}"
                )
            if current < SCHEMA_VERSION:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self._backup(current)
                conn.executescript(_SCHEMA_SQL)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                conn.commit()
        finally:
            conn.close()

    def _writer_loop(self) -> None:
        conn = self._connect()
        try:
            while True:
                job = self._queue.get()
                if job is None:
                    break
                try:
                    result = job.fn(conn)
                    conn.commit()
                    job.future.set_result(result)
                except Exception as exc:  # noqa: BLE001 — surface to caller
                    conn.rollback()
                    job.future.set_exception(exc)
        finally:
            conn.close()

    def open(self) -> None:
        if self._opened:
            raise RuntimeError("store already opened")
        self._migrate()
        self._thread = threading.Thread(
            target=self._writer_loop, name="memebot-db-writer", daemon=False
        )
        self._thread.start()
        self._opened = True
        now = datetime.now(UTC).isoformat()
        self.kv_set("instance_id", uuid.uuid4().hex)
        self.kv_set("heartbeat_at", now)
        self.kv_set("started_at", now)

    def submit(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        if not self._opened or self._closed:
            raise RuntimeError("store is not open")
        job = _Job(fn=fn, future=Future())
        self._queue.put(job)
        return cast(T, job.future.result())

    async def execute(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await asyncio.to_thread(self.submit, fn)

    def _reader(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
            with self._readers_lock:
                self._readers.append(conn)
        return conn

    def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        if not self._opened or self._closed:
            raise RuntimeError("store is not open")
        return fn(self._reader())

    def close_sync(self) -> None:
        if not self._opened or self._closed:
            return
        self._closed = True
        self._queue.put(None)
        if self._thread is not None:
            self._thread.join()
            self._thread = None
        with self._readers_lock:
            for conn in self._readers:
                conn.close()
            self._readers.clear()

    async def close(self) -> None:
        await asyncio.to_thread(self.close_sync)

    def user_version(self) -> int:
        row = self.read(lambda c: c.execute("PRAGMA user_version").fetchone())
        return int(row[0])

    def table_names(self) -> set[str]:
        rows = self.read(
            lambda c: c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        )
        return {str(r[0]) for r in rows}

    def kv_set(self, key: str, value: str | None) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT INTO kv(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

        self.submit(_fn)

    def kv_get(self, key: str) -> str | None:
        row = self.read(
            lambda c: c.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        )
        return None if row is None else row[0]

    def incr_step(self, date_utc: str, step: str, n: int = 1) -> int:
        if step not in STEP_NAMES:
            raise ValueError(f"unknown step {step}")

        def _fn(conn: sqlite3.Connection) -> int:
            conn.execute(
                "INSERT INTO step_counts(date_utc, step, n) VALUES (?, ?, ?) "
                "ON CONFLICT(date_utc, step) DO UPDATE SET n = n + excluded.n",
                (date_utc, step, n),
            )
            row = conn.execute(
                "SELECT n FROM step_counts WHERE date_utc = ? AND step = ?",
                (date_utc, step),
            ).fetchone()
            return int(row[0])

        return self.submit(_fn)

    def get_step(self, date_utc: str, step: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT n FROM step_counts WHERE date_utc = ? AND step = ?",
                (date_utc, step),
            ).fetchone()
        )
        return 0 if row is None else int(row[0])

    def add_credits(self, date_utc: str, kind: str, calls: int = 1) -> int:
        if kind not in CREDIT_KINDS:
            raise ValueError(f"unknown credit kind {kind}")

        def _fn(conn: sqlite3.Connection) -> int:
            conn.execute(
                "INSERT INTO credit_usage(date_utc, kind, calls) VALUES (?, ?, ?) "
                "ON CONFLICT(date_utc, kind) DO UPDATE SET calls = calls + excluded.calls",
                (date_utc, kind, calls),
            )
            row = conn.execute(
                "SELECT calls FROM credit_usage WHERE date_utc = ? AND kind = ?",
                (date_utc, kind),
            ).fetchone()
            return int(row[0])

        return self.submit(_fn)

    def insert_event(self, event_type: str, ts: str, payload: str) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT INTO event_log(type, ts, payload) VALUES (?, ?, ?)",
                (event_type, ts, payload),
            )

        self.submit(_fn)

    def cg_calls_today(self, date_utc: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COALESCE(SUM(calls), 0) FROM credit_usage "
                "WHERE date_utc = ? AND kind IN ('collect', 'ohlcv', 'trades')",
                (date_utc,),
            ).fetchone()
        )
        return int(row[0])

    def put_security(
        self,
        network: str,
        token_address: str,
        status: str,
        expires_at: str,
        *,
        mintable: bool | None = None,
        freezable: bool | None = None,
    ) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT INTO security_cache("
                "network, token_address, status, mintable, freezable, expires_at, checked_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(network, token_address) DO UPDATE SET "
                "status = excluded.status, mintable = excluded.mintable, "
                "freezable = excluded.freezable, expires_at = excluded.expires_at, "
                "checked_at = excluded.checked_at",
                (
                    network,
                    token_address,
                    status,
                    None if mintable is None else int(mintable),
                    None if freezable is None else int(freezable),
                    expires_at,
                    datetime.now(UTC).isoformat(),
                ),
            )

        self.submit(_fn)

    def get_security(self, network: str, token_address: str) -> sqlite3.Row | None:
        now = datetime.now(UTC).isoformat()
        return cast(
            sqlite3.Row | None,
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM security_cache "
                    "WHERE network = ? AND token_address = ? AND expires_at > ?",
                    (network, token_address, now),
                ).fetchone()
            ),
        )

    def open_leg(
        self,
        network: str,
        token_address: str,
        *,
        high_price: float | None,
        last_seen_at: str,
    ) -> int:
        if high_price is not None and high_price <= 0:
            raise ValueError("high_price must be null or > 0")

        def _fn(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "INSERT INTO legs(network, token_address, high_price, ended, "
                "last_seen_at, created_at) VALUES (?, ?, ?, 0, ?, ?)",
                (
                    network,
                    token_address,
                    high_price,
                    last_seen_at,
                    datetime.now(UTC).isoformat(),
                ),
            )
            rowid = cur.lastrowid
            if rowid is None:
                raise RuntimeError("insert produced no rowid")
            return int(rowid)

        return self.submit(_fn)

    def get_open_leg(self, network: str, token_address: str) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM legs WHERE network = ? AND token_address = ? AND ended = 0",
                    (network, token_address),
                ).fetchone()
            ),
        )

    def get_last_ended_leg(self, network: str, token_address: str) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM legs WHERE network = ? AND token_address = ? AND ended = 1 "
                    "ORDER BY id DESC LIMIT 1",
                    (network, token_address),
                ).fetchone()
            ),
        )

    def update_leg(
        self,
        leg_id: int,
        *,
        high_price: float | None | object = ...,
        last_seen_at: str | None = None,
        ended: bool | None = None,
    ) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            row = conn.execute("SELECT * FROM legs WHERE id = ?", (leg_id,)).fetchone()
            if row is None:
                raise KeyError(leg_id)
            raw: Any = row["high_price"] if high_price is ... else high_price
            price = None if raw is None else float(raw)
            if price is not None and price <= 0:
                raise ValueError("high_price must be null or > 0")
            seen = row["last_seen_at"] if last_seen_at is None else last_seen_at
            flag = row["ended"] if ended is None else int(ended)
            conn.execute(
                "UPDATE legs SET high_price = ?, last_seen_at = ?, ended = ? WHERE id = ?",
                (price, seen, flag, leg_id),
            )

        self.submit(_fn)

    def list_open_legs(self) -> list[sqlite3.Row]:
        return list(self.read(lambda c: c.execute("SELECT * FROM legs WHERE ended = 0").fetchall()))

    def insert_pending_signal(
        self,
        *,
        network: str,
        token_address: str,
        pool_address: str,
        grade: str,
        leg_id: int,
        price_at_signal: float,
        fdv_usd: float | None,
    ) -> int:
        def _fn(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "INSERT INTO signals(network, token_address, pool_address, grade, leg_id, "
                "price_at_signal, fdv_usd, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)",
                (
                    network,
                    token_address,
                    pool_address,
                    grade,
                    leg_id,
                    price_at_signal,
                    fdv_usd,
                    datetime.now(UTC).isoformat(),
                ),
            )
            rowid = cur.lastrowid
            if rowid is None:
                raise RuntimeError("insert produced no rowid")
            return int(rowid)

        return self.submit(_fn)

    def mark_signal_sent(self, signal_id: int, event_type: str, payload: dict[str, Any]) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            now = datetime.now(UTC).isoformat()
            conn.execute(
                "UPDATE signals SET status = 'sent', sent_at = ? WHERE id = ?",
                (now, signal_id),
            )
            conn.execute(
                "INSERT INTO event_log(type, ts, payload) VALUES (?, ?, ?)",
                (event_type, now, json.dumps(payload, ensure_ascii=True)),
            )

        self.submit(_fn)

    def has_live_signal(self, network: str, token_address: str, grade: str, leg_id: int) -> bool:
        row = self.read(
            lambda c: c.execute(
                "SELECT 1 FROM signals WHERE network = ? AND token_address = ? "
                "AND grade = ? AND leg_id = ? AND status IN ('pending', 'sent')",
                (network, token_address, grade, leg_id),
            ).fetchone()
        )
        return row is not None

    def pending_or_sent_grades(self, network: str, token_address: str, leg_id: int) -> set[str]:
        rows = self.read(
            lambda c: c.execute(
                "SELECT grade FROM signals WHERE network = ? AND token_address = ? "
                "AND leg_id = ? AND status IN ('pending', 'sent')",
                (network, token_address, leg_id),
            ).fetchall()
        )
        return {str(r["grade"]) for r in rows}

    def get_signal(self, signal_id: int) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            self.read(
                lambda c: c.execute("SELECT * FROM signals WHERE id = ?", (signal_id,)).fetchone()
            ),
        )

    def list_signals(self, *, status: str | None = None, limit: int = 10) -> list[sqlite3.Row]:
        if status is None:
            return list(
                self.read(
                    lambda c: c.execute(
                        "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
                    ).fetchall()
                )
            )
        return list(
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM signals WHERE status = ? ORDER BY id DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
            )
        )

    def list_pending(self) -> list[sqlite3.Row]:
        return list(
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM signals WHERE status = 'pending' ORDER BY id"
                ).fetchall()
            )
        )

    def update_signal_status(self, signal_id: int, status: str, *, add_fails: int = 0) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE signals SET status = ?, fail_count = fail_count + ? WHERE id = ?",
                (status, add_fails, signal_id),
            )

        self.submit(_fn)

    def count_send_failures(
        self, network: str, token_address: str, grade: str, leg_id: int
    ) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COALESCE(SUM(fail_count), 0) AS n FROM signals "
                "WHERE network = ? AND token_address = ? AND grade = ? AND leg_id = ? "
                "AND status IN ('failed_perm', 'failed_retry')",
                (network, token_address, grade, leg_id),
            ).fetchone()
        )
        return 0 if row is None else int(row[0])

    def put_signal_card(self, signal_id: int, card: dict[str, Any]) -> None:
        self.insert_event(
            "signal.card",
            datetime.now(UTC).isoformat(),
            json.dumps({"signal_id": signal_id, "card": card}, ensure_ascii=True),
        )

    def get_signal_card(self, signal_id: int) -> dict[str, Any] | None:
        rows = self.read(
            lambda c: c.execute(
                "SELECT payload FROM event_log WHERE type = 'signal.card' ORDER BY id DESC"
            ).fetchall()
        )
        for row in rows:
            try:
                data = json.loads(str(row[0]))
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict) and data.get("signal_id") == signal_id:
                card = data.get("card")
                return card if isinstance(card, dict) else None
        return None

    def list_due_sent(self, cutoff_iso: str) -> list[sqlite3.Row]:
        return list(
            self.read(
                lambda c: c.execute(
                    "SELECT s.* FROM signals s "
                    "LEFT JOIN signal_outcomes o ON o.signal_id = s.id "
                    "WHERE s.status = 'sent' AND s.sent_at IS NOT NULL AND s.sent_at <= ? "
                    "AND (o.signal_id IS NULL OR o.failed = 1)",
                    (cutoff_iso,),
                ).fetchall()
            )
        )

    def get_outcome(self, signal_id: int) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM signal_outcomes WHERE signal_id = ?", (signal_id,)
                ).fetchone()
            ),
        )

    def upsert_outcome(
        self,
        signal_id: int,
        *,
        failed: bool,
        expire_price: float | None = None,
        rel_change_pct: float | None = None,
        peak_price: float | None = None,
        drawdown_pct: float | None = None,
        deep_drawdown: bool | None = None,
    ) -> int:
        def _fn(conn: sqlite3.Connection) -> int:
            now = datetime.now(UTC).isoformat()
            existing = conn.execute(
                "SELECT attempts FROM signal_outcomes WHERE signal_id = ?",
                (signal_id,),
            ).fetchone()
            attempts = 1 if existing is None else int(existing[0]) + 1
            conn.execute(
                "INSERT INTO signal_outcomes("
                "signal_id, attempts, expire_price, rel_change_pct, peak_price, "
                "drawdown_pct, deep_drawdown, evaluated_at, failed"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(signal_id) DO UPDATE SET "
                "attempts = excluded.attempts, "
                "expire_price = excluded.expire_price, "
                "rel_change_pct = excluded.rel_change_pct, "
                "peak_price = excluded.peak_price, "
                "drawdown_pct = excluded.drawdown_pct, "
                "deep_drawdown = excluded.deep_drawdown, "
                "evaluated_at = excluded.evaluated_at, "
                "failed = excluded.failed",
                (
                    signal_id,
                    attempts,
                    expire_price,
                    rel_change_pct,
                    peak_price,
                    drawdown_pct,
                    None if deep_drawdown is None else int(deep_drawdown),
                    now,
                    int(failed),
                ),
            )
            return attempts

        return self.submit(_fn)

    def month_cg_calls(self, month: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COALESCE(SUM(calls), 0) FROM credit_usage "
                "WHERE date_utc LIKE ? AND kind IN ('collect', 'ohlcv', 'trades')",
                (f"{month}%",),
            ).fetchone()
        )
        return int(row[0])

    def abandon_expired_pending(self, cutoff_iso: str) -> list[int]:
        def _fn(conn: sqlite3.Connection) -> list[int]:
            rows = conn.execute(
                "SELECT id FROM signals WHERE status = 'pending' AND created_at < ?",
                (cutoff_iso,),
            ).fetchall()
            ids = [int(r[0]) for r in rows]
            if ids:
                conn.executemany(
                    "UPDATE signals SET status = 'abandoned' WHERE id = ?",
                    [(i,) for i in ids],
                )
            return ids

        return self.submit(_fn)

    def cleanup(self, cfg: CleanupConfig) -> None:
        now = cfg.now

        def iso(days: float) -> str:
            return (now - timedelta(days=days)).isoformat()

        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "DELETE FROM signal_outcomes WHERE evaluated_at IS NOT NULL AND evaluated_at < ?",
                (iso(cfg.outcomes_retain_days),),
            )
            conn.execute("DELETE FROM event_log WHERE ts < ?", (iso(cfg.event_log_retain_days),))
            stale = conn.execute(
                "SELECT id FROM legs WHERE ended = 1 AND last_seen_at < ?",
                (iso(cfg.legs_retain_days),),
            ).fetchall()
            leg_ids = [int(r[0]) for r in stale]
            if leg_ids:
                marks = ",".join("?" * len(leg_ids))
                sigs = conn.execute(
                    f"SELECT id FROM signals WHERE leg_id IN ({marks})",
                    leg_ids,
                ).fetchall()
                sig_ids = [int(r[0]) for r in sigs]
                if sig_ids:
                    smarks = ",".join("?" * len(sig_ids))
                    conn.execute(
                        f"DELETE FROM signal_outcomes WHERE signal_id IN ({smarks})",
                        sig_ids,
                    )
                    conn.execute(f"DELETE FROM signals WHERE id IN ({smarks})", sig_ids)
                conn.execute(f"DELETE FROM legs WHERE id IN ({marks})", leg_ids)
            conn.execute(
                "DELETE FROM security_cache WHERE expires_at < ?",
                (now.isoformat(),),
            )
            conn.execute(
                "DELETE FROM step_counts WHERE date_utc < ?",
                ((now - timedelta(days=cfg.outcomes_retain_days)).strftime("%Y-%m-%d"),),
            )

        self.submit(_fn)

    def wal_checkpoint_truncate(self) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        self.submit(_fn)
