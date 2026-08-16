"""SQLite persistence: schema, forward migrations, single writer thread."""

from __future__ import annotations

import asyncio
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
    "pools",
    "signals",
    "watch_log",
    "signal_outcomes",
    "security_cache",
    "token_batch_cache",
    "symbol_counter",
    "credit_usage",
    "event_log",
    "funnel_counts",
    "runtime_kv",
)
FUNNEL_LAYERS = frozenset({"stream", "l0", "l1", "l2a", "l2b", "scoring", "watch"})
CREDIT_KINDS = frozenset({"collect", "security", "watch", "track"})

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pools (
  network TEXT NOT NULL,
  pool_id TEXT NOT NULL,
  address TEXT NOT NULL,
  token_address TEXT NOT NULL,
  symbol TEXT,
  name TEXT,
  pool_created_at TEXT,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  last_price_usd REAL,
  last_fdv REAL,
  last_reserve REAL,
  last_m5 REAL,
  last_m15 REAL,
  last_h1 REAL,
  last_volume_m15 REAL,
  PRIMARY KEY (network, pool_id)
);
CREATE INDEX IF NOT EXISTS idx_pools_last_seen_at ON pools(last_seen_at);
CREATE UNIQUE INDEX IF NOT EXISTS ux_pools_network_address ON pools(network, address);

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  network TEXT NOT NULL,
  token_address TEXT NOT NULL,
  pool_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  score REAL,
  created_at TEXT NOT NULL,
  price_at_signal REAL,
  fdv_at_signal REAL,
  telegram_message_id TEXT,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  features_json TEXT,
  config_hash TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_signals_live
  ON signals(network, token_address, kind)
  WHERE status IN ('pending', 'sent');
CREATE INDEX IF NOT EXISTS idx_signals_status_created ON signals(status, created_at);

CREATE TABLE IF NOT EXISTS watch_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pool_id TEXT NOT NULL,
  network TEXT NOT NULL,
  token_address TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  outcome TEXT,
  baseline_price REAL,
  stats_json TEXT,
  features_json TEXT,
  config_hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_watch_log_started_at ON watch_log(started_at);
CREATE INDEX IF NOT EXISTS idx_watch_log_network_token ON watch_log(network, token_address);

CREATE TABLE IF NOT EXISTS signal_outcomes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  source_id INTEGER NOT NULL,
  network TEXT NOT NULL,
  token_address TEXT NOT NULL,
  evaluated_at TEXT NOT NULL,
  baseline_price REAL,
  max_gain_pct REAL,
  max_drawdown_pct REAL,
  t_to_peak_min REAL,
  price_1h REAL,
  price_24h REAL,
  is_rug INTEGER,
  ohlcv_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_signal_outcomes_evaluated_at ON signal_outcomes(evaluated_at);
CREATE INDEX IF NOT EXISTS idx_signal_outcomes_network_token
  ON signal_outcomes(network, token_address);

CREATE TABLE IF NOT EXISTS security_cache (
  network TEXT NOT NULL,
  token_address TEXT NOT NULL,
  result_json TEXT,
  passed INTEGER NOT NULL,
  checked_at TEXT NOT NULL,
  PRIMARY KEY (network, token_address)
);
CREATE INDEX IF NOT EXISTS idx_security_cache_checked_at ON security_cache(checked_at);

CREATE TABLE IF NOT EXISTS token_batch_cache (
  network TEXT NOT NULL,
  token_address TEXT NOT NULL,
  graduated INTEGER NOT NULL,
  graduation_pct REAL,
  total_reserve_usd REAL,
  main_pool_share REAL,
  checked_at TEXT NOT NULL,
  PRIMARY KEY (network, token_address)
);
CREATE INDEX IF NOT EXISTS idx_token_batch_cache_checked_at ON token_batch_cache(checked_at);

CREATE TABLE IF NOT EXISTS symbol_counter (
  network TEXT NOT NULL,
  symbol_norm TEXT NOT NULL,
  hour_bucket TEXT NOT NULL,
  cnt INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (network, symbol_norm, hour_bucket)
);
CREATE INDEX IF NOT EXISTS idx_symbol_counter_hour_bucket ON symbol_counter(hour_bucket);

CREATE TABLE IF NOT EXISTS credit_usage (
  date_utc TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('collect', 'security', 'watch', 'track')),
  calls INTEGER NOT NULL DEFAULT 0,
  credits INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (date_utc, kind)
);

CREATE TABLE IF NOT EXISTS event_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  ts TEXT NOT NULL,
  payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log(ts);

CREATE TABLE IF NOT EXISTS funnel_counts (
  date_utc TEXT NOT NULL,
  layer TEXT NOT NULL,
  rule TEXT NOT NULL,
  n INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (date_utc, layer, rule)
);

CREATE TABLE IF NOT EXISTS runtime_kv (
  key TEXT PRIMARY KEY,
  value TEXT
);
"""

T = TypeVar("T")


@dataclass(frozen=True)
class CleanupConfig:
    pools_retain_h: float
    watch_log_retain_days: float
    outcomes_raw_retain_days: float
    credit_usage_retain_days: float
    event_log_retain_days: float
    security_cache_hours: float
    ungraduated_recheck_min: float | None
    copycat_lookback_h: float
    now: datetime


def cleanup_config_from_raw(raw: dict[str, Any], now: datetime | None = None) -> CleanupConfig:
    storage = raw["storage"]
    security = raw["security"]
    copycat = raw["collection_gates"]["copycat"]
    ungrad = security["batch"]["cache"]["ungraduated_recheck_min"]
    return CleanupConfig(
        pools_retain_h=float(storage["pools_retain_h"]),
        watch_log_retain_days=float(storage["watch_log_retain_days"]),
        outcomes_raw_retain_days=float(storage["outcomes_raw_retain_days"]),
        credit_usage_retain_days=float(storage["credit_usage_retain_days"]),
        event_log_retain_days=float(storage["event_log_retain_days"]),
        security_cache_hours=float(security["cache_hours"]),
        ungraduated_recheck_min=None if ungrad is None else float(ungrad),
        copycat_lookback_h=float(copycat["lookback_h"]),
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
        dest = self.backup_dir / f"memebot-v{from_version}-{stamp}.db"
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
                for version in range(current + 1, SCHEMA_VERSION + 1):
                    if version == 1:
                        conn.executescript(_SCHEMA_SQL)
                    else:
                        raise RuntimeError(f"missing migration for user_version {version}")
                    conn.execute(f"PRAGMA user_version = {version}")
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
        instance_id = uuid.uuid4().hex
        self.kv_set("instance_id", instance_id)
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
                "INSERT INTO runtime_kv(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

        self.submit(_fn)

    def kv_get(self, key: str) -> str | None:
        row = self.read(
            lambda c: c.execute("SELECT value FROM runtime_kv WHERE key = ?", (key,)).fetchone()
        )
        if row is None:
            return None
        return cast(str | None, row[0])

    def incr_funnel(self, date_utc: str, layer: str, rule: str, n: int = 1) -> int:
        if layer not in FUNNEL_LAYERS:
            raise ValueError(f"invalid funnel layer: {layer}")

        def _fn(conn: sqlite3.Connection) -> int:
            conn.execute(
                "INSERT INTO funnel_counts(date_utc, layer, rule, n) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(date_utc, layer, rule) DO UPDATE SET n = n + excluded.n",
                (date_utc, layer, rule, n),
            )
            row = conn.execute(
                "SELECT n FROM funnel_counts WHERE date_utc = ? AND layer = ? AND rule = ?",
                (date_utc, layer, rule),
            ).fetchone()
            return int(row[0])

        return self.submit(_fn)

    def get_funnel(self, date_utc: str, layer: str, rule: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT n FROM funnel_counts WHERE date_utc = ? AND layer = ? AND rule = ?",
                (date_utc, layer, rule),
            ).fetchone()
        )
        return 0 if row is None else int(row[0])

    def add_credits(self, date_utc: str, kind: str, calls: int = 1, credits: int = 1) -> int:
        if kind not in CREDIT_KINDS:
            raise ValueError(f"invalid credit kind: {kind}")

        def _fn(conn: sqlite3.Connection) -> int:
            conn.execute(
                "INSERT INTO credit_usage(date_utc, kind, calls, credits) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(date_utc, kind) DO UPDATE SET "
                "calls = calls + excluded.calls, credits = credits + excluded.credits",
                (date_utc, kind, calls, credits),
            )
            row = conn.execute(
                "SELECT calls FROM credit_usage WHERE date_utc = ? AND kind = ?",
                (date_utc, kind),
            ).fetchone()
            return int(row[0])

        return self.submit(_fn)

    def daily_calls(self, date_utc: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COALESCE(SUM(calls), 0) AS n FROM credit_usage WHERE date_utc = ?",
                (date_utc,),
            ).fetchone()
        )
        return int(row["n"])

    def month_credits(self, month_prefix: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COALESCE(SUM(credits), 0) AS n FROM credit_usage WHERE date_utc LIKE ?",
                (f"{month_prefix}%",),
            ).fetchone()
        )
        return int(row["n"])

    def put_security(
        self,
        network: str,
        token_address: str,
        result_json: str,
        passed: bool,
        checked_at: str,
    ) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT INTO security_cache("
                "network, token_address, result_json, passed, checked_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(network, token_address) DO UPDATE SET "
                "result_json = excluded.result_json, passed = excluded.passed, "
                "checked_at = excluded.checked_at",
                (network, token_address, result_json, int(passed), checked_at),
            )

        self.submit(_fn)

    def get_security(self, network: str, token_address: str) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM security_cache WHERE network = ? AND token_address = ?",
                    (network, token_address),
                ).fetchone()
            ),
        )

    def put_token_batch(
        self,
        network: str,
        token_address: str,
        graduated: bool | None,
        checked_at: str,
        graduation_pct: float | None = None,
        total_reserve_usd: float | None = None,
        main_pool_share: float | None = None,
    ) -> None:
        flag = -1 if graduated is None else int(graduated)

        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT INTO token_batch_cache("
                "network, token_address, graduated, graduation_pct, "
                "total_reserve_usd, main_pool_share, checked_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(network, token_address) DO UPDATE SET "
                "graduated = excluded.graduated, graduation_pct = excluded.graduation_pct, "
                "total_reserve_usd = excluded.total_reserve_usd, "
                "main_pool_share = excluded.main_pool_share, checked_at = excluded.checked_at",
                (
                    network,
                    token_address,
                    flag,
                    graduation_pct,
                    total_reserve_usd,
                    main_pool_share,
                    checked_at,
                ),
            )

        self.submit(_fn)

    def insert_signal(
        self,
        network: str,
        token_address: str,
        pool_id: str,
        kind: str,
        status: str,
        created_at: str,
        config_hash: str,
        score: float | None = None,
        features_json: str | None = None,
    ) -> int:
        def _fn(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "INSERT INTO signals("
                "network, token_address, pool_id, kind, score, created_at, "
                "status, features_json, config_hash) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    network,
                    token_address,
                    pool_id,
                    kind,
                    score,
                    created_at,
                    status,
                    features_json,
                    config_hash,
                ),
            )
            return int(cur.lastrowid or 0)

        return self.submit(_fn)

    def insert_watch_log(
        self,
        pool_id: str,
        network: str,
        token_address: str,
        started_at: str,
        config_hash: str,
        ended_at: str | None = None,
        outcome: str | None = None,
        baseline_price: float | None = None,
        stats_json: str | None = None,
        features_json: str | None = None,
    ) -> int:
        def _fn(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "INSERT INTO watch_log(pool_id, network, token_address, started_at, ended_at, "
                "outcome, baseline_price, stats_json, features_json, config_hash) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    pool_id,
                    network,
                    token_address,
                    started_at,
                    ended_at,
                    outcome,
                    baseline_price,
                    stats_json,
                    features_json,
                    config_hash,
                ),
            )
            return int(cur.lastrowid or 0)

        return self.submit(_fn)

    def finish_watch(
        self,
        watch_id: int,
        ended_at: str,
        outcome: str,
        stats_json: str | None = None,
    ) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE watch_log SET ended_at = ?, outcome = ?, stats_json = ? WHERE id = ?",
                (ended_at, outcome, stats_json, watch_id),
            )

        self.submit(_fn)

    def hanging_watches(self) -> list[sqlite3.Row]:
        return list(
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM watch_log WHERE ended_at IS NULL ORDER BY id"
                ).fetchall()
            )
        )

    def watch_history(self, network: str, token_address: str) -> list[sqlite3.Row]:
        return list(
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM watch_log WHERE network = ? AND token_address = ? "
                    "AND ended_at IS NOT NULL ORDER BY ended_at DESC, id DESC",
                    (network, token_address),
                ).fetchall()
            )
        )

    def get_token_batch(self, network: str, token_address: str) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM token_batch_cache WHERE network = ? AND token_address = ?",
                    (network, token_address),
                ).fetchone()
            ),
        )

    def symbol_count(self, network: str, symbol_norm: str, min_bucket: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COALESCE(SUM(cnt), 0) AS n FROM symbol_counter "
                "WHERE network = ? AND symbol_norm = ? AND hour_bucket >= ?",
                (network, symbol_norm, min_bucket),
            ).fetchone()
        )
        return int(row["n"])

    def kind_calls(self, date_utc: str, kind: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COALESCE(SUM(calls), 0) AS n FROM credit_usage "
                "WHERE date_utc = ? AND kind = ?",
                (date_utc, kind),
            ).fetchone()
        )
        return int(row["n"])

    def insert_signal_with_event(
        self,
        *,
        network: str,
        token_address: str,
        pool_id: str,
        kind: str,
        status: str,
        created_at: str,
        config_hash: str,
        event_type: str,
        payload: str,
        score: float | None = None,
        features_json: str | None = None,
        price_at_signal: float | None = None,
        fdv_at_signal: float | None = None,
    ) -> int:
        def _fn(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "INSERT INTO signals("
                "network, token_address, pool_id, kind, score, created_at, "
                "price_at_signal, fdv_at_signal, status, features_json, config_hash) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    network,
                    token_address,
                    pool_id,
                    kind,
                    score,
                    created_at,
                    price_at_signal,
                    fdv_at_signal,
                    status,
                    features_json,
                    config_hash,
                ),
            )
            sid = int(cur.lastrowid or 0)
            conn.execute(
                "INSERT INTO event_log(type, ts, payload) VALUES (?, ?, ?)",
                (event_type, created_at, payload),
            )
            return sid

        return self.submit(_fn)

    def update_signal_status(
        self,
        signal_id: int,
        status: str,
        telegram_message_id: str | None = None,
        attempts: int | None = None,
    ) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            if attempts is None:
                conn.execute(
                    "UPDATE signals SET status = ?, "
                    "telegram_message_id = COALESCE(?, telegram_message_id) "
                    "WHERE id = ?",
                    (status, telegram_message_id, signal_id),
                )
            else:
                conn.execute(
                    "UPDATE signals SET status = ?, "
                    "telegram_message_id = COALESCE(?, telegram_message_id), "
                    "attempts = ? WHERE id = ?",
                    (status, telegram_message_id, attempts, signal_id),
                )

        self.submit(_fn)

    def count_send_failures(self, network: str, token_address: str, kind: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COUNT(*) AS n FROM signals WHERE network = ? AND token_address = ? "
                "AND kind = ? AND status IN ('failed_perm', 'failed_retry')",
                (network, token_address, kind),
            ).fetchone()
        )
        return int(row["n"])

    def has_live_signal(self, network: str, token_address: str, kind: str) -> bool:
        row = self.read(
            lambda c: c.execute(
                "SELECT 1 FROM signals WHERE network = ? AND token_address = ? AND kind = ? "
                "AND status IN ('pending', 'sent') LIMIT 1",
                (network, token_address, kind),
            ).fetchone()
        )
        return row is not None

    def get_signal(self, signal_id: int) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            self.read(
                lambda c: c.execute("SELECT * FROM signals WHERE id = ?", (signal_id,)).fetchone()
            ),
        )

    def list_signals(self, status: str | None = None) -> list[sqlite3.Row]:
        if status is None:
            return list(
                self.read(lambda c: c.execute("SELECT * FROM signals ORDER BY id").fetchall())
            )
        return list(
            self.read(
                lambda c: c.execute(
                    "SELECT * FROM signals WHERE status = ? ORDER BY id", (status,)
                ).fetchall()
            )
        )

    def abandon_expired_pending(self, cutoff_iso: str) -> list[int]:
        def _fn(conn: sqlite3.Connection) -> list[int]:
            rows = conn.execute(
                "SELECT id FROM signals WHERE status = 'pending' AND created_at < ?",
                (cutoff_iso,),
            ).fetchall()
            ids = [int(r[0]) for r in rows]
            if ids:
                conn.execute(
                    "UPDATE signals SET status = 'abandoned' "
                    "WHERE status = 'pending' AND created_at < ?",
                    (cutoff_iso,),
                )
            return ids

        return self.submit(_fn)

    def funnel_day(self, date_utc: str) -> list[sqlite3.Row]:
        return list(
            self.read(
                lambda c: c.execute(
                    "SELECT layer, rule, n FROM funnel_counts "
                    "WHERE date_utc = ? ORDER BY layer, rule",
                    (date_utc,),
                ).fetchall()
            )
        )

    def sent_count_on(self, date_utc: str) -> int:
        row = self.read(
            lambda c: c.execute(
                "SELECT COUNT(*) AS n FROM signals WHERE status = 'sent' AND created_at LIKE ?",
                (f"{date_utc}%",),
            ).fetchone()
        )
        return int(row["n"])

    def update_outcome_metrics(
        self,
        outcome_id: int,
        *,
        baseline_price: float | None,
        max_gain_pct: float | None,
        max_drawdown_pct: float | None,
        t_to_peak_min: float | None,
        price_1h: float | None,
        price_24h: float | None,
        is_rug: bool,
        ohlcv_json: str | None,
    ) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE signal_outcomes SET baseline_price=?, max_gain_pct=?, max_drawdown_pct=?, "
                "t_to_peak_min=?, price_1h=?, price_24h=?, is_rug=?, ohlcv_json=? WHERE id=?",
                (
                    baseline_price,
                    max_gain_pct,
                    max_drawdown_pct,
                    t_to_peak_min,
                    price_1h,
                    price_24h,
                    int(is_rug),
                    ohlcv_json,
                    outcome_id,
                ),
            )

        self.submit(_fn)

    def insert_event(self, event_type: str, ts: str, payload: str | None = None) -> int:
        def _fn(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "INSERT INTO event_log(type, ts, payload) VALUES (?, ?, ?)",
                (event_type, ts, payload),
            )
            return int(cur.lastrowid or 0)

        return self.submit(_fn)

    def insert_outcome(
        self,
        source: str,
        source_id: int,
        network: str,
        token_address: str,
        evaluated_at: str,
        ohlcv_json: str | None = None,
    ) -> int:
        def _fn(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "INSERT INTO signal_outcomes(source, source_id, network, token_address, "
                "evaluated_at, ohlcv_json) VALUES (?, ?, ?, ?, ?, ?)",
                (source, source_id, network, token_address, evaluated_at, ohlcv_json),
            )
            return int(cur.lastrowid or 0)

        return self.submit(_fn)

    def upsert_pool(
        self,
        network: str,
        pool_id: str,
        address: str,
        token_address: str,
        first_seen_at: str,
        last_seen_at: str,
        symbol: str | None = None,
    ) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT INTO pools(network, pool_id, address, token_address, symbol, "
                "first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(network, pool_id) DO UPDATE SET "
                "last_seen_at = excluded.last_seen_at, symbol = excluded.symbol",
                (network, pool_id, address, token_address, symbol, first_seen_at, last_seen_at),
            )

        self.submit(_fn)

    def incr_symbol(self, network: str, symbol_norm: str, hour_bucket: str, n: int = 1) -> int:
        def _fn(conn: sqlite3.Connection) -> int:
            conn.execute(
                "INSERT INTO symbol_counter(network, symbol_norm, hour_bucket, cnt) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(network, symbol_norm, hour_bucket) "
                "DO UPDATE SET cnt = cnt + excluded.cnt",
                (network, symbol_norm, hour_bucket, n),
            )
            row = conn.execute(
                "SELECT cnt FROM symbol_counter "
                "WHERE network = ? AND symbol_norm = ? AND hour_bucket = ?",
                (network, symbol_norm, hour_bucket),
            ).fetchone()
            return int(row[0])

        return self.submit(_fn)

    def wal_checkpoint_truncate(self) -> None:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        self.submit(_fn)

    def cleanup(self, cfg: CleanupConfig) -> None:
        now = cfg.now
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)

        def iso(dt: datetime) -> str:
            return dt.astimezone(UTC).isoformat()

        pools_cut = iso(now - timedelta(hours=cfg.pools_retain_h))
        watch_cut = iso(now - timedelta(days=cfg.watch_log_retain_days))
        outcomes_cut = iso(now - timedelta(days=cfg.outcomes_raw_retain_days))
        credit_cut = (now - timedelta(days=cfg.credit_usage_retain_days)).date().isoformat()
        event_cut = iso(now - timedelta(days=cfg.event_log_retain_days))
        security_cut = iso(now - timedelta(hours=cfg.security_cache_hours))
        lookback_cut = (now - timedelta(hours=cfg.copycat_lookback_h)).strftime("%Y-%m-%dT%H")
        ungrad_cut = (
            None
            if cfg.ungraduated_recheck_min is None
            else iso(now - timedelta(minutes=cfg.ungraduated_recheck_min))
        )

        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute("DELETE FROM pools WHERE last_seen_at < ?", (pools_cut,))
            conn.execute(
                "DELETE FROM watch_log WHERE COALESCE(ended_at, started_at) < ?",
                (watch_cut,),
            )
            conn.execute(
                "UPDATE signal_outcomes SET ohlcv_json = NULL "
                "WHERE evaluated_at < ? AND ohlcv_json IS NOT NULL",
                (outcomes_cut,),
            )
            conn.execute("DELETE FROM credit_usage WHERE date_utc < ?", (credit_cut,))
            conn.execute("DELETE FROM funnel_counts WHERE date_utc < ?", (credit_cut,))
            conn.execute("DELETE FROM event_log WHERE ts < ?", (event_cut,))
            conn.execute("DELETE FROM security_cache WHERE checked_at < ?", (security_cut,))
            if ungrad_cut is not None:
                conn.execute(
                    "DELETE FROM token_batch_cache WHERE graduated != 1 AND checked_at < ?",
                    (ungrad_cut,),
                )
            conn.execute("DELETE FROM symbol_counter WHERE hour_bucket < ?", (lookback_cut,))

        self.submit(_fn)

    def maintenance_once(self, cfg: CleanupConfig) -> None:
        self.cleanup(cfg)
        self.wal_checkpoint_truncate()
