"""Radar-task orchestration: no-network judge, then dependent parallel IO."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from memebot.admin import AdminHandler
from memebot.budget import Budget
from memebot.cg_client import CgClient, CgSettings
from memebot.config import AppConfig
from memebot.events import EventBus
from memebot.gates import evaluate
from memebot.goplus_client import (
    GoPlusClient,
    GoPlusSettings,
    SecurityVerdict,
    goplus_ready,
    solana_authority_open,
)
from memebot.grade import compute_metrics, decide, maker_stats, net_buy, parse_ohlcv, parse_trades
from memebot.heartbeat import write_heartbeat
from memebot.legs import ensure_open_leg, expire_inactive, init_high_from_close, touch_leg
from memebot.notify import Notifier
from memebot.pool import PoolSnapshot
from memebot.radar import Radar, allocate_by_share
from memebot.store import Store, cleanup_config_from_raw
from memebot.tracker import Tracker

log = logging.getLogger("memebot.runtime")

SleepFn = Callable[[float], Awaitable[None]]


class SignalSink:
    async def send_grade(self, **payload: Any) -> bool:
        return False

    async def alert(self, message: str, key: str, now: datetime) -> None:
        return None

    def reconcile_startup(self, now: datetime, config_hash: str) -> tuple[list[int], bool]:
        return [], False


@dataclass
class GradeJob:
    pool: PoolSnapshot
    leg_id: int
    verdict: SecurityVerdict
    bars: list[Any] | None = None
    trades_ok: bool = False
    buy_usd: float = 0.0
    sell_usd: float = 0.0
    net: float | None = None


@dataclass
class Runtime:
    cfg: AppConfig
    store: Store
    client: CgClient | None = None
    goplus: GoPlusClient | None = None
    notifier: SignalSink | None = None
    sleep: SleepFn | None = None
    budget: Budget | None = None
    _stop: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    network_calls: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.bus = EventBus(self.store)
        if self.notifier is None:
            self.notifier = cast(
                SignalSink,
                Notifier(
                    self.store,
                    self.cfg.raw,
                    self.cfg.secrets.telegram_bot_token,
                    self.cfg.secrets.telegram_admin_id,
                ),
            )
        if self.sleep is None:
            self.sleep = asyncio.sleep
        self.admin: AdminHandler | None = None
        if isinstance(self.notifier, Notifier):
            self.admin = AdminHandler(
                self.store,
                self.cfg.raw,
                self.notifier,
                self.cfg.secrets.telegram_admin_id,
            )
        if self.budget is None:
            def _budget_alert(msg: str) -> None:
                self.network_calls.append(f"alert:{msg}")
                sink = self.notifier
                if sink is None:
                    return
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    return
                loop.create_task(sink.alert(msg, "budget.cg_cap", datetime.now(UTC)))

            self.budget = Budget(
                self.store,
                cg_daily_call_cap=int(self.cfg.get("budget.cg_daily_call_cap")),
                alert=_budget_alert,
            )
        if self.client is None:
            self.client = CgClient(
                self.cfg.secrets.coingecko_api_key,
                CgSettings.from_app(self.cfg),
                self.budget,
            )
        if self.goplus is None:
            def _record_goplus() -> None:
                self.store.add_credits(datetime.now(UTC).date().isoformat(), "goplus", 1)

            self.goplus = GoPlusClient(
                self.cfg.secrets.goplus_app_key,
                self.cfg.secrets.goplus_app_secret,
                GoPlusSettings.from_app(self.cfg),
                self.store,
                record_credit=_record_goplus,
            )
        self.radar = Radar(self.client, self.store, self.cfg.raw)
        self.tracker = Tracker(self.store, self.cfg.raw, self.client)

    async def aclose(self) -> None:
        for obj in (self.client, self.goplus, self.notifier):
            close = getattr(obj, "aclose", None)
            if close is None:
                continue
            await close()

    def request_stop(self) -> None:
        self._stop.set()

    def _sink(self) -> SignalSink:
        sink = self.notifier
        if sink is None:
            raise RuntimeError("notifier is not configured")
        return sink

    def _sleep_fn(self) -> SleepFn:
        fn = self.sleep
        if fn is None:
            raise RuntimeError("sleep is not configured")
        return fn

    def _day(self, now: datetime) -> str:
        return now.date().isoformat()

    def _write_hb(self, last_ok: str | None = None) -> None:
        write_heartbeat(
            Path(str(self.cfg.get("paths.heartbeat_file"))),
            self.started_at,
            last_ok,
        )
        if last_ok:
            self.store.kv_set("heartbeat_at", last_ok)

    async def start(self) -> None:
        now = datetime.now(UTC)
        sink = self._sink()
        prev_hash = self.store.kv_get("config_hash")
        hash_changed = prev_hash is not None and prev_hash != self.cfg.config_hash
        abandoned, _ = sink.reconcile_startup(now, self.cfg.config_hash)
        if abandoned:
            await sink.alert(
                f"启动对账：{len(abandoned)} 条过期 pending 已标 abandoned，不自动补发",
                "startup.abandoned",
                now,
            )
        if hash_changed:
            self.store.clear_security_cache()
            await sink.alert(
                f"参数变更：配置指纹已更新为 {self.cfg.config_hash}，已清空安全缓存",
                "startup.config_hash",
                now,
            )
        resume = getattr(sink, "resume_pending", None)
        if resume is not None:
            await resume()
        self.store.kv_set("started_at", self.started_at)
        self.store.kv_set("config_hash", self.cfg.config_hash)
        self._write_hb()

    async def run(self) -> None:
        await self.start()
        await asyncio.gather(
            self._radar_loop(),
            self._tracker_loop(),
            self._admin_loop(),
            self._cleanup_loop(),
        )

    async def _radar_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_radar_round(datetime.now(UTC))
            except Exception:
                log.exception("radar round failed")
            await self._sleep_fn()(1.0)

    async def _tracker_loop(self) -> None:
        interval = float(self.cfg.get("tracker.scan_interval_h")) * 3600
        while not self._stop.is_set():
            try:
                await self.tracker.scan(datetime.now(UTC))
            except Exception:
                log.exception("tracker scan failed")
            await self._sleep_fn()(max(1.0, interval))

    async def _admin_loop(self) -> None:
        interval = float(self.cfg.get("telegram.admin_poll_interval_sec"))
        while not self._stop.is_set():
            if self.admin is not None:
                try:
                    await self.admin.poll_once()
                except Exception:
                    log.exception("admin poll failed")
            await self._sleep_fn()(max(1.0, interval))

    async def _cleanup_loop(self) -> None:
        interval = float(self.cfg.get("storage.cleanup_interval_h")) * 3600
        while not self._stop.is_set():
            try:
                self.store.cleanup(cleanup_config_from_raw(self.cfg.raw))
            except Exception:
                log.exception("cleanup failed")
            await self._sleep_fn()(max(1.0, interval))

    async def run_radar_round(self, now: datetime) -> None:
        assert self.client is not None
        collected = await self.radar.collect(now)
        try:
            await self._process_rows(collected.rows, now)
        finally:
            expire_inactive(self.store, now, float(self.cfg.get("legs.max_inactive_h")))
        if collected.any_success:
            iso = now.isoformat()
            self._write_hb(iso)
            server = collected.last_server_date or self.client.last_server_date
            warn = float(self.cfg.get("runtime.health.clock_skew_warn_sec"))
            if server is not None and abs((server - now).total_seconds()) > warn:
                await self._sink().alert(
                    f"clock skew: server {server.isoformat()} local {iso}",
                    "core.clock_skew",
                    now,
                )

    def _judge_row(
        self,
        pool: PoolSnapshot,
        now: datetime,
        need_security: list[PoolSnapshot],
        ready: list[tuple[PoolSnapshot, SecurityVerdict]],
    ) -> None:
        assert self.goplus is not None
        seen = now.isoformat()
        end_dd = float(self.cfg.get("legs.end_drawdown_pct"))
        open_leg = self.store.get_open_leg(pool.network, pool.token_address)
        if open_leg is not None:
            touch_leg(
                self.store,
                int(open_leg["id"]),
                spot=pool.price_usd,
                last_seen_at=seen,
                end_drawdown_pct=end_dd,
            )
            open_leg = self.store.get_open_leg(pool.network, pool.token_address)
        if open_leg is not None:
            live = self.store.pending_or_sent_grades(
                pool.network, pool.token_address, int(open_leg["id"])
            )
            if "strong" in live:
                return
        gate = evaluate(pool, self.cfg.raw, now=now)
        if not gate.passed:
            if gate.step:
                self.store.incr_step(self._day(now), gate.step)
            return
        if self._reopen_cooling(pool, now):
            self.store.incr_step(self._day(now), "leg_cooldown")
            return
        hit = self.goplus.cached(pool.network, pool.token_address)
        if hit is None:
            need_security.append(pool)
            return
        ready_ok = goplus_ready(pool.network, hit)
        if ready_ok:
            ready.append((pool, hit))
        elif hit.status == "pass":
            need_security.append(pool)
        elif hit.status == "reject":
            self.store.incr_step(self._day(now), "security_reject")
        else:
            self.store.incr_step(self._day(now), "security_transient")

    def _reopen_cooling(self, pool: PoolSnapshot, now: datetime) -> bool:
        if self.store.get_open_leg(pool.network, pool.token_address) is not None:
            return False
        cooldown = float(self.cfg.get("legs.reopen_cooldown_min") or 0)
        if cooldown <= 0:
            return False
        row = self.store.get_last_ended_leg(pool.network, pool.token_address)
        if row is None:
            return False
        try:
            seen = datetime.fromisoformat(str(row["last_seen_at"]))
        except ValueError:
            return False
        if seen.tzinfo is None:
            seen = seen.replace(tzinfo=UTC)
        return (now - seen).total_seconds() < cooldown * 60.0

    def _admit(
        self, pool: PoolSnapshot, verdict: SecurityVerdict, now: datetime
    ) -> GradeJob | None:
        if not goplus_ready(pool.network, verdict):
            return None
        open_leg = self.store.get_open_leg(pool.network, pool.token_address)
        if open_leg is not None:
            live = self.store.pending_or_sent_grades(
                pool.network, pool.token_address, int(open_leg["id"])
            )
            if "strong" in live:
                return None
        if open_leg is None:
            leg_id = ensure_open_leg(
                self.store,
                pool.network,
                pool.token_address,
                spot=pool.price_usd,
                last_seen_at=now.isoformat(),
            )
        else:
            leg_id = int(open_leg["id"])
        self.store.incr_step(self._day(now), "grade_input")
        return GradeJob(pool=pool, leg_id=leg_id, verdict=verdict)

    async def _fetch_security(
        self, pools: list[PoolSnapshot]
    ) -> list[tuple[PoolSnapshot, SecurityVerdict]]:
        gp = self.goplus
        if gp is None:
            raise RuntimeError("goplus is not configured")
        by_net: dict[str, list[PoolSnapshot]] = {}
        for pool in pools:
            by_net.setdefault(pool.network, []).append(pool)
        out: list[tuple[PoolSnapshot, SecurityVerdict]] = []


        async def _one(network: str, group: list[PoolSnapshot]) -> None:
            addrs = [p.token_address for p in group]
            self.network_calls.append(f"goplus:{network}:{len(addrs)}")
            results = await gp.check_many(
                network,
                addrs,
                pool_by_token={p.token_address: p.address for p in group},
            )
            for pool in group:
                out.append((pool, results[pool.token_address]))

        await asyncio.gather(*[_one(net, group) for net, group in by_net.items()])
        return out

    async def _fetch_ohlcv(self, pool: PoolSnapshot) -> list[Any] | None:
        assert self.client is not None
        limit = int(self.cfg.get("grade.ohlcv_limit"))
        self.network_calls.append(f"ohlcv-pool:{pool.address}")
        try:
            payload = await self.client.pool_ohlcv(
                pool.network,
                pool.address,
                "minute",
                aggregate=1,
                include_empty_intervals=True,
                limit=limit,
            )
            bars = parse_ohlcv(payload)
            if bars:
                return bars
        except Exception as exc:
            log.warning("pool ohlcv failed %s: %s", pool.address, exc)
        self.network_calls.append(f"ohlcv-token:{pool.token_address}")
        try:
            payload = await self.client.token_ohlcv(
                pool.network,
                pool.token_address,
                "minute",
                aggregate=1,
                include_empty_intervals=True,
                limit=limit,
            )
            bars = parse_ohlcv(payload)
            if bars:
                return bars
        except Exception as exc:
            log.warning("token ohlcv failed %s: %s", pool.token_address, exc)
        return None

    async def _fetch_trades(self, pool: PoolSnapshot) -> Any:
        assert self.client is not None
        self.network_calls.append(f"trades:{pool.address}")
        return await self.client.trades(
            pool.network,
            pool.address,
            trade_volume_in_usd_greater_than=float(self.cfg.get("grade.min_trade_usd")),
        )

    async def _process_rows(self, rows: list[PoolSnapshot], now: datetime) -> None:
        need_security: list[PoolSnapshot] = []
        ready: list[tuple[PoolSnapshot, SecurityVerdict]] = []
        for pool in rows:
            self._judge_row(pool, now, need_security, ready)

        candidates = list(need_security) + [pool for pool, _verdict in ready]
        share = self.cfg.raw["radar"]["chain_share"]
        max_n = int(self.cfg.get("radar.max_detect_per_round"))
        chosen = allocate_by_share(candidates, share, max_n)
        chosen_ids = {id(pool) for pool in chosen}
        skipped = len(candidates) - len(chosen)
        if skipped:
            self.store.incr_step(self._day(now), "detect_quota", skipped)
        need_security = [pool for pool in need_security if id(pool) in chosen_ids]
        ready = [(pool, verdict) for pool, verdict in ready if id(pool) in chosen_ids]

        jobs: list[GradeJob] = []
        for pool, verdict in ready:
            job = self._admit(pool, verdict, now)
            if job is not None:
                jobs.append(job)

        if need_security:
            fetched = await self._fetch_security(need_security)
            for pool, verdict in fetched:
                if verdict.status == "transient":
                    self.store.incr_step(self._day(now), "security_transient")
                    continue
                if not goplus_ready(pool.network, verdict):
                    self.store.incr_step(self._day(now), "security_reject")
                    continue
                job = self._admit(pool, verdict, now)
                if job is not None:
                    jobs.append(job)

        if not jobs:
            return

        ohlcv_results = await asyncio.gather(*[self._fetch_ohlcv(job.pool) for job in jobs])
        need_trades: list[int] = []
        for i, (job, bars) in enumerate(zip(jobs, ohlcv_results, strict=True)):
            if not bars:
                self.store.incr_step(self._day(now), "ohlcv_fail")
                continue
            job.bars = bars
            init_high_from_close(self.store, job.leg_id, bars[-1].c)
            if self.cfg.get("grade.require_net_buy"):
                need_trades.append(i)
            else:
                job.trades_ok = True
                job.net = 1.0

        if need_trades:
            trade_payloads = await asyncio.gather(
                *[self._fetch_trades(jobs[i].pool) for i in need_trades],
                return_exceptions=True,
            )
            for idx, payload in zip(need_trades, trade_payloads, strict=True):
                job = jobs[idx]
                if isinstance(payload, BaseException):
                    self.store.incr_step(self._day(now), "trades_fail")
                    continue
                parsed = parse_trades(payload if isinstance(payload, dict) else None)
                if parsed is None:
                    self.store.incr_step(self._day(now), "trades_fail")
                    continue
                buy, sell, net = net_buy(
                    parsed,
                    int(now.timestamp()),
                    float(self.cfg.get("grade.trade_lookback_sec")),
                    float(self.cfg.get("grade.min_trade_usd")),
                )
                unique, per = maker_stats(
                    parsed,
                    int(now.timestamp()),
                    float(self.cfg.get("grade.trade_lookback_sec")),
                )
                min_makers = float(self.cfg.get("grade.min_window_makers") or 0)
                max_per = float(self.cfg.get("grade.max_window_trades_per_maker") or 0)
                if min_makers > 0 and unique < min_makers:
                    self.store.incr_step(self._day(now), "trade_wash")
                    continue
                if max_per > 0 and unique > 0 and per > max_per:
                    self.store.incr_step(self._day(now), "trade_wash")
                    continue
                max_bs = float(self.cfg.get("grade.max_window_buy_sell_ratio") or 0)
                if max_bs > 1:
                    if sell <= 0:
                        if buy > 0:
                            self.store.incr_step(self._day(now), "trade_imbalance")
                            continue
                    elif buy / sell > max_bs:
                        self.store.incr_step(self._day(now), "trade_imbalance")
                        continue
                job.buy_usd, job.sell_usd, job.net = buy, sell, net
                job.trades_ok = True

        for job in jobs:
            if job.bars is None or not job.trades_ok:
                continue
            await self._grade_and_push(job, now)

    async def _grade_and_push(self, job: GradeJob, now: datetime) -> None:
        pool = job.pool
        if not goplus_ready(pool.network, job.verdict):
            self.store.incr_step(self._day(now), "security_reject")
            return
        metrics = compute_metrics(
            job.bars or [],
            int(now.timestamp()),
            int(self.cfg.get("grade.near_high_bars")),
        )
        if metrics is None:
            self.store.incr_step(self._day(now), "ohlcv_fail")
            return
        h1 = (pool.price_change_usd or {}).get("h1")
        m5 = (pool.price_change_usd or {}).get("m5")
        caps = self.cfg.get("grade.max_h1_pct")
        max_h1 = None
        if isinstance(caps, dict) and pool.network in caps:
            max_h1 = float(caps[pool.network])
        weak_live = "weak" in self.store.pending_or_sent_grades(
            pool.network, pool.token_address, job.leg_id
        )
        decision = decide(
            chg_1m=metrics.chg_1m,
            dist=metrics.dist,
            h1=h1,
            m5=m5,
            net=job.net,
            require_net_buy=bool(self.cfg.get("grade.require_net_buy")),
            authority_open=solana_authority_open(
                job.verdict.mintable, job.verdict.freezable, pool.network
            ),
            weak_live=weak_live,
            strong_min_1m_pct=float(self.cfg.get("grade.strong_min_1m_pct")),
            weak_min_1m_pct=float(self.cfg.get("grade.weak_min_1m_pct")),
            strong_max_dist_pct=float(self.cfg.get("grade.strong_max_dist_pct")),
            weak_max_dist_pct=float(self.cfg.get("grade.weak_max_dist_pct")),
            min_to_h1=float(self.cfg.get("grade.strong_min_1m_to_h1")),
            min_to_m5=float(self.cfg.get("grade.strong_min_1m_to_m5")),
            max_h1_pct=max_h1,
        )
        if decision.grade is None:
            self.store.incr_step(self._day(now), decision.reason)
            return
        if self.store.has_live_signal(pool.network, pool.token_address, decision.grade, job.leg_id):
            return
        max_fail = int(self.cfg.get("telegram.max_send_failures"))
        if self.store.count_send_failures(
            pool.network, pool.token_address, decision.grade, job.leg_id
        ) >= max_fail:
            return
        signal_id = self.store.insert_pending_signal(
            network=pool.network,
            token_address=pool.token_address,
            pool_address=pool.address,
            grade=decision.grade,
            leg_id=job.leg_id,
            price_at_signal=metrics.c_now,
            fdv_usd=pool.fdv_usd,
        )
        sent = await self._sink().send_grade(
            signal_id=signal_id,
            grade=decision.grade,
            pool=pool,
            metrics=metrics,
            job=job,
            now=now,
        )
        if sent:
            step = "pushed_strong" if decision.grade == "strong" else "pushed_weak"
            self.store.incr_step(self._day(now), step)
