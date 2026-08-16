"""Long-running orchestration: streams, watch, track, maintenance, heartbeat."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from memebot.budget import Budget
from memebot.cg_client import CgClient, CgSettings
from memebot.config import AppConfig
from memebot.events import EventBus
from memebot.filters import Funnel
from memebot.heartbeat import write_heartbeat
from memebot.notify import Notifier, payload_from_signal_row
from memebot.pipeline import collect_stream, process_batches, recent_ttl_sec
from memebot.store import Store, cleanup_config_from_raw
from memebot.tracker import (
    build_daily_report,
    compute_metrics,
    ohlcv_rows,
    report_date_utc,
    report_stats,
)
from memebot.watch import ConfirmStats, Watcher, evaluate_trades, parse_trades

log = logging.getLogger("memebot.runtime")


class Runtime:
    def __init__(self, cfg: AppConfig, store: Store) -> None:
        self.cfg = cfg
        self.store = store
        self.bus = EventBus(store)
        self.notifier = Notifier(store, cfg.raw, cfg.secrets.telegram_bot_token)
        self._budget_alerts: list[str] = []
        self.budget = Budget(
            store,
            global_daily_call_cap=int(cfg.get("budget.global_daily_call_cap")),
            monthly_credit_warn_pct=float(cfg.get("budget.monthly_credit_warn_pct")),
            alert=self._budget_alerts.append,
        )
        self.client = CgClient(cfg.secrets.coingecko_api_key, CgSettings.from_app(cfg), self.budget)
        self.watcher = Watcher(store, cfg.raw, cfg.config_hash)
        self.started_at = datetime.now(UTC).isoformat()
        self._stop = asyncio.Event()
        self._cycle_lock = asyncio.Lock()
        self._recent_pool_ids: dict[str, datetime] = {}
        self._stream_fails: dict[str, int] = {}

    async def start(self) -> None:
        now = datetime.now(UTC)
        for row in self.store.hanging_watches():
            self.store.finish_watch(int(row["id"]), now.isoformat(), "aborted_shutdown", None)
        abandoned, hash_changed = self.notifier.reconcile_startup(now, self.cfg.config_hash)
        if abandoned:
            await self.notifier.alert(
                f"启动对账：{len(abandoned)} 条过期 pending 已标 abandoned，不自动补发",
                "startup.abandoned",
                now,
            )
        if hash_changed:
            log.warning("config_hash changed to %s", self.cfg.config_hash)
            await self.notifier.alert(
                f"参数变更：配置指纹已更新为 {self.cfg.config_hash}",
                "startup.config_hash",
                now,
            )
        for row in self.store.list_signals("pending"):
            await self.notifier.send_signal(int(row["id"]), payload_from_signal_row(row))
        self.store.kv_set("started_at", self.started_at)
        self._write_hb()
        self.bus.subscribe("signal.confirmed", self._on_signal)
        self.bus.subscribe("core.health", self._on_health)

    async def close(self) -> None:
        self.watcher.abort_all(datetime.now(UTC))
        await self.client.aclose()
        await self.notifier.aclose()

    def request_stop(self) -> None:
        self._stop.set()

    async def _flush_budget_alerts(self) -> None:
        now = datetime.now(UTC)
        while self._budget_alerts:
            msg = self._budget_alerts.pop(0)
            await self.notifier.alert(msg, "budget.cap", now)

    def _write_hb(self) -> None:
        path = Path(str(self.cfg.get("paths.heartbeat_file")))
        write_heartbeat(path, self.started_at, self.store.kv_get("last_collection_ok_at"))
        self.store.kv_set("heartbeat_at", datetime.now(UTC).isoformat())

    async def _on_signal(self, _type: str, payload: dict[str, Any]) -> None:
        sid = payload.get("signal_id")
        if isinstance(sid, int):
            await self.notifier.send_signal(sid, payload)

    async def _on_health(self, _type: str, payload: dict[str, Any]) -> None:
        log.info("core.health %s", payload)

    async def run(self) -> None:
        await self.start()
        tasks = [
            asyncio.create_task(self._loop_watch(), name="watch"),
            asyncio.create_task(self._loop_track(), name="track"),
            asyncio.create_task(self._loop_maint(), name="maint"),
            asyncio.create_task(self._loop_report(), name="report"),
            asyncio.create_task(self._loop_heartbeat(), name="hb"),
        ]
        source = str(self.cfg.get("streams.source"))
        if source == "megafilter" and self.cfg.get("streams.megafilter.enabled"):
            tasks.append(
                asyncio.create_task(
                    self._loop_stream(
                        "megafilter", float(self.cfg.get("streams.megafilter.interval_sec"))
                    ),
                    name="megafilter",
                )
            )
        elif source == "new_pools" and self.cfg.get("streams.new_pools.enabled"):
            tasks.append(
                asyncio.create_task(
                    self._loop_stream(
                        "new_pools", float(self.cfg.get("streams.new_pools.interval_sec"))
                    ),
                    name="new_pools",
                )
            )
        if self.cfg.get("streams.trending_5m.enabled"):
            tasks.append(
                asyncio.create_task(
                    self._loop_stream(
                        "trending_5m", float(self.cfg.get("streams.trending_5m.interval_sec"))
                    ),
                    name="trending_5m",
                )
            )
        if self.cfg.get("streams.trending_1h.enabled"):
            tasks.append(
                asyncio.create_task(
                    self._loop_stream(
                        "trending_1h", float(self.cfg.get("streams.trending_1h.interval_sec"))
                    ),
                    name="trending_1h",
                )
            )
        await self._stop.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.close()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            return

    def _recent_ttl_sec(self) -> float:
        return recent_ttl_sec(self.cfg.raw)

    async def _loop_stream(self, stream: str, interval: float) -> None:
        while not self._stop.is_set():
            try:
                batches = await collect_stream(self.client, self.cfg.raw, stream)
                async with self._cycle_lock:
                    await process_batches(
                        batches=batches,
                        client=self.client,
                        store=self.store,
                        raw=self.cfg.raw,
                        watcher=self.watcher,
                        now=datetime.now(UTC),
                        config_hash=self.cfg.config_hash,
                        recent_ids=self._recent_pool_ids,
                        recent_ttl_sec=self._recent_ttl_sec(),
                    )
                self._stream_fails[stream] = 0
                await self._flush_budget_alerts()
            except Exception:
                n = self._stream_fails.get(stream, 0) + 1
                self._stream_fails[stream] = n
                log.exception("%s cycle failed; skip round", stream)
                threshold = int(self.cfg.get("telegram.consecutive_failure_alert"))
                if n >= threshold:
                    await self.notifier.alert(
                        f"{stream} 连续失败 {n} 次",
                        f"collect.fail.{stream}",
                        datetime.now(UTC),
                    )
            await self._sleep(interval)

    async def _loop_watch(self) -> None:
        interval = float(self.cfg.get("watch.poll_interval_sec"))
        window = float(self.cfg.get("watch.window_min"))
        min_usd = float(self.cfg.get("watch.min_trade_usd"))
        while not self._stop.is_set():
            now = datetime.now(UTC)
            funnel = Funnel(self.store, now)
            for sess in list(self.watcher.sessions.values()):
                await self._poll_one_watch(sess, now, window, min_usd, funnel)
            await self._sleep(interval)

    async def _poll_one_watch(
        self,
        sess: Any,
        now: datetime,
        window: float,
        min_usd: float,
        funnel: Funnel,
    ) -> None:
        try:
            payload = await self.client.trades(
                sess.network,
                sess.address,
                trade_volume_in_usd_greater_than=min_usd,
            )
            trades = parse_trades(payload, min_usd, sess.entered_at)
            result = evaluate_trades(
                trades,
                baseline=sess.baseline,
                confirm=self.cfg.raw["watch"]["confirm"],
                now=now,
                entered_at=sess.entered_at,
            )
            if result.confirmed:
                self.watcher.finish(sess, now, "confirmed", result.stats, funnel.add)
                await self._emit_confirmed(sess, result.stats, now)
            elif (now - sess.entered_at).total_seconds() >= window * 60:
                self.watcher.finish(sess, now, "timeout", result.stats, funnel.add)
        except Exception:
            elapsed = (now - sess.entered_at).total_seconds()
            timed_out = elapsed >= window * 60
            if timed_out:
                self.watcher.finish(sess, now, "timeout", ConfirmStats(), funnel.add)
            log.exception("watch poll failed %s", sess.pool_id)

    async def _emit_confirmed(self, sess: Any, stats: Any, now: datetime) -> None:
        payload = {
            "symbol": sess.features.get("symbol"),
            "network": sess.network,
            "token_address": sess.token_address,
            "pool_address": sess.address,
            "created_at": now.isoformat(),
            "fdv_usd": sess.features.get("fdv_usd"),
            "reserve_usd": sess.features.get("reserve_usd"),
            "age_min": sess.features.get("age_min"),
            "buyers": stats.buyers,
            "sellers": stats.sellers,
            "buy_sell_ratio": stats.buy_sell_ratio,
            "price_change_pct": stats.price_change_pct,
            "price_change_usd": sess.features.get("price_change_usd"),
            "price_change_native": sess.features.get("price_change_native"),
            "holders": sess.features.get("holders"),
            "dwell_sec": stats.actual_dwell_sec,
            "gt_score": sess.features.get("gt_score"),
            "mint_authority": sess.features.get("mint_authority"),
            "freeze_authority": sess.features.get("freeze_authority"),
            "honeypot": sess.features.get("honeypot"),
        }
        features = dict(sess.features)
        features.update(
            {
                "dwell_sec": stats.actual_dwell_sec,
                "buyers": stats.buyers,
                "sellers": stats.sellers,
                "buy_sell_ratio": stats.buy_sell_ratio,
                "price_change_pct": stats.price_change_pct,
            }
        )
        sid = self.notifier.try_insert_pending(
            network=sess.network,
            token_address=sess.token_address,
            pool_id=sess.pool_id,
            created_at=now.isoformat(),
            config_hash=self.cfg.config_hash,
            payload=payload,
            score=sess.score,
            features=features,
            price=sess.baseline,
            fdv=sess.features.get("fdv_usd"),
        )
        if sid is not None:
            payload["signal_id"] = sid
            await self.bus.publish("signal.confirmed", payload, persist=False)

    async def _loop_track(self) -> None:
        hours = float(self.cfg.get("tracking.scan_interval_h"))
        while not self._stop.is_set():
            if self.cfg.get("tracking.enabled"):
                await self._track_once(datetime.now(UTC))
            await self._sleep(hours * 3600)

    async def _track_once(self, now: datetime) -> None:
        after = float(self.cfg.get("tracking.evaluate_after_h"))
        cutoff = now - timedelta(hours=after)
        drawdown = float(self.cfg.get("tracking.rug.price_drawdown_pct"))
        confirm_h = float(self.cfg.get("tracking.rug.confirm_hours"))
        for row in self.store.list_signals("sent"):
            created = datetime.fromisoformat(str(row["created_at"]))
            if created.tzinfo is None:
                created = created.replace(tzinfo=UTC)
            if created > cutoff:
                continue
            await self._eval_row(
                "signal",
                int(row["id"]),
                str(row["network"]),
                str(row["token_address"]),
                float(row["price_at_signal"] or 0),
                created,
                now,
                drawdown,
                confirm_h,
            )
        if self.cfg.get("tracking.track_negatives"):
            negs = self.store.read(
                lambda c: c.execute(
                    "SELECT * FROM watch_log "
                    "WHERE outcome IN ('timeout','evicted') AND ended_at IS NOT NULL"
                ).fetchall()
            )
            for row in negs:
                ended = datetime.fromisoformat(str(row["ended_at"]))
                if ended.tzinfo is None:
                    ended = ended.replace(tzinfo=UTC)
                if ended > cutoff:
                    continue
                src = "watch_timeout" if row["outcome"] == "timeout" else "watch_evicted"
                await self._eval_row(
                    src,
                    int(row["id"]),
                    str(row["network"]),
                    str(row["token_address"]),
                    float(row["baseline_price"] or 0),
                    ended,
                    now,
                    drawdown,
                    confirm_h,
                )

    async def _eval_row(
        self,
        source: str,
        source_id: int,
        network: str,
        token: str,
        baseline: float,
        start: datetime,
        now: datetime,
        drawdown: float,
        confirm_h: float,
    ) -> None:
        existing = self.store.read(
            lambda c: c.execute(
                "SELECT id FROM signal_outcomes WHERE source = ? AND source_id = ?",
                (source, source_id),
            ).fetchone()
        )
        if existing:
            return
        tf = str(self.cfg.get("tracking.granularity.full.tf"))
        agg = int(self.cfg.get("tracking.granularity.full.agg"))
        payload = await self.client.token_ohlcv(
            network, token, tf, aggregate=agg, include_empty_intervals=True
        )
        rows = ohlcv_rows(payload)
        metrics = compute_metrics(
            rows,
            baseline=baseline or 1.0,
            start_ts=start,
            drawdown_pct=drawdown,
            confirm_hours=confirm_h,
        )
        oid = self.store.insert_outcome(
            source, source_id, network, token, now.isoformat(), json.dumps(payload)
        )
        self.store.update_outcome_metrics(
            oid,
            baseline_price=baseline,
            is_rug=bool(metrics["is_rug"]),
            ohlcv_json=json.dumps(payload),
            **{
                k: metrics[k]
                for k in (
                    "max_gain_pct",
                    "max_drawdown_pct",
                    "t_to_peak_min",
                    "price_1h",
                    "price_24h",
                )
            },
        )

    async def _loop_maint(self) -> None:
        hours = float(self.cfg.get("storage.cleanup_interval_h"))
        while not self._stop.is_set():
            self.store.maintenance_once(cleanup_config_from_raw(self.cfg.raw))
            await self._sleep(hours * 3600)

    async def _loop_report(self) -> None:
        while not self._stop.is_set():
            now = datetime.now(UTC)
            target = str(self.cfg.get("report.daily_at_utc"))
            hh, mm = (int(x) for x in target.split(":"))
            due = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            wait = (due - now).total_seconds()
            if wait <= 0:
                wait += 86400
            await self._sleep(wait)
            if self._stop.is_set() or not self.cfg.get("report.enabled"):
                continue
            await self._push_report(datetime.now(UTC))

    async def _push_report(self, now: datetime) -> None:
        day = report_date_utc(now)
        stats = report_stats(self.store, day)
        funnel_rows = [
            (str(r["layer"]), str(r["rule"]), int(r["n"])) for r in self.store.funnel_day(day)
        ]
        l2 = [
            r
            for r in funnel_rows
            if r[0] in {"l2a", "l2b"} and ":" not in r[1] and not r[1].startswith("_")
        ]
        top = max(l2, key=lambda x: x[2])[1] if l2 else None
        zero_days = 0
        alert_n = int(self.cfg.get("report.zero_signal_alert_days"))
        for i in range(1, alert_n + 1):
            d = (now.date() - timedelta(days=i)).isoformat()
            if self.store.sent_count_on(d) == 0:
                zero_days += 1
            else:
                break
        text = build_daily_report(
            config_hash=self.cfg.config_hash,
            sent=stats["sent"],
            doubled_1h=stats["doubled_1h"],
            rugs=stats["rugs"],
            outcomes_n=stats["outcomes_n"],
            timeout_n=stats["timeout_n"],
            timeout_later_double=stats["timeout_later_double"],
            dwell_median=stats["dwell_median"],
            funnel=funnel_rows,
            top_l2_rule=top,
            day_credits=self.store.daily_calls(day),
            month_credits=self.store.month_credits(day[:7]),
            zero_days=zero_days,
            zero_alert_days=int(self.cfg.get("report.zero_signal_alert_days")),
            timeout_max_summary=stats["timeout_max_summary"],
        )
        await self.notifier.alert(text, f"report.{day}", now)

    async def _loop_heartbeat(self) -> None:
        interval = float(self.cfg.get("runtime.health.heartbeat_interval_sec"))
        while not self._stop.is_set():
            self._write_hb()
            now = datetime.now(UTC)
            await self._flush_budget_alerts()
            await self.bus.publish("core.health", {"at": now.isoformat()})
            server = self.client.last_server_date
            warn = float(self.cfg.get("runtime.health.clock_skew_warn_sec"))
            if server is not None and abs((now - server).total_seconds()) > warn:
                await self.notifier.alert(
                    f"时钟偏差 {abs((now - server).total_seconds()):.0f}s 超过 {warn}s",
                    "runtime.clock_skew",
                    now,
                )
            await self._sleep(interval)
