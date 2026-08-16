"""1h outcome scan for sent signals."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from memebot.grade import Bar, parse_ohlcv
from memebot.store import Store

log = logging.getLogger("memebot.tracker")


@dataclass(frozen=True)
class Outcome:
    expire_price: float
    rel_change_pct: float
    peak_price: float
    drawdown_pct: float
    deep_drawdown: bool


def ohlcv_limit(after_h: float) -> int:
    return min(1000, math.ceil(after_h * 60) + 2)


def compute_outcome(
    bars: list[Bar],
    *,
    sent_at: datetime,
    after_h: float,
    price_at_signal: float,
    drawdown_pct: float,
) -> Outcome | None:
    if price_at_signal is None or price_at_signal <= 0:
        return None
    sent_ts = sent_at.timestamp()
    expire_ts = sent_ts + after_h * 3600
    window = [bar for bar in bars if sent_ts <= bar.ts <= expire_ts]
    if not window:
        return None
    expire_price = window[-1].c
    peak_price = max(bar.h for bar in window)
    if expire_price <= 0 or peak_price <= 0:
        return None
    rel = (expire_price - price_at_signal) / price_at_signal * 100
    dd = (peak_price - expire_price) / peak_price * 100
    return Outcome(
        expire_price=expire_price,
        rel_change_pct=rel,
        peak_price=peak_price,
        drawdown_pct=dd,
        deep_drawdown=dd >= drawdown_pct,
    )


class Tracker:
    def __init__(self, store: Store, raw: dict[str, Any], client: Any) -> None:
        self.store = store
        self.raw = raw
        self.client = client

    async def _ohlcv(self, network: str, pool_address: str, token_address: str) -> list[Bar]:
        limit = ohlcv_limit(float(self.raw["tracker"]["after_h"]))
        try:
            payload = await self.client.pool_ohlcv(
                network,
                pool_address,
                "minute",
                aggregate=1,
                include_empty_intervals=True,
                limit=limit,
            )
            bars = parse_ohlcv(payload)
            if bars:
                return bars
        except Exception as exc:
            log.warning("tracker pool ohlcv failed %s: %s", pool_address, exc)
        try:
            payload = await self.client.token_ohlcv(
                network,
                token_address,
                "minute",
                aggregate=1,
                include_empty_intervals=True,
                limit=limit,
            )
            return parse_ohlcv(payload)
        except Exception as exc:
            log.warning("tracker token ohlcv failed %s: %s", token_address, exc)
            return []

    async def scan(self, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        after_h = float(self.raw["tracker"]["after_h"])
        max_attempts = int(self.raw["tracker"]["max_attempts"])
        drawdown = float(self.raw["tracker"]["drawdown_pct"])
        cutoff = (now - timedelta(hours=after_h)).isoformat()
        done = 0
        for row in self.store.list_due_sent(cutoff):
            existing = self.store.get_outcome(int(row["id"]))
            if existing is not None and int(existing["failed"]) == 0:
                continue
            if existing is not None and int(existing["attempts"]) >= max_attempts:
                continue
            sent_raw = str(row["sent_at"])
            try:
                sent_at = datetime.fromisoformat(sent_raw)
            except ValueError:
                self.store.upsert_outcome(int(row["id"]), failed=True)
                continue
            if sent_at.tzinfo is None:
                sent_at = sent_at.replace(tzinfo=UTC)
            bars = await self._ohlcv(
                str(row["network"]), str(row["pool_address"]), str(row["token_address"])
            )
            price = row["price_at_signal"]
            price_f = float(price) if price is not None else 0.0
            outcome = compute_outcome(
                bars,
                sent_at=sent_at,
                after_h=after_h,
                price_at_signal=price_f,
                drawdown_pct=drawdown,
            )
            if outcome is None:
                self.store.upsert_outcome(int(row["id"]), failed=True)
                continue
            self.store.upsert_outcome(
                int(row["id"]),
                failed=False,
                expire_price=outcome.expire_price,
                rel_change_pct=outcome.rel_change_pct,
                peak_price=outcome.peak_price,
                drawdown_pct=outcome.drawdown_pct,
                deep_drawdown=outcome.deep_drawdown,
            )
            done += 1
        return done
