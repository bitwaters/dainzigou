"""Persist CoinGecko credits and enforce the CG daily cap only."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

from memebot.store import CREDIT_KINDS, Store

log = logging.getLogger("memebot.budget")

AlertFn = Callable[[str], None]


class BudgetExhausted(RuntimeError):
    """Raised when collect+ohlcv+trades hit the CoinGecko daily cap."""


class Budget:
    def __init__(
        self,
        store: Store,
        *,
        cg_daily_call_cap: int,
        alert: AlertFn | None = None,
    ) -> None:
        self.store = store
        self.cg_daily_call_cap = cg_daily_call_cap
        self._alert = alert
        self._cap_alerted = False

    def today_utc(self) -> str:
        return datetime.now(UTC).date().isoformat()

    def remaining(self, date_utc: str | None = None) -> int:
        used = self.store.cg_calls_today(date_utc or self.today_utc())
        return max(0, self.cg_daily_call_cap - used)

    def record(self, kind: str, date_utc: str | None = None) -> int:
        if kind not in CREDIT_KINDS:
            raise ValueError(f"invalid credit kind: {kind}")
        day = date_utc or self.today_utc()
        if kind == "goplus":
            return self.store.add_credits(day, kind, calls=1)
        used = self.store.cg_calls_today(day)
        if used >= self.cg_daily_call_cap:
            self._fire_cap(used)
            raise BudgetExhausted(
                f"cg_daily_call_cap reached ({used}/{self.cg_daily_call_cap})"
            )
        total = self.store.add_credits(day, kind, calls=1)
        if self.store.cg_calls_today(day) >= self.cg_daily_call_cap:
            self._fire_cap(self.store.cg_calls_today(day))
        return total

    def _fire_cap(self, used: int) -> None:
        msg = (
            f"emergency: CoinGecko daily cap reached "
            f"({used}/{self.cg_daily_call_cap}); collection stopped"
        )
        log.error(msg)
        if not self._cap_alerted and self._alert is not None:
            self._alert(msg)
            self._cap_alerted = True
