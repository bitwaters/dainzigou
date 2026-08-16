"""Persist CoinGecko credits and enforce the global daily cap."""

from __future__ import annotations

import calendar
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from memebot.store import CREDIT_KINDS, Store

log = logging.getLogger("memebot.budget")

AlertFn = Callable[[str], None]


class BudgetExhausted(RuntimeError):
    """Raised when collect+security+watch+track hit the daily cap."""


class Budget:
    def __init__(
        self,
        store: Store,
        *,
        global_daily_call_cap: int,
        monthly_credit_warn_pct: float,
        alert: AlertFn | None = None,
    ) -> None:
        self.store = store
        self.global_daily_call_cap = global_daily_call_cap
        self.monthly_credit_warn_pct = monthly_credit_warn_pct
        self._alert = alert
        self._month_warned = False
        self._cap_alerted = False

    def today_utc(self) -> str:
        return datetime.now(UTC).date().isoformat()

    def remaining(self, date_utc: str | None = None) -> int:
        used = self.store.daily_calls(date_utc or self.today_utc())
        return max(0, self.global_daily_call_cap - used)

    def record(self, kind: str, date_utc: str | None = None) -> int:
        if kind not in CREDIT_KINDS:
            raise ValueError(f"invalid credit kind: {kind}")
        day = date_utc or self.today_utc()
        used = self.store.daily_calls(day)
        if used >= self.global_daily_call_cap:
            self._fire_cap(used)
            raise BudgetExhausted(
                f"global_daily_call_cap reached ({used}/{self.global_daily_call_cap})"
            )
        total = self.store.add_credits(day, kind, calls=1, credits=1)
        if self.store.daily_calls(day) >= self.global_daily_call_cap:
            self._fire_cap(self.store.daily_calls(day))
        self._maybe_month_warn(day)
        return total

    def _fire_cap(self, used: int) -> None:
        msg = (
            f"emergency: CoinGecko daily cap reached "
            f"({used}/{self.global_daily_call_cap}); collection stopped"
        )
        log.error(msg)
        if not self._cap_alerted and self._alert is not None:
            self._alert(msg)
            self._cap_alerted = True

    def _maybe_month_warn(self, day: str) -> None:
        if self._month_warned:
            return
        year, month, _ = (int(p) for p in day.split("-"))
        days = calendar.monthrange(year, month)[1]
        month_budget = self.global_daily_call_cap * days
        used = self.store.month_credits(f"{year:04d}-{month:02d}")
        if month_budget <= 0:
            return
        if used / month_budget * 100 >= self.monthly_credit_warn_pct:
            msg = (
                f"monthly CoinGecko credits at {used}/{month_budget} "
                f"({self.monthly_credit_warn_pct}%)"
            )
            log.warning(msg)
            if self._alert is not None:
                self._alert(msg)
            self._month_warned = True
