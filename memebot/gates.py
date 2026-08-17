"""Gate checks: chain, quote, age, liquidity, fdv, turnover, list m5."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from memebot.pool import PoolSnapshot


@dataclass(frozen=True)
class GateDecision:
    passed: bool
    step: str | None


def _norm_quote(network: str, address: str) -> str:
    if network == "bsc":
        return address.lower()
    return address


def _mcap_usd(pool: PoolSnapshot) -> float | None:
    if pool.market_cap_usd is not None and pool.market_cap_usd > 0:
        return pool.market_cap_usd
    return pool.fdv_usd


def _window_volume(pool: PoolSnapshot) -> float | None:
    vol = pool.volume or {}
    for key in ("m15", "m5"):
        value = vol.get(key)
        if value is not None and float(value) > 0:
            return float(value)
    return None


def evaluate(
    pool: PoolSnapshot,
    raw: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> GateDecision:
    clock = now or datetime.now(UTC)
    networks = raw.get("networks") or []
    if pool.network not in networks:
        return GateDecision(False, "gate_chain")

    quotes = (raw.get("gates") or {}).get("quote_tokens")
    if quotes is not None:
        allowed_raw = quotes.get(pool.network) if isinstance(quotes, Mapping) else None
        allowed = allowed_raw if isinstance(allowed_raw, list) else []
        if not pool.quote_address:
            return GateDecision(False, "gate_quote")
        needle = _norm_quote(pool.network, pool.quote_address)
        haystack = {
            _norm_quote(pool.network, addr) for addr in allowed if isinstance(addr, str)
        }
        if needle not in haystack:
            return GateDecision(False, "gate_quote")

    gates = raw.get("gates") or {}
    min_age = gates.get("min_pool_age_min")
    if min_age is not None:
        created = pool.pool_created_at
        if created is None:
            return GateDecision(False, "gate_age")
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        age_min = (clock - created).total_seconds() / 60.0
        if age_min < float(min_age):
            return GateDecision(False, "gate_age")

    min_reserve = gates.get("min_reserve_usd")
    if min_reserve is not None:
        if pool.reserve_usd is None or pool.reserve_usd < float(min_reserve):
            return GateDecision(False, "gate_liq")

    min_fdv = gates.get("min_fdv_usd")
    if min_fdv is not None:
        mcap = _mcap_usd(pool)
        if mcap is None or mcap < float(min_fdv):
            return GateDecision(False, "gate_fdv")

    max_turn = gates.get("max_m15_vol_to_reserve")
    if max_turn is not None:
        vol = _window_volume(pool)
        if vol is None or pool.reserve_usd is None or pool.reserve_usd <= 0:
            return GateDecision(False, "gate_turnover")
        turnover = vol / pool.reserve_usd
        if turnover > float(max_turn):
            return GateDecision(False, "gate_turnover")

    min_m5 = (raw.get("radar") or {}).get("min_m5_pct")
    if min_m5 is not None:
        changes = pool.price_change_usd or {}
        m5 = changes.get("m5")
        if m5 is None or m5 < float(min_m5):
            return GateDecision(False, "gate_m5")

    return GateDecision(True, None)
