"""Gate checks: chain, quote, age, liquidity, fdv, turnover, list m5."""

from __future__ import annotations

import time
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
        if value is not None:
            return float(value)
    return None


# #region agent log
def _dbg(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    try:
        import json as _json

        with open("/Users/yang/Documents/tgbot/.cursor/debug-1ea519.log", "a", encoding="utf-8") as _f:
            _f.write(
                _json.dumps(
                    {
                        "sessionId": "1ea519",
                        "hypothesisId": hypothesis_id,
                        "location": location,
                        "message": message,
                        "data": data,
                        "timestamp": int(time.time() * 1000),
                    },
                    default=str,
                )
                + "\n"
            )
    except Exception:
        pass


# #endregion


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
            # #region agent log
            _dbg(
                "F",
                "gates.py:evaluate",
                "gate_age",
                {
                    "addr_tail": pool.token_address[-8:],
                    "age_min": round(age_min, 3),
                    "min_age": min_age,
                    "reserve": pool.reserve_usd,
                    "fdv": pool.fdv_usd,
                },
            )
            # #endregion
            return GateDecision(False, "gate_age")

    min_reserve = gates.get("min_reserve_usd")
    if min_reserve is not None:
        if pool.reserve_usd is None or pool.reserve_usd < float(min_reserve):
            return GateDecision(False, "gate_liq")

    min_fdv = gates.get("min_fdv_usd")
    if min_fdv is not None:
        mcap = _mcap_usd(pool)
        if mcap is None or mcap < float(min_fdv):
            # #region agent log
            _dbg(
                "G",
                "gates.py:evaluate",
                "gate_fdv",
                {
                    "addr_tail": pool.token_address[-8:],
                    "mcap": mcap,
                    "min_fdv": min_fdv,
                    "reserve": pool.reserve_usd,
                },
            )
            # #endregion
            return GateDecision(False, "gate_fdv")

    max_turn = gates.get("max_m15_vol_to_reserve")
    if max_turn is not None:
        vol = _window_volume(pool)
        if vol is None or pool.reserve_usd is None or pool.reserve_usd <= 0:
            return GateDecision(False, "gate_turnover")
        turnover = vol / pool.reserve_usd
        if turnover > float(max_turn):
            # #region agent log
            _dbg(
                "H",
                "gates.py:evaluate",
                "gate_turnover",
                {
                    "addr_tail": pool.token_address[-8:],
                    "vol": vol,
                    "reserve": pool.reserve_usd,
                    "turnover": round(turnover, 3),
                    "max_turn": max_turn,
                },
            )
            # #endregion
            return GateDecision(False, "gate_turnover")

    min_m5 = (raw.get("radar") or {}).get("min_m5_pct")
    if min_m5 is not None:
        changes = pool.price_change_usd or {}
        m5 = changes.get("m5")
        if m5 is None or m5 < float(min_m5):
            return GateDecision(False, "gate_m5")

    return GateDecision(True, None)
