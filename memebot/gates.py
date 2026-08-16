"""Gate checks: chain, quote token, liquidity, list m5."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
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


def evaluate(pool: PoolSnapshot, raw: Mapping[str, Any]) -> GateDecision:
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

    min_reserve = (raw.get("gates") or {}).get("min_reserve_usd")
    if min_reserve is not None:
        if pool.reserve_usd is None or pool.reserve_usd < float(min_reserve):
            return GateDecision(False, "gate_liq")

    min_m5 = (raw.get("radar") or {}).get("min_m5_pct")
    if min_m5 is not None:
        changes = pool.price_change_usd or {}
        m5 = changes.get("m5")
        if m5 is None or m5 < float(min_m5):
            return GateDecision(False, "gate_m5")

    return GateDecision(True, None)
