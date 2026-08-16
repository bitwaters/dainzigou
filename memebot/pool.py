"""Parse CoinGecko pool JSON:API objects into snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _f(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _i(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_ts(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def normalize_symbol(symbol: str) -> str:
    lowered = symbol.lower()
    chars: list[str] = []
    for ch in lowered:
        o = ord(ch)
        if ch.isalnum() and not (0x200B <= o <= 0x200F or 0x202A <= o <= 0x202E):
            chars.append(ch)
    return "".join(chars)


@dataclass
class PoolSnapshot:
    network: str
    pool_id: str
    address: str
    token_address: str
    quote_address: str
    symbol: str
    name: str
    source: str
    pool_created_at: datetime | None
    reserve_usd: float | None
    fdv_usd: float | None
    market_cap_usd: float | None
    price_usd: float | None
    price_native: float | None
    volume: dict[str, float]
    tx: dict[str, dict[str, float]]
    price_change_usd: dict[str, float | None]
    price_change_native: dict[str, float | None] = field(default_factory=dict)
    sus_reports: int | None = None
    included_tokens: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def symbol_norm(self) -> str:
        return normalize_symbol(self.symbol)


def _included_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in payload.get("included") or []:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            out[item["id"]] = item
    return out


def _rel_id(item: dict[str, Any], name: str) -> str | None:
    rel = item.get("relationships")
    if not isinstance(rel, dict):
        return None
    node = rel.get(name)
    if not isinstance(node, dict):
        return None
    data = node.get("data")
    if isinstance(data, dict):
        ident = data.get("id")
        if isinstance(ident, str):
            return ident
    return None


def _addr_from_included(inc: dict[str, dict[str, Any]], rel_id: str | None) -> str:
    if not rel_id:
        return ""
    item = inc.get(rel_id)
    if not item:
        if "_" in rel_id:
            return rel_id.split("_", 1)[1]
        return ""
    attrs = item.get("attributes")
    if isinstance(attrs, dict):
        addr = attrs.get("address")
        if isinstance(addr, str):
            return addr
    if "_" in rel_id:
        return rel_id.split("_", 1)[1]
    return ""


def _symbol_from_included(inc: dict[str, dict[str, Any]], rel_id: str | None) -> str:
    if not rel_id:
        return ""
    item = inc.get(rel_id)
    if not item:
        return ""
    attrs = item.get("attributes")
    if isinstance(attrs, dict):
        symbol = attrs.get("symbol")
        if isinstance(symbol, str):
            return symbol
    return ""


def parse_pools(payload: dict[str, Any], source: str) -> list[PoolSnapshot]:
    inc = _included_map(payload)
    out: list[PoolSnapshot] = []
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        attrs = as_dict(item.get("attributes"))
        pid = str(item.get("id") or "")
        net = _rel_id(item, "network") or (pid.split("_", 1)[0] if "_" in pid else "")
        base_id = _rel_id(item, "base_token")
        quote_id = _rel_id(item, "quote_token")
        vol_raw = as_dict(attrs.get("volume_usd"))
        tx_raw = as_dict(attrs.get("transactions"))
        ch_raw = as_dict(attrs.get("price_change_percentage"))
        volume = {k: v for k, val in vol_raw.items() if (v := _f(val)) is not None}
        tx: dict[str, dict[str, float]] = {}
        for win, node in tx_raw.items():
            if not isinstance(node, dict):
                continue
            tx[win] = {
                k: float(v)
                for k, v in node.items()
                if isinstance(v, (int, float)) or (isinstance(v, str) and v != "")
            }
        changes = {k: _f(v) for k, v in ch_raw.items()}
        native_raw = as_dict(attrs.get("price_change_percentage_native"))
        native_changes = {k: _f(v) for k, v in native_raw.items()}
        symbol = _symbol_from_included(inc, base_id) or str(attrs.get("name") or "").split(" /")[0]
        name_attrs = inc.get(base_id or "", {}).get("attributes")
        name = ""
        if isinstance(name_attrs, dict) and isinstance(name_attrs.get("name"), str):
            name = name_attrs["name"]
        else:
            name = str(attrs.get("name") or symbol)
        out.append(
            PoolSnapshot(
                network=net,
                pool_id=pid,
                address=str(attrs.get("address") or ""),
                token_address=_addr_from_included(inc, base_id),
                quote_address=_addr_from_included(inc, quote_id),
                symbol=symbol,
                name=name,
                source=source,
                pool_created_at=parse_ts(attrs.get("pool_created_at")),
                reserve_usd=_f(attrs.get("reserve_in_usd")),
                fdv_usd=_f(attrs.get("fdv_usd")),
                market_cap_usd=_f(attrs.get("market_cap_usd")),
                price_usd=_f(attrs.get("base_token_price_usd")),
                price_native=_f(attrs.get("base_token_price_native_currency")),
                volume=volume,
                tx=tx,
                price_change_usd=changes,
                price_change_native=native_changes,
                sus_reports=_i(attrs.get("community_sus_report")),
                included_tokens=inc,
            )
        )
    return out
