"""Pure grading: OHLCV metrics, net buy, strong/weak decision order."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from memebot.pool import parse_ts

Grade = Literal["strong", "weak"]


@dataclass(frozen=True)
class Bar:
    ts: int
    o: float
    h: float
    low: float
    c: float


@dataclass(frozen=True)
class Trade:
    ts: int
    kind: str
    usd: float
    maker: str = ""


@dataclass(frozen=True)
class Metrics:
    c_now: float
    c_ref: float
    high_short: float
    chg_1m: float
    dist: float


@dataclass(frozen=True)
class GradeDecision:
    grade: Grade | None
    reason: str


def parse_ohlcv(payload: dict[str, Any] | None) -> list[Bar]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    attrs = data.get("attributes") if isinstance(data, dict) else {}
    rows = attrs.get("ohlcv_list") if isinstance(attrs, dict) else []
    out: list[Bar] = []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            ts = float(row[0])
            o = float(row[1])
            h = float(row[2])
            low = float(row[3])
            c = float(row[4])
        except (TypeError, ValueError):
            continue
        out.append(Bar(ts=int(ts), o=o, h=h, low=low, c=c))
    out.sort(key=lambda b: b.ts)
    return out


def _trade_ts(raw: Any) -> int | None:
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int | float):
        return int(raw)
    parsed = parse_ts(raw)
    if parsed is None:
        return None
    return int(parsed.timestamp())


def parse_trades(payload: dict[str, Any] | None) -> list[Trade] | None:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, list):
        return None
    out: list[Trade] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        attrs = item.get("attributes")
        if not isinstance(attrs, dict):
            continue
        kind = attrs.get("kind")
        if kind not in ("buy", "sell"):
            continue
        ts = _trade_ts(attrs.get("block_timestamp") or attrs.get("timestamp"))
        if ts is None:
            continue
        try:
            usd = float(attrs.get("volume_in_usd") or attrs.get("price_usd_volume") or 0)
        except (TypeError, ValueError):
            continue
        out.append(
            Trade(
                ts=ts,
                kind=str(kind),
                usd=usd,
                maker=str(attrs.get("tx_from_address") or ""),
            )
        )
    return out


def compute_metrics(bars: list[Bar], now_ts: int, near_high_bars: int) -> Metrics | None:
    if not bars or near_high_bars < 1:
        return None
    c_now = bars[-1].c
    cutoff = now_ts - 60
    refs = [b for b in bars if b.ts <= cutoff]
    if not refs:
        return None
    c_ref = refs[-1].c
    window = bars[-near_high_bars:]
    high_short = max(b.h for b in window)
    if c_now <= 0 or c_ref <= 0 or high_short <= 0:
        return None
    chg_1m = (c_now - c_ref) / c_ref * 100
    dist = (high_short - c_now) / high_short * 100
    return Metrics(c_now=c_now, c_ref=c_ref, high_short=high_short, chg_1m=chg_1m, dist=dist)


def ratio_ok(
    chg_1m: float,
    h1: float | None,
    m5: float | None,
    *,
    min_to_h1: float,
    min_to_m5: float,
) -> bool:
    if h1 is None:
        if m5 is None or m5 <= 0:
            return False
        return min(chg_1m / m5, 1) >= min_to_m5
    if h1 > 0:
        return min(chg_1m / h1, 1) >= min_to_h1
    return chg_1m > 0


def net_buy(
    trades: list[Trade],
    now_ts: int,
    lookback_sec: float,
    min_trade_usd: float,
) -> tuple[float, float, float]:
    cutoff = now_ts - lookback_sec
    buy = 0.0
    sell = 0.0
    for trade in trades:
        if trade.ts <= cutoff or trade.ts > now_ts:
            continue
        if trade.usd < min_trade_usd:
            continue
        if trade.kind == "buy":
            buy += trade.usd
        elif trade.kind == "sell":
            sell += trade.usd
    return buy, sell, buy - sell


def maker_stats(
    trades: list[Trade],
    now_ts: int,
    lookback_sec: float,
) -> tuple[int, int]:
    cutoff = now_ts - lookback_sec
    counts: dict[str, int] = {}
    for trade in trades:
        if trade.ts <= cutoff or trade.ts > now_ts:
            continue
        maker = trade.maker.strip()
        if not maker:
            continue
        counts[maker] = counts.get(maker, 0) + 1
    if not counts:
        return 0, 0
    return len(counts), max(counts.values())


def decide(
    *,
    chg_1m: float,
    dist: float,
    h1: float | None,
    m5: float | None,
    net: float | None,
    require_net_buy: bool,
    authority_open: bool,
    weak_live: bool,
    strong_min_1m_pct: float,
    weak_min_1m_pct: float,
    strong_max_dist_pct: float,
    weak_max_dist_pct: float,
    min_to_h1: float,
    min_to_m5: float,
) -> GradeDecision:
    if require_net_buy and (net is None or net <= 0):
        return GradeDecision(None, "net_buy_nonpositive")
    if dist > weak_max_dist_pct:
        return GradeDecision(None, "pullback")
    if not authority_open:
        if (
            chg_1m >= strong_min_1m_pct
            and ratio_ok(chg_1m, h1, m5, min_to_h1=min_to_h1, min_to_m5=min_to_m5)
            and dist <= strong_max_dist_pct
        ):
            return GradeDecision("strong", "strong")
    if (
        not weak_live
        and chg_1m >= weak_min_1m_pct
        and dist <= weak_max_dist_pct
    ):
        return GradeDecision("weak", "weak")
    return GradeDecision(None, "grade_none")
