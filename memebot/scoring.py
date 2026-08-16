"""Scoring primitives and four-dimension composite score."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any

from memebot.pool import PoolSnapshot

_TERM_WEIGHT = 0.5
_BUYER_SHARE_MID = 0.5


def saturate(x: float, half: float) -> float:
    x = max(0.0, x)
    denom = x + half
    if denom == 0:
        return 0.0
    return x / denom


def exp_decay(age_min: float, half_life_min: float) -> float:
    if half_life_min <= 0:
        return 0.0
    return math.exp(-math.log(2) * max(0.0, age_min) / half_life_min)


def buy_pressure(
    *,
    buyers: float,
    sellers: float,
    buys: float,
    sells: float,
    volume_usd: float | None,
    net_buy_volume_usd: float | None,
    fallback_half_ratio: float,
) -> float:
    """Independent-buyer share plus net-buy share, with buys/sells fallback."""
    total = buyers + sellers
    if total == 0:
        share = 0.0
    else:
        share = max(0.0, (buyers / total - _BUYER_SHARE_MID) * 2)

    if net_buy_volume_usd is not None and volume_usd is not None and volume_usd > 0:
        net = max(0.0, net_buy_volume_usd / volume_usd)
    elif sells == 0:
        net = 0.0
    else:
        net = saturate(max(0.0, buys / sells - 1.0), fallback_half_ratio)
    return _TERM_WEIGHT * share + _TERM_WEIGHT * net


def momentum_score(changes: dict[str, float | None], windows: list[dict[str, Any]]) -> float:
    used: list[tuple[float, float]] = []
    for spec in windows:
        win = str(spec["window"])
        raw = changes.get(win)
        if raw is None:
            continue
        half = float(spec["normalize"]["half"])
        used.append((float(spec["weight"]), saturate(raw, half)))
    total_w = sum(w for w, _ in used)
    if total_w == 0:
        return 0.0
    return sum((w / total_w) * val for w, val in used)


def freshness_score(age_min: float | None, half_life_min: float) -> float:
    if age_min is None:
        return 0.0
    return exp_decay(age_min, half_life_min)


def extract_features(
    pool: PoolSnapshot, now: datetime, extra: dict[str, Any] | None = None
) -> dict[str, Any]:
    age = None
    if pool.pool_created_at is not None:
        age = (now.astimezone(UTC) - pool.pool_created_at.astimezone(UTC)).total_seconds() / 60
    feats: dict[str, Any] = {
        "source": pool.source,
        "network": pool.network,
        "symbol": pool.symbol,
        "symbol_norm": pool.symbol_norm,
        "price_usd": pool.price_usd,
        "price_native": pool.price_native,
        "reserve_usd": pool.reserve_usd,
        "fdv_usd": pool.fdv_usd,
        "age_min": age,
        "volume": pool.volume,
        "tx": pool.tx,
        "price_change_usd": pool.price_change_usd,
        "price_change_native": dict(pool.price_change_native),
        "sus_reports": pool.sus_reports,
    }
    if extra:
        feats.update(extra)
    return feats


def score_pool(
    pool: PoolSnapshot, raw: dict[str, Any], now: datetime
) -> tuple[float, dict[str, Any]]:
    scoring = raw["scoring"]
    age = None
    if pool.pool_created_at is not None:
        age = (now.astimezone(UTC) - pool.pool_created_at.astimezone(UTC)).total_seconds() / 60
    mom = momentum_score(pool.price_change_usd, list(scoring["momentum"]["windows"]))
    win = str(scoring["buy_pressure"]["window"])
    tx = pool.tx.get(win) or {}
    half = float(scoring["turnover"]["normalize"]["half"])
    bp = buy_pressure(
        buyers=float(tx.get("buyers") or 0),
        sellers=float(tx.get("sellers") or 0),
        buys=float(tx.get("buys") or 0),
        sells=float(tx.get("sells") or 0),
        volume_usd=pool.volume.get(win),
        net_buy_volume_usd=None,
        fallback_half_ratio=half,
    )
    turn_win = str(scoring["turnover"]["window"])
    turn_raw = 0.0
    if pool.reserve_usd and pool.reserve_usd > 0 and turn_win in pool.volume:
        turn_raw = pool.volume[turn_win] / pool.reserve_usd
    turn = saturate(turn_raw, half)
    fresh = freshness_score(age, float(scoring["freshness"]["normalize"]["half_life_min"]))
    total = (
        float(scoring["momentum"]["weight"]) * mom
        + float(scoring["buy_pressure"]["weight"]) * bp
        + float(scoring["turnover"]["weight"]) * turn
        + float(scoring["freshness"]["weight"]) * fresh
    )
    features = extract_features(
        pool,
        now,
        {
            "dim_momentum": mom,
            "dim_buy_pressure": bp,
            "dim_turnover": turn,
            "dim_freshness": fresh,
            "score": total,
        },
    )
    return total, features


def pick_top_n(
    scored: list[tuple[PoolSnapshot, float, dict[str, Any]]],
    n: int,
) -> tuple[
    list[tuple[PoolSnapshot, float, dict[str, Any]]],
    list[tuple[PoolSnapshot, float, dict[str, Any]]],
]:
    by_net: dict[str, list[tuple[PoolSnapshot, float, dict[str, Any]]]] = {}
    for item in scored:
        by_net.setdefault(item[0].network, []).append(item)
    picked: list[tuple[PoolSnapshot, float, dict[str, Any]]] = []
    rest: list[tuple[PoolSnapshot, float, dict[str, Any]]] = []
    for group in by_net.values():
        ordered = sorted(group, key=lambda x: x[1], reverse=True)
        picked.extend(ordered[:n])
        rest.extend(ordered[n:])
    return picked, rest


def rescore_features(features: dict[str, Any], raw: dict[str, Any]) -> float:
    """Replay a stored features blob with current (or given) scoring config."""
    scoring = raw["scoring"]
    changes = {
        k: (float(v) if v is not None else None)
        for k, v in (features.get("price_change_usd") or {}).items()
    }
    mom = momentum_score(changes, list(scoring["momentum"]["windows"]))
    win = str(scoring["buy_pressure"]["window"])
    tx = (features.get("tx") or {}).get(win) or {}
    half = float(scoring["turnover"]["normalize"]["half"])
    bp = buy_pressure(
        buyers=float(tx.get("buyers") or 0),
        sellers=float(tx.get("sellers") or 0),
        buys=float(tx.get("buys") or 0),
        sells=float(tx.get("sells") or 0),
        volume_usd=(features.get("volume") or {}).get(win),
        net_buy_volume_usd=None,
        fallback_half_ratio=half,
    )
    reserve = features.get("reserve_usd")
    vol = (features.get("volume") or {}).get(str(scoring["turnover"]["window"]))
    turn_raw = 0.0
    if reserve and reserve > 0 and vol is not None:
        turn_raw = float(vol) / float(reserve)
    turn = saturate(turn_raw, half)
    fresh = freshness_score(
        features.get("age_min"),
        float(scoring["freshness"]["normalize"]["half_life_min"]),
    )
    return (
        float(scoring["momentum"]["weight"]) * mom
        + float(scoring["buy_pressure"]["weight"]) * bp
        + float(scoring["turnover"]["weight"]) * turn
        + float(scoring["freshness"]["weight"]) * fresh
    )
