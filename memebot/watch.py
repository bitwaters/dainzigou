"""Watch confirmation, eviction, and cooldown derivation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from memebot.pool import as_dict, parse_ts
from memebot.store import Store


@dataclass
class Trade:
    ts: datetime
    side: str
    usd: float
    price: float | None
    sender: str


@dataclass
class ConfirmStats:
    buyers: int = 0
    sellers: int = 0
    buys: int = 0
    sells: int = 0
    buyer_seller_ratio: float | None = None
    buy_sell_ratio: float | None = None
    price_change_pct: float | None = None
    max_buyers: int = 0
    max_buyer_seller_ratio: float | None = None
    max_buy_sell_ratio: float | None = None
    max_price_change_pct: float | None = None
    actual_dwell_sec: float = 0.0
    aborted_rule: str | None = None


@dataclass
class ConfirmResult:
    confirmed: bool
    stats: ConfirmStats
    at: datetime | None = None
    last_price: float | None = None
    aborted_rule: str | None = None
    peak_price: float | None = None


def _token_price_usd(attrs: dict[str, Any], kind: str) -> float | None:
    # buy: token is "to"; sell: token is "from". Only the primary field is
    # used: the fallback field holds the OTHER side's USD price (e.g. quote
    # SOL ~$75), which explodes price-change %, pollutes peak/drawdown, and
    # even passes the sane band when the real price field is missing.
    key = "price_to_in_usd" if kind == "buy" else "price_from_in_usd"
    raw = attrs.get(key)
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def parse_trades(
    payload: dict[str, Any], min_trade_usd: float, entered_at: datetime
) -> list[Trade]:
    out: list[Trade] = []
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        attrs = as_dict(item.get("attributes"))
        ts = parse_ts(attrs.get("block_timestamp") or attrs.get("timestamp"))
        if ts is None or ts < entered_at:
            continue
        usd = 0.0
        try:
            usd = float(attrs.get("volume_in_usd") or attrs.get("price_usd_volume") or 0)
        except (TypeError, ValueError):
            usd = 0.0
        if usd < min_trade_usd:
            continue
        kind = str(attrs.get("kind") or attrs.get("tx_type") or "").lower()
        if kind not in {"buy", "sell"}:
            continue
        sender = str(attrs.get("tx_from_address") or attrs.get("from_token_address") or "")
        price = _token_price_usd(attrs, kind)
        out.append(Trade(ts=ts, side=kind, usd=usd, price=price, sender=sender))
    out.sort(key=lambda t: t.ts)
    return out


def _sane_price_change(chg: float | None, confirm: dict[str, Any]) -> float | None:
    if chg is None:
        return None
    lo = confirm.get("sane_pct_min")
    hi = confirm.get("sane_pct_max")
    try:
        if lo is not None and chg < float(lo):
            return None
        if hi is not None and chg > float(hi):
            return None
    except (TypeError, ValueError):
        return None
    return chg


def scale_from_baseline(value: float | None, baseline: float, last: float | None) -> float | None:
    if value is None or last is None or not baseline:
        return value
    return value * (last / baseline)


def _peak_drawdown_pct(peak_price: float, last_price: float) -> float | None:
    if peak_price <= 0:
        return None
    return (peak_price - last_price) / peak_price * 100.0


def evaluate_trades(
    trades: list[Trade],
    *,
    baseline: float,
    confirm: dict[str, Any],
    now: datetime,
    entered_at: datetime,
    seen_peak: float | None = None,
) -> ConfirmResult:
    buyers: set[str] = set()
    sellers: set[str] = set()
    buys = 0
    sells = 0
    last_price = baseline
    stats = ConfirmStats()
    min_buyers = float(confirm["confirm_min_buyers"])
    min_bs_addr = float(confirm["min_buyer_seller_ratio"])
    min_bs_cnt = float(confirm["min_buy_sell_ratio"])
    min_chg = float(confirm["min_price_change_pct"])
    dd_cap = confirm.get("max_drawdown_from_peak_pct")
    confirmed_at: datetime | None = None
    aborted_rule: str | None = None
    peak_price = seen_peak if seen_peak is not None else baseline
    cap: float | None = None
    if dd_cap is not None:
        try:
            cap = float(dd_cap)
        except (TypeError, ValueError):
            cap = None
    for trade in trades:
        if trade.side == "buy":
            buys += 1
            if trade.sender:
                buyers.add(trade.sender)
        elif trade.side == "sell":
            sells += 1
            if trade.sender:
                sellers.add(trade.sender)
        if trade.price is not None:
            last_price = trade.price
            peak_price = max(peak_price, last_price)
        raw_chg = ((last_price - baseline) / baseline * 100) if baseline else None
        chg = _sane_price_change(raw_chg, confirm)
        addr_ratio = (len(buyers) / len(sellers)) if sellers else None
        cnt_ratio = (buys / sells) if sells else None
        stats.buyers = len(buyers)
        stats.sellers = len(sellers)
        stats.buys = buys
        stats.sells = sells
        stats.buyer_seller_ratio = addr_ratio
        stats.buy_sell_ratio = cnt_ratio
        stats.price_change_pct = raw_chg
        stats.max_buyers = max(stats.max_buyers, len(buyers))
        if addr_ratio is not None:
            stats.max_buyer_seller_ratio = max(
                stats.max_buyer_seller_ratio or addr_ratio, addr_ratio
            )
        if cnt_ratio is not None:
            stats.max_buy_sell_ratio = max(stats.max_buy_sell_ratio or cnt_ratio, cnt_ratio)
        if chg is not None:
            stats.max_price_change_pct = max(stats.max_price_change_pct or chg, chg)
        no_sellers = len(sellers) == 0 and sells == 0
        ok_buyers = len(buyers) >= min_buyers
        ok_addr = (no_sellers and ok_buyers) or (
            addr_ratio is not None and addr_ratio >= min_bs_addr
        )
        ok_cnt = (no_sellers and ok_buyers) or (
            cnt_ratio is not None and cnt_ratio >= min_bs_cnt
        )
        ok_chg = chg is not None and chg >= min_chg
        if cap is not None and last_price is not None:
            dd = _peak_drawdown_pct(peak_price, last_price)
            if dd is not None and dd >= cap:
                aborted_rule = "drawdown_from_peak"
                break
        if ok_buyers and ok_addr and ok_cnt and ok_chg and confirmed_at is None:
            confirmed_at = trade.ts
            break
    stats.actual_dwell_sec = (now - entered_at).total_seconds()
    stats.aborted_rule = aborted_rule
    return ConfirmResult(
        confirmed=confirmed_at is not None,
        stats=stats,
        at=confirmed_at,
        last_price=last_price,
        aborted_rule=aborted_rule,
        peak_price=peak_price,
    )


def cooldown_active(
    store: Store,
    network: str,
    token_address: str,
    *,
    max_timeouts: int,
    cooldown_h: float,
    now: datetime,
) -> bool:
    rows = store.watch_history(network, token_address)
    streak = 0
    last_timeout: datetime | None = None
    for row in rows:
        outcome = row["outcome"]
        if outcome in {"evicted", "aborted_shutdown"}:
            continue
        if outcome == "confirmed":
            break
        if outcome == "timeout":
            streak += 1
            if last_timeout is None:
                ended = row["ended_at"]
                last_timeout = datetime.fromisoformat(str(ended))
                if last_timeout.tzinfo is None:
                    last_timeout = last_timeout.replace(tzinfo=UTC)
    if streak < max_timeouts or last_timeout is None:
        return False
    return now.astimezone(UTC) < last_timeout.astimezone(UTC) + timedelta(hours=cooldown_h)


@dataclass
class WatchSession:
    watch_id: int
    pool_id: str
    network: str
    token_address: str
    address: str
    score: float
    entered_at: datetime
    baseline: float
    features: dict[str, Any]
    dwell_protected_until: datetime
    peak_price: float
    last_price: float | None = None


@dataclass
class Watcher:
    store: Store
    raw: dict[str, Any]
    config_hash: str
    sessions: dict[str, WatchSession] = field(default_factory=dict)

    def key(self, network: str, token: str) -> str:
        return f"{network}:{token}"

    def can_add(self, now: datetime, watch_calls_today: int) -> bool:
        cap = int(self.raw["watch"]["daily_call_cap"])
        return watch_calls_today < cap

    def admit(
        self,
        *,
        pool_id: str,
        network: str,
        token_address: str,
        address: str,
        score: float,
        baseline: float,
        features: dict[str, Any],
        now: datetime,
        funnel_add: Any,
    ) -> WatchSession | None:
        max_n = int(self.raw["watch"]["max_concurrent"])
        min_dwell = float(self.raw["watch"]["min_dwell_sec"])
        k = self.key(network, token_address)
        if k in self.sessions:
            return None
        if len(self.sessions) >= max_n:
            victim = min(self.sessions.values(), key=lambda s: s.score)
            protected = now < victim.dwell_protected_until
            new_lower_or_eq = score <= victim.score
            will_evict = not protected and not new_lower_or_eq
            if not will_evict:
                return None
            self.finish(victim, now, "evicted", ConfirmStats(), funnel_add)
        wid = self.store.insert_watch_log(
            pool_id=pool_id,
            network=network,
            token_address=token_address,
            started_at=now.isoformat(),
            config_hash=self.config_hash,
            baseline_price=baseline,
            features_json=json.dumps(features, ensure_ascii=True),
        )
        sess = WatchSession(
            watch_id=wid,
            pool_id=pool_id,
            network=network,
            token_address=token_address,
            address=address,
            score=score,
            entered_at=now,
            baseline=baseline,
            features=features,
            dwell_protected_until=now + timedelta(seconds=min_dwell),
            peak_price=baseline,
            last_price=baseline,
        )
        self.sessions[k] = sess
        funnel_add("watch", "_input")
        return sess

    def finish(
        self,
        sess: WatchSession,
        now: datetime,
        outcome: str,
        stats: ConfirmStats,
        funnel_add: Any,
    ) -> None:
        payload = {
            "buyers": stats.buyers,
            "sellers": stats.sellers,
            "buys": stats.buys,
            "sells": stats.sells,
            "buyer_seller_ratio": stats.buyer_seller_ratio,
            "buy_sell_ratio": stats.buy_sell_ratio,
            "price_change_pct": stats.price_change_pct,
            "max_buyers": stats.max_buyers,
            "max_buyer_seller_ratio": stats.max_buyer_seller_ratio,
            "max_buy_sell_ratio": stats.max_buy_sell_ratio,
            "max_price_change_pct": stats.max_price_change_pct,
            "actual_dwell_sec": (now - sess.entered_at).total_seconds(),
            "aborted_rule": stats.aborted_rule,
        }
        self.store.finish_watch(sess.watch_id, now.isoformat(), outcome, json.dumps(payload))
        if self.sessions.get(self.key(sess.network, sess.token_address)) is sess:
            self.sessions.pop(self.key(sess.network, sess.token_address), None)
        if outcome == "confirmed":
            funnel_add("watch", "_passed")
        elif outcome in {"timeout", "evicted"}:
            funnel_add("watch", outcome)

    def is_active(self, sess: WatchSession) -> bool:
        return self.sessions.get(self.key(sess.network, sess.token_address)) is sess

    def abort_all(self, now: datetime) -> None:
        for sess in list(self.sessions.values()):
            self.store.finish_watch(sess.watch_id, now.isoformat(), "aborted_shutdown", None)
        self.sessions.clear()
