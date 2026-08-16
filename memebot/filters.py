"""Layer 0/1/2 filters. L0/L1 are pure; L2 may call CoinGecko."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from memebot.cg_client import CgClient, CgHttpError
from memebot.pool import PoolSnapshot, as_dict
from memebot.store import Store


def _disabled(value: Any) -> bool:
    return value is None


def source_gate(pool: PoolSnapshot, raw: dict[str, Any], key: str, fallback: Any) -> Any:
    stream = (raw.get("streams") or {}).get(pool.source)
    if isinstance(stream, dict) and stream.get(key) is not None:
        return stream[key]
    return fallback


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _req_float(value: Any) -> float:
    return float(value)


@dataclass
class FilterResult:
    pool: PoolSnapshot
    rejected_rule: str | None = None

    @property
    def passed(self) -> bool:
        return self.rejected_rule is None


class Funnel:
    def __init__(self, store: Store, now: datetime | None = None) -> None:
        self.store = store
        self.day = (now or datetime.now(UTC)).date().isoformat()

    def add(self, layer: str, rule: str, n: int = 1) -> None:
        if n:
            self.store.incr_funnel(self.day, layer, rule, n)

    def add_src(self, layer: str, rule: str, source: str, n: int = 1) -> None:
        self.add(layer, rule, n)
        if source:
            self.add(layer, f"{source}:{rule}", n)

    def add_once(self, layer: str, rule: str, pool_id: str, source: str) -> None:
        if source:
            self.add(layer, f"{source}:{rule}")
        if self.store.funnel_seen_once(self.day, layer, rule, pool_id):
            self.add(layer, rule)


def record_symbols(store: Store, pools: list[PoolSnapshot], now: datetime) -> None:
    bucket = now.astimezone(UTC).strftime("%Y-%m-%dT%H")
    for pool in pools:
        if pool.symbol_norm:
            store.incr_symbol(pool.network, pool.symbol_norm, bucket, pool.token_address)


def eval_l0(pool: PoolSnapshot, raw: dict[str, Any], store: Store, now: datetime) -> FilterResult:
    gates = raw["collection_gates"]
    networks = {str(n) for n in raw["networks"]}
    if pool.network not in networks:
        return FilterResult(pool, "network_whitelist")
    quotes = {a.lower() for a in gates["quote_tokens"].get(pool.network, [])}
    if pool.quote_address.lower() not in quotes:
        return FilterResult(pool, "quote_whitelist")
    min_reserve = gates.get("min_reserve_usd")
    if not _disabled(min_reserve) and (
        pool.reserve_usd is None or pool.reserve_usd < float(min_reserve)
    ):
        return FilterResult(pool, "min_reserve_usd")
    max_fdv = source_gate(pool, raw, "max_fdv_usd", gates.get("max_fdv_usd"))
    if not _disabled(max_fdv) and pool.fdv_usd is not None and pool.fdv_usd > float(max_fdv):
        return FilterResult(pool, "max_fdv_usd")
    ratio_cap = gates.get("max_fdv_to_reserve")
    if (
        not _disabled(ratio_cap)
        and pool.fdv_usd is not None
        and pool.reserve_usd
        and pool.reserve_usd > 0
        and pool.fdv_usd / pool.reserve_usd > float(ratio_cap)
    ):
        return FilterResult(pool, "max_fdv_to_reserve")
    turn = gates.get("max_turnover_ratio") or {}
    turn_max = turn.get("max")
    if not _disabled(turn_max) and pool.reserve_usd and pool.reserve_usd > 0:
        window = str(turn.get("window") or "m15")
        vol = pool.volume.get(window)
        if vol is not None and vol / pool.reserve_usd > _req_float(turn_max):
            return FilterResult(pool, "max_turnover_ratio")
    wash = gates.get("anti_wash") or {}
    window = str(wash.get("window") or "m15")
    tx = pool.tx.get(window) or {}
    buys = tx.get("buys")
    buyers = tx.get("buyers")
    min_ratio = wash.get("min_buyers_to_buys")
    if not _disabled(min_ratio) and buys and buys > 0 and buyers is not None:
        if buyers / buys < _req_float(min_ratio):
            return FilterResult(pool, "min_buyers_to_buys")
    min_buyers = wash.get("min_buyers_m15")
    if not _disabled(min_buyers) and (buyers is None or buyers < _req_float(min_buyers)):
        return FilterResult(pool, "min_buyers_m15")
    copycat = gates.get("copycat") or {}
    enabled = copycat.get("enabled") is True
    max_same = copycat.get("max_same_symbol")
    if enabled and not _disabled(max_same) and pool.symbol_norm:
        lookback_h = float(copycat.get("lookback_h") or 0)
        min_bucket = (now.astimezone(UTC) - timedelta(hours=lookback_h)).strftime("%Y-%m-%dT%H")
        cnt = store.symbol_count(pool.network, pool.symbol_norm, min_bucket)
        if cnt > _req_float(max_same):
            return FilterResult(pool, "copycat")
    max_sus = gates.get("max_sus_reports")
    if (
        not _disabled(max_sus)
        and pool.sus_reports is not None
        and pool.sus_reports > float(max_sus)
    ):
        return FilterResult(pool, "max_sus_reports")
    return FilterResult(pool)


def eval_l1(pool: PoolSnapshot, raw: dict[str, Any], now: datetime) -> FilterResult:
    gates = raw["business_gates"]
    min_age = gates.get("min_age_min")
    max_age_h = source_gate(pool, raw, "max_age_h", gates.get("max_age_h"))
    if pool.pool_created_at is None:
        return FilterResult(pool, "missing_pool_created_at")
    age_min = (now.astimezone(UTC) - pool.pool_created_at.astimezone(UTC)).total_seconds() / 60
    if not _disabled(min_age) and age_min < float(min_age):
        return FilterResult(pool, "min_age_min")
    if not _disabled(max_age_h) and age_min > float(max_age_h) * 60:
        return FilterResult(pool, "max_age_h")
    vol_cfg = gates.get("min_volume") or {}
    usd = vol_cfg.get("usd")
    if not _disabled(usd):
        window = str(vol_cfg.get("window") or "m15")
        vol = pool.volume.get(window)
        if vol is None or vol < _req_float(usd):
            return FilterResult(pool, "min_volume")
    return FilterResult(pool)


def apply_layer(
    layer: str,
    results: list[FilterResult],
    funnel: Funnel,
) -> list[PoolSnapshot]:
    passed: list[PoolSnapshot] = []
    for item in results:
        src = item.pool.source
        pid = item.pool.pool_id
        funnel.add_once(layer, "_input", pid, src)
        if item.passed:
            passed.append(item.pool)
            funnel.add_once(layer, "_passed", pid, src)
        elif item.rejected_rule:
            funnel.add_once(layer, item.rejected_rule, pid, src)
    return passed


def run_l0_l1(
    pools: list[PoolSnapshot],
    raw: dict[str, Any],
    store: Store,
    now: datetime,
    funnel: Funnel,
) -> list[PoolSnapshot]:
    record_symbols(store, pools, now)
    l0 = apply_layer("l0", [eval_l0(p, raw, store, now) for p in pools], funnel)
    l1 = apply_layer("l1", [eval_l1(p, raw, now) for p in l0], funnel)
    return l1


def _cache_fresh(checked_at: str, minutes: float | None, now: datetime) -> bool:
    if minutes is None:
        return True
    ts = datetime.fromisoformat(checked_at)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return now.astimezone(UTC) - ts.astimezone(UTC) < timedelta(minutes=minutes)


def _l2_once(funnel: Funnel, layer: str, rule: str, pool: PoolSnapshot) -> None:
    funnel.add_once(layer, rule, pool.pool_id, pool.source)


async def run_l2a(
    pools: list[PoolSnapshot],
    raw: dict[str, Any],
    store: Store,
    client: CgClient,
    now: datetime,
    funnel: Funnel,
) -> list[PoolSnapshot]:
    sec = raw["security"]
    batch_cfg = sec["batch"]
    cache_cfg = batch_cfg["cache"]
    min_share = float(batch_cfg["min_main_pool_share"])
    reject_ungrad = bool(sec.get("reject_ungraduated"))
    size = int(batch_cfg["size"])
    passed: list[PoolSnapshot] = []
    need: dict[str, list[PoolSnapshot]] = {}
    for pool in pools:
        _l2_once(funnel, "l2a", "_input", pool)
        row = store.get_token_batch(pool.network, pool.token_address)
        use_cache = False
        if row is not None:
            flag = int(row["graduated"])
            is_graduated = flag == 1
            share_ttl = cache_cfg.get("main_pool_share_ttl_min")
            ungrad_ttl = cache_cfg.get("ungraduated_recheck_min")
            grad_ttl = cache_cfg.get("graduated_ttl_min")
            share_fresh = _cache_fresh(
                str(row["checked_at"]), None if share_ttl is None else float(share_ttl), now
            )
            grad_fresh = _cache_fresh(
                str(row["checked_at"]), None if grad_ttl is None else float(grad_ttl), now
            )
            if is_graduated and grad_fresh and share_fresh:
                use_cache = True
            elif not is_graduated and _cache_fresh(
                str(row["checked_at"]), None if ungrad_ttl is None else float(ungrad_ttl), now
            ):
                use_cache = True
        if use_cache and row is not None:
            reason = _l2a_from_row(row, min_share, reject_ungrad)
            if reason:
                _l2_once(funnel, "l2a", reason, pool)
            else:
                passed.append(pool)
                _l2_once(funnel, "l2a", "_passed", pool)
        else:
            need.setdefault(pool.network, []).append(pool)
    for network, group in need.items():
        for i in range(0, len(group), size):
            chunk = group[i : i + size]
            addrs = [p.token_address for p in chunk]
            payload = await client.tokens_multi(network, addrs)
            by_addr = _index_tokens(payload)
            for pool in chunk:
                token = by_addr.get(pool.token_address.lower())
                if token is None:
                    _l2_once(funnel, "l2a", "tokens_multi_missing", pool)
                    continue
                attrs = as_dict(token.get("attributes"))
                launch = as_dict(attrs.get("launchpad_details"))
                completed = launch.get("completed")
                total = _as_float(attrs.get("total_reserve_in_usd"))
                share = None
                if total and total > 0 and pool.reserve_usd is not None:
                    share = pool.reserve_usd / total
                grad_state: bool | None
                if completed is True:
                    grad_state = True
                elif completed is False:
                    grad_state = False
                else:
                    grad_state = None
                store.put_token_batch(
                    network,
                    pool.token_address,
                    graduated=grad_state,
                    checked_at=now.astimezone(UTC).isoformat(),
                    graduation_pct=_as_float(launch.get("graduation_percentage")),
                    total_reserve_usd=total,
                    main_pool_share=share,
                )
                if reject_ungrad and completed is False:
                    _l2_once(funnel, "l2a", "ungraduated", pool)
                    continue
                if share is None or share < min_share:
                    _l2_once(funnel, "l2a", "min_main_pool_share", pool)
                    continue
                passed.append(pool)
                _l2_once(funnel, "l2a", "_passed", pool)
    return passed


def _l2a_from_row(row: Any, min_share: float, reject_ungrad: bool) -> str | None:
    if reject_ungrad and int(row["graduated"]) == 0:
        return "ungraduated"
    share = row["main_pool_share"]
    if share is None or float(share) < min_share:
        return "min_main_pool_share"
    return None


def _index_tokens(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        attrs = as_dict(item.get("attributes"))
        addr = str(attrs.get("address") or "")
        if addr:
            out[addr.lower()] = item
    return out


def _parse_security_info(raw: str) -> dict[str, Any]:
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def l2b_card_fields(info: dict[str, Any], network: str) -> dict[str, Any]:
    data = as_dict(info.get("data")) if isinstance(info.get("data"), dict) else info
    attrs = as_dict(data.get("attributes") if isinstance(data, dict) else None)
    out: dict[str, Any] = {}
    holders = as_dict(attrs.get("holders"))
    count = _as_float(holders.get("count"))
    if count is not None:
        out["holders"] = count
    gt = attrs.get("gt_score")
    if isinstance(gt, dict):
        gt_n = _as_float(gt.get("score") or gt.get("total"))
    else:
        gt_n = _as_float(gt)
    if gt_n is not None:
        out["gt_score"] = gt_n
    if attrs.get("mint_authority") is not None:
        out["mint_authority"] = attrs.get("mint_authority")
    if attrs.get("freeze_authority") is not None:
        out["freeze_authority"] = attrs.get("freeze_authority")
    if attrs.get("is_honeypot") is not None:
        out["honeypot"] = attrs.get("is_honeypot")
    return out


def _remember_card(
    card_fields: dict[tuple[str, str], dict[str, Any]] | None,
    pool: PoolSnapshot,
    info: dict[str, Any],
) -> None:
    if card_fields is None:
        return
    fields = l2b_card_fields(info, pool.network)
    if fields:
        card_fields[(pool.network, pool.token_address)] = fields


async def run_l2b(
    pools: list[PoolSnapshot],
    raw: dict[str, Any],
    store: Store,
    client: CgClient,
    now: datetime,
    funnel: Funnel,
    card_fields: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> list[PoolSnapshot]:
    sec = raw["security"]
    cache_h = float(sec["cache_hours"])
    passed: list[PoolSnapshot] = []
    for pool in pools:
        _l2_once(funnel, "l2b", "_input", pool)
        cached = store.get_security(pool.network, pool.token_address)
        if cached is not None and _cache_fresh(str(cached["checked_at"]), cache_h * 60, now):
            cached_ok = int(cached["passed"])
            if cached_ok:
                passed.append(pool)
                _l2_once(funnel, "l2b", "_passed", pool)
                _remember_card(
                    card_fields, pool, _parse_security_info(str(cached["result_json"]))
                )
            else:
                _l2_once(funnel, "l2b", "cached_reject", pool)
            continue
        try:
            info = await client.token_info(pool.network, pool.token_address)
        except CgHttpError as exc:
            if exc.status == 404:
                _l2_once(funnel, "l2b", "token_info_404", pool)
                store.put_security(pool.network, pool.token_address, "{}", False, now.isoformat())
                continue
            raise
        ok, rule = eval_l2b_info(info, pool.network, sec, now, funnel, source=pool.source)
        store.put_security(
            pool.network,
            pool.token_address,
            json.dumps(info, ensure_ascii=True),
            ok,
            now.astimezone(UTC).isoformat(),
        )
        if ok:
            passed.append(pool)
            _l2_once(funnel, "l2b", "_passed", pool)
            _remember_card(card_fields, pool, info)
        elif rule:
            _l2_once(funnel, "l2b", rule, pool)
    return passed


def eval_l2b_info(
    info: dict[str, Any],
    network: str,
    sec: dict[str, Any],
    now: datetime,
    funnel: Funnel | None,
    source: str | None = None,
) -> tuple[bool, str | None]:
    data = as_dict(info.get("data")) if isinstance(info.get("data"), dict) else info
    attrs = as_dict(data.get("attributes") if isinstance(data, dict) else None)
    rules = (sec.get("rules") or {}).get(network) or {}
    honeypot = attrs.get("is_honeypot")
    policy = str(sec.get("honeypot_policy") or "off")
    if policy == "reject_true" and honeypot is True:
        return False, "honeypot"
    if policy == "reject_unknown" and honeypot not in {False, "no"}:
        return False, "honeypot_unknown"
    for field, expected in (("mint_authority", "no"), ("freeze_authority", "no")):
        if field in rules and attrs.get(field) != expected:
            return False, field
    holders = as_dict(attrs.get("holders"))
    unknown = sec.get("unknown_policy") or {}
    stale_h = sec.get("holders_max_staleness_h")
    last = holders.get("last_updated") if holders else None
    holders_unknown = not holders
    if last and not _disabled(stale_h):
        ts = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
        if now.astimezone(UTC) - ts.astimezone(UTC) > timedelta(hours=_req_float(stale_h)):
            holders_unknown = True

    def _unknown(field: str, rule: str) -> tuple[bool, str | None] | None:
        mode = unknown.get(field) or "pass_and_log"
        if funnel is not None:
            if source:
                funnel.add_src("l2b", f"missing_{rule}", source)
            else:
                funnel.add("l2b", f"missing_{rule}")
        if mode == "reject":
            return False, f"missing_{rule}"
        return None

    if "max_dev_holding_pct" in rules:
        dev = _as_float(attrs.get("developer_holding_percentage"))
        if dev is None or holders_unknown:
            hit = _unknown("dev_holding", "dev_holding")
            if hit:
                return hit
        elif dev > float(rules["max_dev_holding_pct"]):
            return False, "max_dev_holding_pct"
    dist = as_dict(holders.get("distribution_percentage"))
    if "max_top10_holding_pct" in rules:
        top10 = _as_float(dist.get("top_10"))
        if top10 is None or holders_unknown:
            hit = _unknown("top10_holding", "top10_holding")
            if hit:
                return hit
        elif top10 > float(rules["max_top10_holding_pct"]):
            return False, "max_top10_holding_pct"
    if "min_holders" in rules:
        count = _as_float(holders.get("count"))
        if count is None or holders_unknown:
            hit = _unknown("holders_count", "holders_count")
            if hit:
                return hit
        elif count < float(rules["min_holders"]):
            return False, "min_holders"
    return True, None
