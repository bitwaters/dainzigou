"""One collection cycle: merge streams → funnel → filter → score → candidates."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from memebot.cg_client import CgClient, CgError
from memebot.filters import Funnel, run_l0_l1, run_l2a, run_l2b
from memebot.pool import PoolSnapshot, parse_pools, parse_ts
from memebot.scoring import pick_top_n, score_pool
from memebot.store import Store
from memebot.watch import Watcher, cooldown_active

QUEUE_KEY = "new_pools_queue"


def m15_admit_ok(pool: PoolSnapshot, raw: dict[str, Any]) -> bool:
    floor = ((raw.get("watch") or {}).get("admit") or {}).get("min_m15_pct")
    if floor is None:
        return True
    chg = (pool.price_change_usd or {}).get("m15")
    if chg is None:
        return False
    try:
        return float(chg) >= float(floor)
    except (TypeError, ValueError):
        return False


def drop_recently_seen(
    pools: list[PoolSnapshot],
    recent: dict[str, datetime],
    now: datetime,
    ttl_sec: float,
) -> list[PoolSnapshot]:
    if ttl_sec <= 0:
        return pools
    cutoff = now - timedelta(seconds=ttl_sec)
    for pid, seen in list(recent.items()):
        if seen < cutoff:
            del recent[pid]
    out: list[PoolSnapshot] = []
    for pool in pools:
        last = recent.get(pool.pool_id)
        if last is not None and last >= cutoff:
            continue
        recent[pool.pool_id] = now
        out.append(pool)
    return out


def merge_cycle(
    batches: list[tuple[str, dict[str, Any]]], networks: set[str]
) -> list[PoolSnapshot]:
    seen: set[str] = set()
    out: list[PoolSnapshot] = []
    for source, payload in batches:
        for pool in parse_pools(payload, source):
            if pool.network not in networks:
                continue
            if pool.pool_id in seen:
                continue
            seen.add(pool.pool_id)
            out.append(pool)
    return out


def _age_min(pool: PoolSnapshot, now: datetime) -> float | None:
    if pool.pool_created_at is None:
        return None
    return (now.astimezone(UTC) - pool.pool_created_at.astimezone(UTC)).total_seconds() / 60


def _pool_record(pool: PoolSnapshot) -> dict[str, Any]:
    return {
        "network": pool.network,
        "pool_id": pool.pool_id,
        "address": pool.address,
        "token_address": pool.token_address,
        "quote_address": pool.quote_address,
        "symbol": pool.symbol,
        "name": pool.name,
        "source": pool.source,
        "pool_created_at": pool.pool_created_at.isoformat() if pool.pool_created_at else None,
        "reserve_usd": pool.reserve_usd,
        "fdv_usd": pool.fdv_usd,
        "price_usd": pool.price_usd,
        "price_native": pool.price_native,
        "volume": pool.volume,
        "tx": pool.tx,
        "price_change_usd": pool.price_change_usd,
        "price_change_native": pool.price_change_native,
        "sus_reports": pool.sus_reports,
    }


def _pool_from_record(rec: dict[str, Any]) -> PoolSnapshot:
    created = parse_ts(rec.get("pool_created_at"))
    return PoolSnapshot(
        network=str(rec["network"]),
        pool_id=str(rec["pool_id"]),
        address=str(rec["address"]),
        token_address=str(rec["token_address"]),
        quote_address=str(rec.get("quote_address") or ""),
        symbol=str(rec.get("symbol") or ""),
        name=str(rec.get("name") or ""),
        source=str(rec.get("source") or "new_pools"),
        pool_created_at=created,
        reserve_usd=rec.get("reserve_usd"),
        fdv_usd=rec.get("fdv_usd"),
        price_usd=rec.get("price_usd"),
        price_native=rec.get("price_native"),
        volume=dict(rec.get("volume") or {}),
        tx=dict(rec.get("tx") or {}),
        price_change_usd=dict(rec.get("price_change_usd") or {}),
        price_change_native=dict(rec.get("price_change_native") or {}),
        sus_reports=rec.get("sus_reports"),
    )


def apply_maturation_queue(
    store: Store,
    pools: list[PoolSnapshot],
    raw: dict[str, Any],
    now: datetime,
) -> list[PoolSnapshot]:
    qcfg = raw["streams"]["new_pools"]["maturation_queue"]
    if not qcfg.get("enabled"):
        return pools
    min_age = raw["business_gates"].get("min_age_min")
    max_size = int(qcfg["max_queue_size"])
    batch = int(qcfg["recheck_batch_size"])
    raw_q = store.kv_get(QUEUE_KEY)
    queue: list[dict[str, Any]] = json.loads(raw_q) if raw_q else []
    ready: list[PoolSnapshot] = []
    for pool in pools:
        age = _age_min(pool, now)
        if min_age is not None and (age is None or age < float(min_age)):
            if len(queue) < max_size:
                queue.append(_pool_record(pool))
            continue
        ready.append(pool)
    still: list[dict[str, Any]] = []
    released = 0
    for rec in queue:
        pool = _pool_from_record(rec)
        age = _age_min(pool, now)
        if min_age is None or (age is not None and age >= float(min_age) and released < batch):
            ready.append(pool)
            released += 1
        else:
            still.append(rec)
    store.kv_set(QUEUE_KEY, json.dumps(still[:max_size], ensure_ascii=True))
    return ready


async def collect_stream(
    client: CgClient, raw: dict[str, Any], stream: str
) -> list[tuple[str, dict[str, Any]]]:
    nets = list(raw["networks"])
    if stream == "megafilter":
        pre = raw["streams"]["megafilter"]["prefilter"]
        return [
            (
                "megafilter",
                await client.megafilter(
                    networks=nets,
                    pool_created_hour_min=float(pre["pool_created_hour_min"]),
                    pool_created_hour_max=float(pre["pool_created_hour_max"]),
                    reserve_in_usd_min=float(pre["reserve_in_usd_min"]),
                    sort=str(pre["sort"]),
                    page=int(raw["streams"]["megafilter"]["pages"]),
                ),
            )
        ]
    if stream == "new_pools":
        return [
            ("new_pools", await client.new_pools(page=int(raw["streams"]["new_pools"]["pages"])))
        ]
    if stream == "trending_5m":
        return [
            (
                "trending_5m",
                await client.trending_pools(
                    duration=str(raw["streams"]["trending_5m"]["duration"]), page=1
                ),
            )
        ]
    if stream == "trending_1h":
        return [
            (
                "trending_1h",
                await client.trending_pools(
                    duration=str(raw["streams"]["trending_1h"]["duration"]), page=1
                ),
            )
        ]
    raise ValueError(f"unknown stream: {stream}")


async def collect_batches(
    client: CgClient, raw: dict[str, Any]
) -> list[tuple[str, dict[str, Any]]]:
    batches: list[tuple[str, dict[str, Any]]] = []
    source = str(raw["streams"]["source"])
    if source == "megafilter" and raw["streams"]["megafilter"]["enabled"]:
        batches.extend(await collect_stream(client, raw, "megafilter"))
    elif source == "new_pools" and raw["streams"]["new_pools"]["enabled"]:
        batches.extend(await collect_stream(client, raw, "new_pools"))
    if raw["streams"]["trending_5m"]["enabled"]:
        batches.extend(await collect_stream(client, raw, "trending_5m"))
    if raw["streams"]["trending_1h"]["enabled"]:
        batches.extend(await collect_stream(client, raw, "trending_1h"))
    return batches


async def run_cycle(
    *,
    client: CgClient,
    store: Store,
    raw: dict[str, Any],
    watcher: Watcher,
    now: datetime,
    config_hash: str,
) -> list[PoolSnapshot]:
    funnel = Funnel(store, now)
    try:
        batches = await collect_batches(client, raw)
    except CgError:
        return []
    return await process_batches(
        batches=batches,
        client=client,
        store=store,
        raw=raw,
        watcher=watcher,
        now=now,
        config_hash=config_hash,
        funnel=funnel,
    )


async def process_batches(
    *,
    batches: list[tuple[str, dict[str, Any]]],
    client: CgClient,
    store: Store,
    raw: dict[str, Any],
    watcher: Watcher,
    now: datetime,
    config_hash: str,
    funnel: Funnel | None = None,
    recent_ids: dict[str, datetime] | None = None,
    recent_ttl_sec: float = 0.0,
) -> list[PoolSnapshot]:
    funnel = funnel or Funnel(store, now)
    networks = {str(n) for n in raw["networks"]}
    pools = merge_cycle(batches, networks)
    if str(raw["streams"]["source"]) == "new_pools":
        pools = apply_maturation_queue(store, pools, raw, now)
    if recent_ids is not None:
        pools = drop_recently_seen(pools, recent_ids, now, recent_ttl_sec)
    funnel.add("stream", "_raw", len(pools))
    survivors = run_l0_l1(pools, raw, store, now, funnel)
    survivors = await run_l2a(survivors, raw, store, client, now, funnel)
    card_fields: dict[tuple[str, str], dict[str, Any]] = {}
    survivors = await run_l2b(
        survivors, raw, store, client, now, funnel, card_fields=card_fields
    )
    seen_at = now.isoformat()
    for pool in survivors:
        store.upsert_pool(
            pool.network,
            pool.pool_id,
            pool.address,
            pool.token_address,
            seen_at,
            seen_at,
            symbol=pool.symbol,
        )
    scored: list[tuple[PoolSnapshot, float, dict[str, Any]]] = []
    for pool in survivors:
        total, feats = score_pool(pool, raw, now)
        extra = card_fields.get((pool.network, pool.token_address))
        if extra:
            feats.update(extra)
        scored.append((pool, total, feats))
    n = int(raw["scoring"]["candidates_per_chain_per_cycle"])
    funnel.add("scoring", "_input", len(scored))
    if ((raw.get("watch") or {}).get("admit") or {}).get("min_m15_pct") is not None:
        green: list[tuple[PoolSnapshot, float, dict[str, Any]]] = []
        red_n = 0
        for item in scored:
            if m15_admit_ok(item[0], raw):
                green.append(item)
            else:
                red_n += 1
        funnel.add("scoring", "m15_not_green", red_n)
        scored = green
    picked, rest = pick_top_n(scored, n)
    funnel.add("scoring", "_passed", len(picked))
    funnel.add("scoring", "not_top_n", len(rest))
    day = now.date().isoformat()
    watch_calls = store.kind_calls(day, "watch")
    for pool, score, features in picked:
        if store.has_live_signal(pool.network, pool.token_address, "confirmed"):
            continue
        if cooldown_active(
            store,
            pool.network,
            pool.token_address,
            max_timeouts=int(raw["watch"]["timeout_cooldown"]["max_timeouts"]),
            cooldown_h=float(raw["watch"]["timeout_cooldown"]["cooldown_h"]),
            now=now,
        ):
            continue
        if not watcher.can_add(now, watch_calls):
            break
        if pool.price_usd is None:
            continue
        watcher.admit(
            pool_id=pool.pool_id,
            network=pool.network,
            token_address=pool.token_address,
            address=pool.address,
            score=score,
            baseline=pool.price_usd,
            features=features,
            now=now,
            funnel_add=funnel.add,
        )
    store.kv_set("last_collection_ok_at", now.isoformat())
    return survivors
