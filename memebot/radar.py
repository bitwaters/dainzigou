"""Fetch due radar streams, page them, merge, and dedup by token."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from memebot.pool import PoolSnapshot, parse_pools
from memebot.store import Store

log = logging.getLogger("memebot.radar")

STREAM_NAMES = ("momentum", "trending_5m", "trending_1h")
_TRENDING_DURATION = {"trending_5m": "5m", "trending_1h": "1h"}


class RadarClient(Protocol):
    last_server_date: datetime | None

    async def megafilter(
        self,
        *,
        networks: list[str],
        pool_created_hour_min: float,
        pool_created_hour_max: float,
        reserve_in_usd_min: float,
        sort: str,
        page: int,
        include: str = "base_token",
        extra_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    async def trending_pools(
        self,
        *,
        duration: str,
        page: int,
        include: str = "base_token",
        include_gt_community_data: bool = True,
    ) -> dict[str, Any]: ...


@dataclass
class CollectResult:
    rows: list[PoolSnapshot]
    fetched_streams: list[str]
    failed_streams: list[str]
    any_success: bool
    last_server_date: datetime | None = None


def due_stream_names(
    raw: Mapping[str, Any],
    last_run: Mapping[str, datetime],
    now: datetime,
) -> list[str]:
    due: list[str] = []
    streams = raw["streams"]
    for name in STREAM_NAMES:
        cfg = streams[name]
        if not cfg.get("enabled"):
            continue
        interval = float(cfg["interval_sec"])
        prev = last_run.get(name)
        if prev is None or (now - prev).total_seconds() >= interval:
            due.append(name)
    return due


def filter_stream_rows(
    name: str,
    rows: list[PoolSnapshot],
    networks: set[str],
) -> list[PoolSnapshot]:
    if name == "momentum":
        return list(rows)
    return [row for row in rows if row.network in networks]


def dedup_rows(rows: list[PoolSnapshot]) -> list[PoolSnapshot]:
    best: dict[tuple[str, str], PoolSnapshot] = {}
    for row in rows:
        if not row.network or not row.token_address:
            continue
        key = (row.network, row.token_address)
        prev = best.get(key)
        if prev is None:
            best[key] = row
            continue
        prev_liq = prev.reserve_usd if prev.reserve_usd is not None else float("-inf")
        cur_liq = row.reserve_usd if row.reserve_usd is not None else float("-inf")
        if cur_liq > prev_liq:
            best[key] = row
    return list(best.values())


async def fetch_stream(
    client: RadarClient, raw: Mapping[str, Any], name: str
) -> list[PoolSnapshot]:
    pages = int(raw["streams"][name]["pages"])
    if name == "momentum":
        pre = raw["streams"]["momentum"]["prefilter"]
        extra = {
            "price_change_percentage_min": pre["price_change_percentage_min"],
            "price_change_percentage_duration": pre["price_change_percentage_duration"],
        }
        coros = [
            client.megafilter(
                networks=list(raw["networks"]),
                pool_created_hour_min=float(pre["pool_created_hour_min"]),
                pool_created_hour_max=float(pre["pool_created_hour_max"]),
                reserve_in_usd_min=float(pre["reserve_in_usd_min"]),
                sort=str(pre["sort"]),
                page=page,
                include="base_token",
                extra_params=extra,
            )
            for page in range(1, pages + 1)
        ]
    else:
        duration = _TRENDING_DURATION[name]
        coros = [
            client.trending_pools(duration=duration, page=page, include="base_token")
            for page in range(1, pages + 1)
        ]
    payloads = await asyncio.gather(*coros)
    rows: list[PoolSnapshot] = []
    for payload in payloads:
        if not isinstance(payload, dict):
            raise ValueError(f"{name}: expected object payload")
        rows.extend(parse_pools(payload, name))
    return rows


class Radar:
    def __init__(self, client: RadarClient, store: Store, raw: Mapping[str, Any]) -> None:
        self.client = client
        self.store = store
        self.raw = raw
        self.last_run: dict[str, datetime] = {}

    async def collect(self, now: datetime | None = None) -> CollectResult:
        now = now or datetime.now(UTC)
        due = due_stream_names(self.raw, self.last_run, now)
        if not due:
            return CollectResult(rows=[], fetched_streams=[], failed_streams=[], any_success=False)

        networks = {str(n) for n in self.raw["networks"]}
        fetched: list[str] = []
        failed: list[str] = []
        merged: list[PoolSnapshot] = []

        outcomes = await asyncio.gather(
            *[fetch_stream(self.client, self.raw, name) for name in due],
            return_exceptions=True,
        )
        for name, outcome in zip(due, outcomes, strict=True):
            if isinstance(outcome, BaseException):
                log.warning("stream %s failed: %s", name, outcome)
                failed.append(name)
                continue
            fetched.append(name)
            self.last_run[name] = now
            merged.extend(filter_stream_rows(name, outcome, networks))

        rows = dedup_rows(merged)
        if fetched:
            self.store.incr_step(now.date().isoformat(), "radar_input", len(rows))
        return CollectResult(
            rows=rows,
            fetched_streams=fetched,
            failed_streams=failed,
            any_success=bool(fetched),
            last_server_date=self.client.last_server_date,
        )
