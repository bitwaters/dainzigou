"""CoinGecko Onchain REST client: auth, retry, rate limit, credit accounting."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote

import httpx

from memebot.budget import Budget, BudgetExhausted
from memebot.config import AppConfig

log = logging.getLogger("memebot.cg")

PRO_BASE = "https://pro-api.coingecko.com/api/v3"
PRO_HEADER = "x-cg-pro-api-key"
DEMO_BASE = "https://api.coingecko.com/api/v3"
DEMO_HEADER = "x-cg-demo-api-key"
_SECONDS_PER_MINUTE = 60


def resolve_tier(tier: str) -> tuple[str, str]:
    if tier == "demo":
        return DEMO_BASE, DEMO_HEADER
    return PRO_BASE, PRO_HEADER


SleepFn = Callable[[float], Awaitable[None]]


class CgError(Exception):
    pass


class CgHttpError(CgError):
    def __init__(self, status: int, path: str, body: str) -> None:
        self.status = status
        self.path = path
        self.body = body
        super().__init__(f"{path} HTTP {status}: {body[:200]}")


class CgRetryExhausted(CgError):
    pass


@dataclass(frozen=True)
class CgSettings:
    timeout_sec: float
    connect_timeout_sec: float
    max_connections: int
    user_agent: str
    max_attempts: int
    backoff_base_sec: float
    backoff_factor: float
    backoff_max_sec: float
    max_requests_per_min: int

    @classmethod
    def from_app(cls, cfg: AppConfig) -> CgSettings:
        return cls(
            timeout_sec=float(cfg.get("runtime.http.timeout_sec")),
            connect_timeout_sec=float(cfg.get("runtime.http.connect_timeout_sec")),
            max_connections=int(cfg.get("runtime.http.max_connections")),
            user_agent=str(cfg.get("runtime.http.user_agent")),
            max_attempts=int(cfg.get("runtime.retry.max_attempts")),
            backoff_base_sec=float(cfg.get("runtime.retry.backoff_base_sec")),
            backoff_factor=float(cfg.get("runtime.retry.backoff_factor")),
            backoff_max_sec=float(cfg.get("runtime.retry.backoff_max_sec")),
            max_requests_per_min=int(cfg.get("runtime.rate_limit.max_requests_per_min")),
        )


class MinuteLimiter:
    def __init__(self, max_per_min: int, sleep: SleepFn) -> None:
        self.max_per_min = max_per_min
        self._sleep = sleep
        self._hits: deque[float] = deque()

    async def acquire(self) -> None:
        now = asyncio.get_running_loop().time()
        window = float(_SECONDS_PER_MINUTE)
        while self._hits and now - self._hits[0] >= window:
            self._hits.popleft()
        if len(self._hits) >= self.max_per_min:
            wait = window - (now - self._hits[0])
            if wait > 0:
                await self._sleep(wait)
            now = asyncio.get_running_loop().time()
            while self._hits and now - self._hits[0] >= window:
                self._hits.popleft()
        self._hits.append(asyncio.get_running_loop().time())


def retry_after_seconds(headers: httpx.Headers, fallback: float) -> float:
    raw = headers.get("retry-after")
    if raw is None:
        return fallback
    try:
        return max(0.0, float(raw))
    except ValueError:
        try:
            when = parsedate_to_datetime(raw)
            if when.tzinfo is None:
                when = when.replace(tzinfo=UTC)
            return max(0.0, (when - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError):
            return fallback


class CgClient:
    def __init__(
        self,
        api_key: str,
        settings: CgSettings,
        budget: Budget,
        *,
        base_url: str = PRO_BASE,
        auth_header: str = PRO_HEADER,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: SleepFn | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.settings = settings
        self.budget = budget
        self._sleep: SleepFn = sleep or asyncio.sleep
        self._limiter = MinuteLimiter(settings.max_requests_per_min, self._sleep)
        self.last_server_date: datetime | None = None
        timeout = httpx.Timeout(settings.timeout_sec, connect=settings.connect_timeout_sec)
        limits = httpx.Limits(max_connections=settings.max_connections)
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            limits=limits,
            headers={
                auth_header: api_key,
                "Accept": "application/json",
                "User-Agent": settings.user_agent,
            },
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> CgClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def request(
        self,
        path: str,
        kind: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.budget.remaining() <= 0:
            self.budget.record(kind)
        last_exc: Exception | None = None
        for attempt in range(self.settings.max_attempts):
            await self._limiter.acquire()
            try:
                self.budget.record(kind)
            except BudgetExhausted:
                raise
            try:
                resp = await self._client.get(path, params=params)
            except httpx.RequestError as exc:
                last_exc = exc
                wait = min(
                    self.settings.backoff_base_sec * (self.settings.backoff_factor**attempt),
                    self.settings.backoff_max_sec,
                )
                log.warning("network error %s %s; backoff %.1fs", path, exc, wait)
                await self._sleep(wait)
                continue
            if resp.status_code == 429:
                wait = retry_after_seconds(resp.headers, self.settings.backoff_base_sec)
                log.warning("%s 429 retry-after=%.1fs", path, wait)
                last_exc = CgHttpError(429, path, resp.text)
                await self._sleep(wait)
                continue
            if 500 <= resp.status_code <= 599:
                wait = min(
                    self.settings.backoff_base_sec * (self.settings.backoff_factor**attempt),
                    self.settings.backoff_max_sec,
                )
                log.warning("%s %s; backoff %.1fs", path, resp.status_code, wait)
                last_exc = CgHttpError(resp.status_code, path, resp.text)
                await self._sleep(wait)
                continue
            if 400 <= resp.status_code <= 499:
                raise CgHttpError(resp.status_code, path, resp.text)
            resp.raise_for_status()
            date_hdr = resp.headers.get("date")
            if date_hdr:
                try:
                    parsed = parsedate_to_datetime(date_hdr)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=UTC)
                    self.last_server_date = parsed
                except (TypeError, ValueError):
                    pass
            payload = resp.json()
            if not isinstance(payload, dict):
                raise CgError(f"{path}: expected object, got {type(payload).__name__}")
            return payload
        raise CgRetryExhausted(f"{path}: retries exhausted") from last_exc

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
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "networks": ",".join(networks),
            "pool_created_hour_min": pool_created_hour_min,
            "pool_created_hour_max": pool_created_hour_max,
            "reserve_in_usd_min": reserve_in_usd_min,
            "sort": sort,
            "page": page,
            "include": include,
        }
        if extra_params:
            params.update(extra_params)
        return await self.request("/onchain/pools/megafilter", "collect", params)

    async def trending_pools(
        self,
        *,
        duration: str,
        page: int,
        include: str = "base_token",
        include_gt_community_data: bool = True,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "duration": duration,
            "page": page,
            "include": include,
            "include_gt_community_data": str(include_gt_community_data).lower(),
        }
        return await self.request("/onchain/networks/trending_pools", "collect", params)

    async def trades(
        self,
        network: str,
        pool_address: str,
        *,
        trade_volume_in_usd_greater_than: float | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if trade_volume_in_usd_greater_than is not None:
            params["trade_volume_in_usd_greater_than"] = trade_volume_in_usd_greater_than
        path = (
            f"/onchain/networks/{quote(network, safe='')}"
            f"/pools/{quote(pool_address, safe='')}/trades"
        )
        return await self.request(path, "trades", params or None)

    async def token_ohlcv(
        self,
        network: str,
        address: str,
        timeframe: str,
        *,
        aggregate: int,
        include_empty_intervals: bool = True,
        limit: int | None = None,
        currency: str = "usd",
        before_timestamp: int | None = None,
    ) -> dict[str, Any]:
        path = (
            f"/onchain/networks/{quote(network, safe='')}/tokens/"
            f"{quote(address, safe='')}/ohlcv/{quote(timeframe, safe='')}"
        )
        params: dict[str, Any] = {
            "aggregate": aggregate,
            "include_empty_intervals": str(include_empty_intervals).lower(),
            "currency": currency,
        }
        if limit is not None:
            params["limit"] = limit
        if before_timestamp is not None:
            params["before_timestamp"] = before_timestamp
        return await self.request(path, "ohlcv", params)

    async def pool_ohlcv(
        self,
        network: str,
        pool_address: str,
        timeframe: str,
        *,
        aggregate: int,
        include_empty_intervals: bool = True,
        limit: int | None = None,
        currency: str = "usd",
        before_timestamp: int | None = None,
    ) -> dict[str, Any]:
        path = (
            f"/onchain/networks/{quote(network, safe='')}/pools/"
            f"{quote(pool_address, safe='')}/ohlcv/{quote(timeframe, safe='')}"
        )
        params: dict[str, Any] = {
            "aggregate": aggregate,
            "include_empty_intervals": str(include_empty_intervals).lower(),
            "currency": currency,
        }
        if limit is not None:
            params["limit"] = limit
        if before_timestamp is not None:
            params["before_timestamp"] = before_timestamp
        return await self.request(path, "ohlcv", params)
