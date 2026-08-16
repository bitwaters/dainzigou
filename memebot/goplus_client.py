"""GoPlus token security: batch fetch, three-state mapping, TTL cache."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import httpx

from memebot.config import AppConfig
from memebot.store import Store

log = logging.getLogger("memebot.goplus")

BASE_URL = "https://api.gopluslabs.io"
TOKEN_PATH = "/api/v1/token"
ENDPOINTS = {
    "bsc": "/api/v1/token_security/56",
    "solana": "/api/v1/solana/token_security",
}

Status = Literal["pass", "reject", "transient"]
CreditFn = Callable[[], None]


@dataclass(frozen=True)
class SecurityVerdict:
    status: Status
    mintable: bool | None = None
    freezable: bool | None = None
    reason: str | None = None


@dataclass(frozen=True)
class GoPlusSettings:
    timeout_sec: float
    batch_size: int
    cache_ttl_min: float
    transient_ttl_sec: float
    max_tax_pct: float

    @classmethod
    def from_app(cls, cfg: AppConfig) -> GoPlusSettings:
        return cls(
            timeout_sec=float(cfg.get("security.timeout_sec")),
            batch_size=int(cfg.get("security.batch_size")),
            cache_ttl_min=float(cfg.get("security.cache_ttl_min")),
            transient_ttl_sec=float(cfg.get("security.transient_ttl_sec")),
            max_tax_pct=float(cfg.get("security.max_tax_pct")),
        )


def sign_access_token(app_key: str, ts: int, app_secret: str) -> str:
    return hashlib.sha1(f"{app_key}{ts}{app_secret}".encode()).hexdigest()


def _flag(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    return str(value).strip() == "1"


def _authority_on(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, dict):
        if "status" not in value:
            return None
        return str(value["status"]).strip() == "1"
    return _flag(value)


def _tax(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_item(result: dict[str, Any], network: str, address: str) -> dict[str, Any] | None:
    item = result.get(address)
    if isinstance(item, dict):
        return item
    if network == "bsc":
        lowered = address.lower()
        item = result.get(lowered)
        if isinstance(item, dict):
            return item
        for key, node in result.items():
            if isinstance(key, str) and key.lower() == lowered and isinstance(node, dict):
                return node
    return None


def map_solana(item: dict[str, Any]) -> SecurityVerdict:
    closable = _authority_on(item.get("closable"))
    mutable = _authority_on(item.get("balance_mutable_authority"))
    default_state = item.get("default_account_state")
    if closable is None or mutable is None or default_state is None or default_state == "":
        return SecurityVerdict("reject", reason="missing_field")
    if closable:
        return SecurityVerdict("reject", reason="closable")
    if mutable:
        return SecurityVerdict("reject", reason="balance_mutable")
    if str(default_state).strip() == "2":
        return SecurityVerdict("reject", reason="default_frozen")
    mintable = _authority_on(item.get("mintable"))
    freezable = _authority_on(item.get("freezable"))
    return SecurityVerdict(
        "pass",
        mintable=bool(mintable),
        freezable=bool(freezable),
    )


def map_bsc(item: dict[str, Any], max_tax_pct: float) -> SecurityVerdict:
    honeypot = _flag(item.get("is_honeypot"))
    pausable = _flag(item.get("transfer_pausable"))
    mintable = _flag(item.get("is_mintable"))
    owner_chg = _flag(item.get("owner_change_balance"))
    buy_tax = _tax(item.get("buy_tax"))
    sell_tax = _tax(item.get("sell_tax"))
    if None in (honeypot, pausable, mintable, owner_chg, buy_tax, sell_tax):
        return SecurityVerdict("reject", reason="missing_field")
    # GoPlus omits cannot_sell_all when the contract has no such function.
    cannot_sell = item.get("cannot_sell_all")
    if cannot_sell is not None and cannot_sell != "":
        flag = _flag(cannot_sell)
        if flag is None:
            return SecurityVerdict("reject", reason="missing_field")
        if flag:
            return SecurityVerdict("reject", reason="cannot_sell_all")
    if honeypot:
        return SecurityVerdict("reject", reason="honeypot")
    if pausable:
        return SecurityVerdict("reject", reason="transfer_pausable")
    assert buy_tax is not None and sell_tax is not None
    if buy_tax >= max_tax_pct or sell_tax >= max_tax_pct:
        return SecurityVerdict("reject", reason="tax")
    if mintable:
        return SecurityVerdict("reject", reason="mintable")
    if owner_chg:
        return SecurityVerdict("reject", reason="owner_change_balance")
    return SecurityVerdict("pass")


def map_address(
    network: str,
    address: str,
    payload: dict[str, Any],
    max_tax_pct: float,
) -> SecurityVerdict:
    result = payload.get("result")
    if not isinstance(result, dict):
        return SecurityVerdict("reject", reason="not_in_result")
    item = _pick_item(result, network, address)
    if item is None:
        return SecurityVerdict("reject", reason="not_in_result")
    if network == "solana":
        return map_solana(item)
    return map_bsc(item, max_tax_pct)


def verdict_from_row(row: Any) -> SecurityVerdict:
    mintable = None if row["mintable"] is None else bool(row["mintable"])
    freezable = None if row["freezable"] is None else bool(row["freezable"])
    return SecurityVerdict(str(row["status"]), mintable=mintable, freezable=freezable)  # type: ignore[arg-type]


class GoPlusClient:
    def __init__(
        self,
        app_key: str,
        app_secret: str,
        settings: GoPlusSettings,
        store: Store,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        record_credit: CreditFn | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self._app_key = app_key
        self._app_secret = app_secret
        self._record_credit = record_credit
        self._token: str | None = None
        self._token_deadline = 0.0
        self._token_lock = asyncio.Lock()
        timeout = httpx.Timeout(settings.timeout_sec)
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "memebot/1.0",
            },
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> GoPlusClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def cached(self, network: str, address: str) -> SecurityVerdict | None:
        row = self.store.get_security(network, address)
        if row is None:
            return None
        return verdict_from_row(row)

    def _remember(self, network: str, address: str, verdict: SecurityVerdict) -> None:
        now = datetime.now(UTC)
        if verdict.status == "transient":
            expires = now + timedelta(seconds=self.settings.transient_ttl_sec)
        else:
            expires = now + timedelta(minutes=self.settings.cache_ttl_min)
        self.store.put_security(
            network,
            address,
            verdict.status,
            expires.isoformat(),
            mintable=verdict.mintable,
            freezable=verdict.freezable,
        )

    async def _ensure_token(self) -> str | None:
        now = time.monotonic()
        if self._token is not None and now < self._token_deadline:
            return self._token
        async with self._token_lock:
            now = time.monotonic()
            if self._token is not None and now < self._token_deadline:
                return self._token
            ts = int(time.time())
            body = {
                "app_key": self._app_key,
                "time": ts,
                "sign": sign_access_token(self._app_key, ts, self._app_secret),
            }
            try:
                resp = await self._client.post(TOKEN_PATH, json=body)
            except httpx.RequestError as exc:
                log.warning("goplus token network error: %s", exc)
                self._token = None
                return None
            if resp.status_code >= 400:
                log.warning("goplus token HTTP %s", resp.status_code)
                self._token = None
                return None
            try:
                payload = resp.json()
            except ValueError:
                log.warning("goplus token invalid json")
                self._token = None
                return None
            result = payload.get("result") if isinstance(payload, dict) else None
            token = result.get("access_token") if isinstance(result, dict) else None
            expires = result.get("expires_in") if isinstance(result, dict) else None
            if (
                not isinstance(payload, dict)
                or payload.get("code") != 1
                or not isinstance(token, str)
                or not token
            ):
                code = payload.get("code") if isinstance(payload, dict) else None
                log.warning("goplus token unexpected payload code=%s", code)
                self._token = None
                return None
            ttl = float(expires) if isinstance(expires, (int, float)) and expires > 0 else 3600.0
            skew = min(60.0, max(1.0, ttl * 0.1))
            self._token = token
            self._token_deadline = time.monotonic() + max(1.0, ttl - skew)
            return self._token

    async def _request(self, network: str, addresses: Sequence[str]) -> dict[str, Any] | None:
        token = await self._ensure_token()
        if token is None:
            return None
        path = ENDPOINTS[network]
        if self._record_credit is not None:
            self._record_credit()
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = await self._client.get(
                path,
                params={"contract_addresses": ",".join(addresses)},
                headers=headers,
            )
        except httpx.RequestError as exc:
            log.warning("goplus %s network error: %s", network, exc)
            return None
        if resp.status_code == 401:
            self._token = None
            self._token_deadline = 0.0
            token = await self._ensure_token()
            if token is None:
                return None
            try:
                resp = await self._client.get(
                    path,
                    params={"contract_addresses": ",".join(addresses)},
                    headers={"Authorization": f"Bearer {token}"},
                )
            except httpx.RequestError as exc:
                log.warning("goplus %s network error: %s", network, exc)
                return None
        if resp.status_code >= 400:
            log.warning("goplus %s HTTP %s", network, resp.status_code)
            return None
        try:
            payload = resp.json()
        except ValueError:
            log.warning("goplus %s invalid json", network)
            return None
        if not isinstance(payload, dict) or payload.get("code") != 1:
            code = payload.get("code") if isinstance(payload, dict) else None
            log.warning("goplus %s unexpected payload code=%s", network, code)
            return None
        return payload

    async def check_many(
        self, network: str, addresses: Sequence[str]
    ) -> dict[str, SecurityVerdict]:
        out: dict[str, SecurityVerdict] = {}
        need: list[str] = []
        for addr in addresses:
            hit = self.cached(network, addr)
            if hit is not None:
                out[addr] = hit
            else:
                need.append(addr)
        if not need:
            return out
        size = max(1, self.settings.batch_size)
        for i in range(0, len(need), size):
            batch = need[i : i + size]
            payload = await self._request(network, batch)
            if payload is None:
                for addr in batch:
                    verdict = SecurityVerdict("transient", reason="http")
                    self._remember(network, addr, verdict)
                    out[addr] = verdict
                continue
            for addr in batch:
                verdict = map_address(network, addr, payload, self.settings.max_tax_pct)
                self._remember(network, addr, verdict)
                out[addr] = verdict
        return out
